package analyticsstore

import (
	"context"
	"fmt"

	"github.com/exaring/otelpgx"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// MaxConns is the analytics writer's (cmd/inventory-projector) per-process
// connection ceiling. The projector's Kafka consumer group
// (kafka.AnalyticsConsumerGroup, "inventory-analytics") is a STABLE, shared
// group with no per-instance uniqueness, so it IS HPA-scalable (chart's
// autoscaling.projector block, min 1 / max 2 — capped below api's 4, same
// reasoning as order-management's ADR-0026: OrderId/SKU-keyed partitioning
// only guarantees per-aggregate ordering, and this is a lightweight
// idempotent-upsert workload that doesn't need wide fan-out). At that
// ceiling, 2 * 5 = 10 connections against the analytical database.
const MaxConns = 5

// ReportsMaxConns is the analytics reader's (cmd/inventory-reports)
// per-process connection ceiling. Stateless REST reads over the read-only
// pool, HPA-scalable (chart's autoscaling.reports block, min 1 / max 3): at
// that ceiling, 3 * 5 = 15 connections against the analytical database. See
// the pgxpool/statement_timeout ADR for the full connection-budget
// accounting across this service's four processes on the ONE shared
// Postgres instance.
const ReportsMaxConns = 5

// StatementTimeout bounds the analytics WRITER's (projector) queries.
// Slightly more generous than the OLTP side's 5s: a Kafka consumer
// replaying a large backlog after a redeploy issues its upserts in a tight
// loop, and a transient lock wait here should not need to be as tight as an
// interactive OLTP request — but it must still not be unbounded, or one
// poisoned/oversized batch could wedge a projector replica's connection
// pool indefinitely.
const StatementTimeout = "10s"

// ReportsStatementTimeout bounds the analytics READER's (reports) queries.
// The Inventory Flow & Accuracy report aggregates rows across a
// caller-chosen time range — wider than the OLTP side's
// always-single-aggregate-by-id shape — so it gets more headroom than
// StatementTimeout, but still a hard ceiling: a caller-supplied unbounded
// date range must not be able to hold a reports connection forever.
const ReportsStatementTimeout = "15s"

// NewPool builds a pgxpool over the analytical database at databaseURL, with
// the OTel pgx tracer installed (mirroring the OLTP postgres.NewPool),
// MaxConns and StatementTimeout applied to every connection. It is used by
// the writer (cmd/inventory-projector).
func NewPool(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	return newPoolWithLimits(ctx, databaseURL, MaxConns, StatementTimeout, false)
}

// NewReadOnlyPool builds a pgxpool over the analytical database in which every
// connection is pinned to a read-only transaction default
// (default_transaction_read_only=on), with ReportsMaxConns and
// ReportsStatementTimeout applied. The reader process
// (cmd/inventory-reports) uses this so a bug there cannot mutate the read
// model even if the database role itself is not read-only — defence in depth
// on top of the read-only ANALYTICS_DATABASE_URL role (ADR-0011).
func NewReadOnlyPool(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	return newPoolWithLimits(ctx, databaseURL, ReportsMaxConns, ReportsStatementTimeout, true)
}

// newPoolWithLimits is the shared implementation behind NewPool/
// NewReadOnlyPool, parameterised so a test can drive a much shorter
// statementTimeout directly (proving the AfterConnect hook actually applies
// the setting to every new connection, by triggering a real cancellation)
// without waiting out the production value.
func newPoolWithLimits(ctx context.Context, databaseURL string, maxConns int32, statementTimeout string, readOnly bool) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, err
	}
	cfg.ConnConfig.Tracer = otelpgx.NewTracer()
	cfg.MaxConns = maxConns
	if readOnly {
		if cfg.ConnConfig.RuntimeParams == nil {
			cfg.ConnConfig.RuntimeParams = map[string]string{}
		}
		cfg.ConnConfig.RuntimeParams["default_transaction_read_only"] = "on"
	}
	cfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		_, err := conn.Exec(ctx, fmt.Sprintf("SET statement_timeout = '%s'", statementTimeout))
		return err
	}
	return pgxpool.NewWithConfig(ctx, cfg)
}

// RecordPoolStats registers observable gauges for pool's connection counts on
// the global MeterProvider, mirroring the OLTP postgres pool instrumentation.
func RecordPoolStats(pool *pgxpool.Pool) error {
	return otelpgx.RecordStats(pool)
}
