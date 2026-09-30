// Package postgres provides Postgres-backed adapters, including the
// transactional-outbox UnitOfWork/OutboxPublisher pair (this file's
// sibling outbox_publisher.go / unit_of_work.go) and the background relay
// that drains outbox_events onto Kafka (ADR 0017).
package postgres

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/inventory-storage/internal/adapters/outbound/kafka"
)

// Sink sends a batch of already-encoded messages to Kafka. kafka.RelaySink
// is the production implementation (one *kafkago.Writer with no fixed
// topic, since outbox rows may span both the integration and analytics
// topics); tests substitute a fake.
type Sink interface {
	Send(ctx context.Context, msgs ...kafka.Encoded) error
}

// outboxRow is one unpublished outbox_events row.
type outboxRow struct {
	id      int64
	encoded kafka.Encoded
}

// OutboxRelay polls outbox_events for unpublished rows and sends them to
// sink ONE AT A TIME in id order, marking each row published as it
// succeeds. It is the only path by which an outbox row ever reaches Kafka
// — Publish (outbox_publisher.go) only ever inserts a row, never sends it
// — so a crash between commit and relay pickup loses nothing: the row is
// picked up on the relay's next poll.
//
// Sending one row at a time (rather than batching the whole claimed set
// into a single sink.Send) means a failure on row N stops exactly there:
// rows before N in this pass are already committed as published, row N's
// attempts/last_error is recorded, and rows after N are left untouched —
// so per-key ordering is never violated by a later row overtaking a
// failed earlier one.
type OutboxRelay struct {
	pool        *pgxpool.Pool
	sink        Sink
	logger      *slog.Logger
	interval    time.Duration
	batchSize   int
	maxAttempts int
}

// OutboxRelayOption configures an OutboxRelay beyond its required pool and
// sink.
type OutboxRelayOption func(*OutboxRelay)

// WithInterval overrides the default 1s poll interval.
func WithInterval(d time.Duration) OutboxRelayOption {
	return func(r *OutboxRelay) { r.interval = d }
}

// WithBatchSize overrides the default 100-row batch size claimed per pass.
func WithBatchSize(n int) OutboxRelayOption {
	return func(r *OutboxRelay) { r.batchSize = n }
}

// WithLogger overrides the default slog.Default().
func WithLogger(l *slog.Logger) OutboxRelayOption {
	return func(r *OutboxRelay) { r.logger = l }
}

// WithMaxAttempts overrides the default of 0 (unlimited retries): a row
// that has failed this many times is excluded from future claims (still
// visible in outbox_events for manual inspection) instead of retried
// indefinitely. 0 keeps retrying without bound.
func WithMaxAttempts(n int) OutboxRelayOption {
	return func(r *OutboxRelay) { r.maxAttempts = n }
}

// NewOutboxRelay builds an OutboxRelay draining pool's outbox_events table
// onto sink.
func NewOutboxRelay(pool *pgxpool.Pool, sink Sink, opts ...OutboxRelayOption) *OutboxRelay {
	r := &OutboxRelay{
		pool:      pool,
		sink:      sink,
		logger:    slog.Default(),
		interval:  time.Second,
		batchSize: 100,
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// Run polls until ctx is cancelled, draining a batch every interval — or
// immediately again, with no wait, whenever a pass claims a full batch
// (there is likely more work waiting right now). It never returns a
// non-nil error itself: a send failure is logged and left for the next
// pass to retry (the row's published_at stays NULL), so a transient
// broker outage self-heals without operator intervention.
func (r *OutboxRelay) Run(ctx context.Context) error {
	for {
		_, full, err := r.relayOnce(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			r.logger.Error("outbox relay: drain failed", "error", err)
		}

		if full {
			select {
			case <-ctx.Done():
				return nil
			default:
				continue
			}
		}

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(r.interval):
		}
	}
}

// RelayOnce runs exactly one claim-and-send pass and reports how many
// rows were successfully published, for tests to drive deterministically
// (production code uses Run's loop instead). Unlike Run, RelayOnce
// returns the pass's error — including a sink send failure — rather than
// only logging it, so a test can assert on the exact failure without
// racing Run's background loop.
func (r *OutboxRelay) RelayOnce(ctx context.Context) (int, error) {
	n, _, err := r.relayOnce(ctx)
	return n, err
}

// relayOnce claims up to batchSize unpublished rows with SELECT ... FOR
// UPDATE SKIP LOCKED (so a second relay instance, if one is ever run,
// claims a disjoint set rather than racing this one), sends them to the
// sink one at a time in id order, and commits whatever was published or
// recorded as failed. It reports how many rows were successfully
// published and whether the claimed batch was full (a signal there is
// likely more work waiting for an immediate next pass).
func (r *OutboxRelay) relayOnce(ctx context.Context) (published int, full bool, err error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return 0, false, err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback(ctx)
		}
	}()

	rows, err := r.claimBatch(ctx, tx)
	if err != nil {
		return 0, false, err
	}
	if len(rows) == 0 {
		return 0, false, tx.Commit(ctx)
	}

	for _, row := range rows {
		if sendErr := r.sink.Send(ctx, row.encoded); sendErr != nil {
			if _, uerr := tx.Exec(ctx, `
				UPDATE outbox_events SET attempts = attempts + 1, last_error = $2
				WHERE id = $1
			`, row.id, sendErr.Error()); uerr != nil {
				return published, false, errors.Join(sendErr, uerr)
			}
			r.logger.Warn("outbox relay: send failed, stopping this pass at the failed row",
				"outbox_id", row.id, "topic", row.encoded.Topic, "error", sendErr)
			// Commit: every row published so far in this loop already
			// has its UPDATE staged in this same tx, and this row's
			// attempts/last_error is staged too. Stop here so a later
			// row can never overtake this one's ordering. Return
			// sendErr itself (not nil) so RelayOnce's caller — a test,
			// most commonly — can assert on the exact failure.
			if commitErr := tx.Commit(ctx); commitErr != nil {
				return published, false, errors.Join(sendErr, commitErr)
			}
			return published, false, sendErr
		}

		if _, uerr := tx.Exec(ctx, `UPDATE outbox_events SET published_at = now() WHERE id = $1`, row.id); uerr != nil {
			return published, false, uerr
		}
		published++
	}

	if err := tx.Commit(ctx); err != nil {
		return published, false, err
	}
	return published, len(rows) == r.batchSize, nil
}

// claimBatch selects and locks up to batchSize unpublished rows, oldest
// first, within tx.
func (r *OutboxRelay) claimBatch(ctx context.Context, tx pgx.Tx) ([]outboxRow, error) {
	var attemptsFilter string
	args := []any{r.batchSize}
	if r.maxAttempts > 0 {
		attemptsFilter = "AND attempts < $2"
		args = append(args, r.maxAttempts)
	}

	sqlRows, err := tx.Query(ctx, `
		SELECT id, topic, event_type, key, value, headers
		FROM outbox_events
		WHERE published_at IS NULL `+attemptsFilter+`
		ORDER BY id ASC
		LIMIT $1
		FOR UPDATE SKIP LOCKED
	`, args...)
	if err != nil {
		return nil, err
	}
	defer sqlRows.Close()

	var out []outboxRow
	for sqlRows.Next() {
		var row outboxRow
		var headersRaw []byte
		if err := sqlRows.Scan(&row.id, &row.encoded.Topic, &row.encoded.EventType, &row.encoded.Key, &row.encoded.Value, &headersRaw); err != nil {
			return nil, err
		}
		headers, err := decodeOutboxHeaders(headersRaw)
		if err != nil {
			return nil, err
		}
		row.encoded.Headers = headers
		out = append(out, row)
	}
	if err := sqlRows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}
