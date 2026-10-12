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
// so a later row never overtakes an earlier one that is still being
// retried.
//
// With WithMaxAttempts(n), n > 0 (ADR 0038; cmd/inventory passes
// OUTBOX_MAX_ATTEMPTS, default 20), a row that has failed n times is
// excluded from claims and logged at ERROR: later rows, including rows with
// the same key, then overtake it. That trade is deliberate — one poison row
// must not stop the whole relay forever — and the excluded row stays in
// outbox_events (attempts, last_error) until an operator re-arms it.
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

// DefaultOutboxMaxAttempts is the attempt cap a composition root uses when
// OUTBOX_MAX_ATTEMPTS is unset (fleet convention, ADR 0038). The relay's own
// zero value stays 0 (unlimited) so the option is always an explicit choice.
const DefaultOutboxMaxAttempts = 20

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
// indefinitely, and the failure that reaches the cap is logged at ERROR
// with the row's id, topic, type and key (ADR 0038). 0 keeps retrying
// without bound. Every cmd/* that builds a relay must pass it (architecture
// fitness test TestOutboxRelaysPassAttemptCap).
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
// broker outage self-heals without operator intervention — up to the
// attempt cap, if one is set (WithMaxAttempts).
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
			return published, false, r.recordFailure(ctx, tx, row, sendErr)
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

// recordFailure stages the failed row's attempts/last_error, commits the
// pass (every row published so far in it has its UPDATE staged in the same
// tx) and stops there, so a later row never overtakes a row that is still
// being retried. It returns sendErr itself (not nil) so RelayOnce's caller —
// a test, most commonly — can assert on the exact failure.
//
// The log is written only after the commit, so an ERROR saying a row
// reached the attempt cap is never emitted for an attempt that was rolled
// back.
func (r *OutboxRelay) recordFailure(ctx context.Context, tx pgx.Tx, row outboxRow, sendErr error) error {
	var attempts int
	if err := tx.QueryRow(ctx, `
		UPDATE outbox_events SET attempts = attempts + 1, last_error = $2
		WHERE id = $1
		RETURNING attempts
	`, row.id, sendErr.Error()).Scan(&attempts); err != nil {
		return errors.Join(sendErr, err)
	}
	if commitErr := tx.Commit(ctx); commitErr != nil {
		return errors.Join(sendErr, commitErr)
	}
	r.logFailure(row, attempts, sendErr)
	return sendErr
}

// logFailure logs a failed send at WARN while the row will be retried, and
// at ERROR once the row has reached the attempt cap (ADR 0038): from then on
// claimBatch excludes it, later rows overtake it (including rows with the
// same key) and it stays in outbox_events until an operator re-arms it
// (`UPDATE outbox_events SET attempts = 0 WHERE id = ...`).
func (r *OutboxRelay) logFailure(row outboxRow, attempts int, sendErr error) {
	if r.maxAttempts > 0 && attempts >= r.maxAttempts {
		r.logger.Error("outbox relay: row reached the attempt cap and is excluded from future claims; re-arm it with attempts = 0 after fixing the cause",
			"outbox_id", row.id, "topic", row.encoded.Topic, "event_type", row.encoded.EventType,
			"key", string(row.encoded.Key), "attempts", attempts, "max_attempts", r.maxAttempts, "error", sendErr)
		return
	}
	r.logger.Warn("outbox relay: send failed, stopping this pass at the failed row",
		"outbox_id", row.id, "topic", row.encoded.Topic, "attempts", attempts, "error", sendErr)
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
