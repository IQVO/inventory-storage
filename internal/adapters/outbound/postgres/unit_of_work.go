package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// querier is the subset of pgx that both *pgxpool.Pool and pgx.Tx satisfy.
// Every adapter in this package issues its SQL through the querier it
// resolves from the context, so the same repo/publisher code runs either
// autonomously (pool) or inside a UnitOfWork transaction (tx) without
// knowing which (ADR 0017, transactional outbox).
type querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type txKey struct{}

// withTx returns a child context carrying tx, so adapters called within
// UnitOfWork.Execute join the transaction.
func withTx(ctx context.Context, tx pgx.Tx) context.Context {
	return context.WithValue(ctx, txKey{}, tx)
}

// txFrom reports the transaction bound to ctx, if any.
func txFrom(ctx context.Context) (pgx.Tx, bool) {
	tx, ok := ctx.Value(txKey{}).(pgx.Tx)
	return tx, ok
}

// querierFrom resolves the transaction bound to ctx, or falls back to the
// pool when the caller is not inside a UnitOfWork.
func querierFrom(ctx context.Context, pool *pgxpool.Pool) querier {
	if tx, ok := txFrom(ctx); ok {
		return tx
	}
	return pool
}

// UnitOfWork implements ports.UnitOfWork over a single Postgres
// transaction. Everything the wrapped function does through this
// package's adapters — the aggregate upsert(s) AND the outbox insert(s) —
// commits together or not at all (ADR 0017).
type UnitOfWork struct {
	pool *pgxpool.Pool
}

// NewUnitOfWork constructs a UnitOfWork over pool.
func NewUnitOfWork(pool *pgxpool.Pool) *UnitOfWork {
	return &UnitOfWork{pool: pool}
}

// Execute runs fn inside one transaction. A nested call (fn invoked with
// a ctx that already carries a transaction) simply joins the outer scope
// rather than opening a second one, so a use case that calls another
// helper which also brackets its own writes atomically (e.g.
// expireIfDue inside ReserveStock) never deadlocks on itself.
func (u *UnitOfWork) Execute(ctx context.Context, fn func(ctx context.Context) error) (err error) {
	if _, ok := txFrom(ctx); ok {
		return fn(ctx)
	}

	tx, err := u.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("postgres: begin unit of work: %w", err)
	}
	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback(ctx)
			panic(p)
		}
		if err != nil {
			if rbErr := tx.Rollback(ctx); rbErr != nil && !errors.Is(rbErr, pgx.ErrTxClosed) {
				err = errors.Join(err, fmt.Errorf("postgres: rollback unit of work: %w", rbErr))
			}
		}
	}()

	if err = fn(withTx(ctx, tx)); err != nil {
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("postgres: commit unit of work: %w", err)
	}
	return nil
}

// beginOrJoin returns the ctx-bound tx (owned=false: commit/rollback below
// are no-ops, the outer UnitOfWork scope owns the transaction's fate) or
// begins a new one on pool (owned=true). Repos that historically opened
// their OWN transaction (none in this service currently do — every
// existing repo issues single statements or joins via querierFrom — but
// this helper is kept available for a future repo that needs a multi-
// statement write of its own) use this instead of pool.Begin directly, so
// their writes join an outer UnitOfWork scope when one is active.
func beginOrJoin(ctx context.Context, pool *pgxpool.Pool) (tx pgx.Tx, commit func(context.Context) error, rollback func(context.Context) error, err error) {
	if existing, ok := txFrom(ctx); ok {
		noop := func(context.Context) error { return nil }
		return existing, noop, noop, nil
	}
	tx, err = pool.Begin(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	return tx, tx.Commit, tx.Rollback, nil
}
