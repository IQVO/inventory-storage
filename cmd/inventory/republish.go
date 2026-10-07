package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/inventory-storage/internal/adapters/outbound/bootretry"
	kafkaadapter "github.com/claudioed/inventory-storage/internal/adapters/outbound/kafka"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/memory"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/postgres"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
)

// republishCommand is the one-shot backfill of product-master ADR 0003
// stage B (ADR 0034):
//
//	kubectl exec deploy/inventory-storage -- ./inventory republish-product-classifications
//
// It enqueues the legacy ProductClassified for every product_classifications
// row through the transactional outbox; the running pod's relay publishes
// them. Removed at product-master ADR 0003 stage E.
const republishCommand = "republish-product-classifications"

// runCommand dispatches a subcommand of the main binary and returns the
// process exit code: 0 on success, 1 on failure, 2 on an unknown command.
func runCommand(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	switch args[0] {
	case republishCommand:
		// Logs go to stderr so stdout carries only the result line.
		logger := slog.New(slog.NewJSONHandler(stderr, nil))
		if err := runRepublish(ctx, os.Getenv("DATABASE_URL"), stdout, logger); err != nil {
			_, _ = fmt.Fprintf(stderr, "%s: %v\n", republishCommand, err)
			return 1
		}
		return 0
	default:
		_, _ = fmt.Fprintf(stderr, "unknown command %q (supported: %s)\n", args[0], republishCommand)
		return 2
	}
}

// runRepublish opens a pool on databaseURL (no migrations: the running pod
// owns the schema), runs the backfill and prints the row count. It needs
// only DATABASE_URL: no broker, no HTTP server.
func runRepublish(ctx context.Context, databaseURL string, out io.Writer, logger *slog.Logger) error {
	if databaseURL == "" {
		return errors.New("DATABASE_URL is required")
	}
	pool, err := postgres.NewPool(ctx, databaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := bootretry.Retry(ctx, logger, "ping database", func() error { return pool.Ping(ctx) }); err != nil {
		return err
	}

	n, err := newRepublishUseCase(pool).Execute(ctx)
	if err != nil {
		return fmt.Errorf("after %d classifications: %w", n, err)
	}
	_, err = fmt.Fprintf(out, "republished %d product classifications\n", n)
	return err
}

// newRepublishUseCase wires the backfill: the Postgres catalogue, and the
// outbox publisher with ONLY the integration-topic encoder (the legacy
// ProductClassified mapping, kept for this command). Encoding
// ProductClassified needs neither a Kafka writer nor the reservation repo.
func newRepublishUseCase(pool *pgxpool.Pool) *usecases.RepublishProductClassifications {
	return &usecases.RepublishProductClassifications{
		Catalogue:  postgres.NewProductClassificationRepo(pool),
		Events:     postgres.NewOutboxPublisher(pool, kafkaadapter.NewPublisher(nil, nil)),
		Clock:      memory.SystemClock{},
		UnitOfWork: postgres.NewUnitOfWork(pool),
	}
}
