// Command inventory is the composition root: it wires env config into
// adapters, adapters into use cases, and use cases into the HTTP router.
//
// The package is split by concern; main.go keeps only the process entry
// point and the top-level startup sequence:
//
//	config.go       env parsing (config, logger, duration helpers)
//	telemetry.go    OpenTelemetry bring-up and final flush
//	adapters.go     repository / publisher / outbox-relay assembly
//	postgres.go     migrations + pgxpool dial under boot retry
//	housekeeping.go idempotency-key / outbox retention sweeper
//	lookup.go       LocationClassificationLookup adapter selection
//	transfer.go     transfer allocation command consumer (Phase 2)
//	productmaster.go product-master classification consumer (ADR 0034)
//	republish.go    republish-product-classifications one-shot subcommand
//	server.go       use-case + HTTP server wiring
//	shutdown.go     signal handling and graceful drain
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"time"

	inboundhttp "github.com/claudioed/inventory-storage/internal/adapters/inbound/http"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/memory"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/telemetry"
)

func main() {
	// A subcommand (e.g. republish-product-classifications, ADR 0034) runs
	// once and exits instead of starting the service.
	if len(os.Args) > 1 {
		os.Exit(runCommand(context.Background(), os.Args[1:], os.Stdout, os.Stderr))
	}
	if err := run(); err != nil {
		slog.Error("service exited with error", "error", err)
		os.Exit(1)
	}
}

// run is the startup sequence. Order matters and mirrors the shutdown
// order in reverse (defers run LIFO): telemetry first (so every adapter is
// built against the real providers) and flushed last; adapters next and
// closed after the HTTP server and the inbound consumers have stopped.
func run() error {
	cfg := loadConfig()
	logger := newLogger(cfg.logLevel)
	slog.SetDefault(logger)

	// Telemetry comes up before any adapter, so the pgx pool and the Kafka
	// writer are built against the real providers rather than the no-op
	// globals. Registered before every other defer so it runs last: the
	// final flush happens once the HTTP server has stopped and the adapters
	// are closed.
	flushTelemetry, err := startTelemetry(logger, cfg)
	if err != nil {
		return err
	}
	defer flushTelemetry()

	adapters, err := buildAdapters(context.Background(), cfg.databaseURL, cfg.migrationsDatabaseURL, cfg.migrationsPath, cfg.eventPublisher, logger)
	if err != nil {
		return err
	}
	defer adapters.close()

	reservationMetrics, err := telemetry.NewReservationMetrics()
	if err != nil {
		return err
	}

	// readiness gates GET /readyz (ADR-0020 §graceful shutdown). The
	// zero value is ready; SetNotReady is called as the FIRST step of
	// the shutdown sequence (see shutdown.go), before the HTTP server
	// itself stops accepting connections.
	readiness := &inboundhttp.Readiness{}

	// The inbound consumers' Kafka loops (facility location cache when
	// LOCATION_LOOKUP_MODE=kafka, transfer allocation commands when
	// TRANSFER_ALLOCATION_CONSUMER_MODE=kafka) must outlive this call
	// and stop on shutdown, so they share one cancellable context
	// rather than the signal context established in
	// serveHTTPUntilSignal — which does not exist yet at this point.
	lookupCtx, stopLookup := context.WithCancel(context.Background())
	lookup, err := buildLocationLookup(lookupCtx, cfg.locationLookupMode, cfg.facilityLayoutBaseURL, cfg.kafkaBrokers, circuitBreakerMetrics(logger), logger)
	if err != nil {
		stopLookup()
		return err
	}

	// Transfer allocation command consumer (Phase 2 command/reply leg of
	// the network transfer saga): consumes network-inventory-planning's
	// TransferAllocationRequested and runs AllocateTransferStock. Runs
	// under the SAME lookupCtx, so shutdown's stopLookup cancel unblocks
	// both consumer loops. Default "off"; see buildTransferAllocationConsumer
	// for the fail-closed guards.
	transfer, err := buildTransferAllocationConsumer(lookupCtx, logger, cfg, adapters)
	if err != nil {
		stopLookup()
		lookup.close()
		return err
	}

	// product-master classification consumer (ADR 0034): keeps the local
	// product_classifications copy StowStock reads in step with
	// product-master. Same lookupCtx, so shutdown drains it with the rest.
	// Not started unless PRODUCT_MASTER_CONSUMER_GROUP is set.
	productMaster, err := buildProductMasterConsumer(lookupCtx, logger, cfg, adapters)
	if err != nil {
		stopLookup()
		lookup.close()
		stopTransferConsumer(logger, transfer)
		return err
	}
	consumers := combineConsumerHandles(transfer, productMaster)

	server := buildServer(adapters, memory.SystemClock{}, lookup.lookup, reservationMetrics, readiness)
	httpServer := &http.Server{
		Addr:              cfg.httpAddr,
		Handler:           inboundhttp.NewRouter(server, logger, cfg.serviceName),
		ReadHeaderTimeout: 5 * time.Second,
	}

	return serveHTTPUntilSignal(logger, httpServer, readiness, stopLookup, lookup.close, lookup.runDone, consumers)
}
