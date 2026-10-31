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
//	inboundreceipt.go inbound-receiving receipt consumer (ADR 0037)
//	confirmpick.go  TaskCompleted confirm-pick consumer (ADR 0035)
//	server.go       use-case + HTTP server wiring
//	shutdown.go     signal handling and graceful drain
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	inboundhttp "github.com/claudioed/inventory-storage/internal/adapters/inbound/http"
	kafkaadapter "github.com/claudioed/inventory-storage/internal/adapters/outbound/kafka"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/system"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/telemetry"
)

func main() {
	// ctx is cancelled by SIGINT/SIGTERM: the start of the graceful drain.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	err := run(ctx)
	stop()
	if err != nil {
		slog.Error("service exited with error", "error", err)
		os.Exit(1)
	}
}

// run is the startup sequence. Order matters and mirrors the shutdown
// order in reverse (defers run LIFO): the mandatory configuration first (a
// missing DATABASE_URL or KAFKA_BROKERS fails the boot before anything is
// dialled), then telemetry (so every adapter is built against the real
// providers) flushed last; adapters next and closed after the HTTP server
// and the inbound consumers have stopped. ctx is the signal context: its
// cancellation starts the drain, and a background worker's death does too
// (run then returns that error, so the process exits non-zero).
func run(ctx context.Context) error {
	cfg, err := loadConfig(os.Getenv)
	if err != nil {
		return err
	}
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

	adapters, err := buildAdapters(context.Background(), cfg.databaseURL, cfg.migrationsDatabaseURL, cfg.migrationsPath, cfg.kafkaBrokers, logger)
	if err != nil {
		return err
	}
	defer adapters.close()

	reservationMetrics, err := telemetry.NewReservationMetrics()
	if err != nil {
		return err
	}

	// readiness gates GET /readyz (ADR-0020 §graceful shutdown): the drain
	// gate (SetNotReady is called as the FIRST step of the shutdown
	// sequence, see shutdown.go, before the HTTP server stops accepting
	// connections) AND Postgres and Kafka answering. /healthz stays liveness.
	readiness := inboundhttp.NewReadiness(inboundhttp.DefaultReadinessTTL, inboundhttp.DefaultReadinessTimeout,
		inboundhttp.Dependency{Name: "postgres", Check: adapters.pool.Ping},
		inboundhttp.Dependency{Name: "kafka", Check: kafkaadapter.NewBrokerCheck(cfg.kafkaBrokers).Check},
	)

	// The inbound consumers' Kafka loops (facility location cache when
	// LOCATION_LOOKUP_MODE=kafka, transfer allocation commands, product
	// classification, receipts, task completions) must outlive this call
	// and stop on shutdown, so they share one cancellable context.
	// The consumers' loops must outlive the signal and stop only in the
	// shutdown sequence (after readiness flips and the HTTP drain), so their
	// context is detached from ctx's cancellation; it keeps ctx's values and
	// carries the deaths channel on which a dead consumer reports.
	workerCtx, deaths := withDeaths(context.WithoutCancel(ctx))
	lookupCtx, stopLookup := context.WithCancel(workerCtx)
	lookup, err := buildLocationLookup(lookupCtx, cfg.locationLookupMode, cfg.facilityLayoutBaseURL, cfg.kafkaBrokers, circuitBreakerMetrics(logger), logger)
	if err != nil {
		stopLookup()
		return err
	}

	// Transfer allocation command consumer (Phase 2 command/reply leg of
	// the network transfer saga): consumes network-inventory-planning's
	// TransferAllocationRequested and runs AllocateTransferStock. Runs
	// under the SAME lookupCtx, so shutdown's stopLookup cancel unblocks
	// both consumer loops. Always on.
	transfer, err := buildTransferAllocationConsumer(lookupCtx, logger, cfg, adapters)
	if err != nil {
		stopLookup()
		lookup.close()
		return err
	}

	// product-master classification consumer (ADR 0034): keeps the local
	// product_classifications copy StowStock reads in step with
	// product-master. Same lookupCtx, so shutdown drains it with the rest.
	productMaster, err := buildProductMasterConsumer(lookupCtx, logger, cfg, adapters)
	if err != nil {
		stopLookup()
		lookup.close()
		stopTransferConsumer(logger, transfer)
		return err
	}

	// TaskCompleted confirm-pick (ADR 0035) and inbound-receiving receipt
	// (ADR 0037) consumers, on the same lookupCtx; see
	// buildEventDrivenConsumers. It stops the consumers built above on error.
	consumers, err := buildEventDrivenConsumers(lookupCtx, logger, cfg, adapters, combineConsumerHandles(transfer, productMaster))
	if err != nil {
		stopLookup()
		lookup.close()
		return err
	}
	server := buildServer(adapters, system.Clock{}, lookup.lookup, lookup.handlingUnits, reservationMetrics, readiness)
	httpServer := &http.Server{
		Addr:              cfg.httpAddr,
		Handler:           inboundhttp.NewRouter(server, logger, cfg.serviceName),
		ReadHeaderTimeout: 5 * time.Second,
	}

	return serveHTTPUntilSignal(ctx, logger, httpServer, readiness, deaths, stopLookup, lookup.close, lookup.runDone, consumers)
}
