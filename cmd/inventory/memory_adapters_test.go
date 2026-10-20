package main

import (
	"log/slog"

	"github.com/claudioed/inventory-storage/internal/adapters/outbound/events"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/memory"
)

// memoryAdapters builds the in-memory outbound set as a TEST DOUBLE for the
// wiring tests that only inspect which repos a use case is wired with. It
// lives in a _test.go file on purpose: Postgres and Kafka are the only
// runtime (the real-wiring ADR), so no non-test file of cmd/inventory may
// import the memory package (TestRuntimeBinaryHasNoInMemoryAdapters).
func memoryAdapters(logger *slog.Logger) adapterSet {
	stockRepo := memory.NewStockRepo()
	return adapterSet{
		stock:               stockRepo,
		locations:           memory.NewLocationRepo(),
		reservations:        memory.NewReservationRepo(),
		classifications:     memory.NewProductClassificationRepo(),
		publisher:           events.NewLogPublisher(logger),
		transfers:           memory.NewTransferAllocationRepo(),
		transferReceipts:    memory.NewTransferReceiptRepo(),
		inventoryExceptions: memory.NewInventoryExceptionRepo(),
		relocations:         memory.NewRelocationRepo(),
		binLevels:           memory.NewBinLevelLedger(stockRepo),
		close:               func() {},
	}
}
