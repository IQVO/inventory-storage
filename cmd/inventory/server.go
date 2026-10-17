package main

import (
	inboundhttp "github.com/claudioed/inventory-storage/internal/adapters/inbound/http"
	"github.com/claudioed/inventory-storage/internal/application/ports"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
)

// buildServer wires the repositories, event publisher, location lookup,
// clock and reservation metrics into use cases and returns the inbound
// HTTP server's dependency struct.
func buildServer(
	a adapterSet,
	clock ports.Clock,
	locationLookup ports.LocationClassificationLookup,
	reservationMetrics ports.ReservationMetrics,
	readiness *inboundhttp.Readiness,
) *inboundhttp.Server {
	stockRepo, locationRepo, reservationRepo, classificationRepo := a.stock, a.locations, a.reservations, a.classifications
	publisher, uow := a.publisher, a.uow
	levels := a.levelEmitter(clock)
	return &inboundhttp.Server{
		ReceiveStock: &usecases.ReceiveStock{Events: publisher, Clock: clock, UnitOfWork: uow},
		StowStock: &usecases.StowStock{
			Stock: stockRepo, Locations: locationRepo, Events: publisher, Clock: clock,
			Classifications: classificationRepo, LocationLookup: locationLookup, UnitOfWork: uow, Levels: levels,
		},
		ReserveStock:               &usecases.ReserveStock{Stock: stockRepo, Reservations: reservationRepo, Events: publisher, Clock: clock, Metrics: reservationMetrics, UnitOfWork: uow, Levels: levels},
		RevokeReservation:          &usecases.RevokeReservation{Stock: stockRepo, Reservations: reservationRepo, Events: publisher, Clock: clock, Metrics: reservationMetrics, UnitOfWork: uow, Levels: levels},
		ConfirmPick:                &usecases.ConfirmPick{Stock: stockRepo, Locations: locationRepo, Reservations: reservationRepo, Events: publisher, Clock: clock, UnitOfWork: uow, Levels: levels},
		GetUsable:                  &usecases.GetUsable{Stock: stockRepo},
		GetReservationsByDemandRef: &usecases.GetReservationsByDemandRef{Stock: stockRepo, Reservations: reservationRepo, Events: publisher, Clock: clock, UnitOfWork: uow, Levels: levels},
		RunCycleCount:              &usecases.RunCycleCount{Stock: stockRepo, Events: publisher, Clock: clock, UnitOfWork: uow, Levels: levels},
		RegisterBin:                &usecases.RegisterBin{Locations: locationRepo, UnitOfWork: uow},
		GetBin:                     &usecases.GetBin{Locations: locationRepo},
		StageTransferReceipt: &usecases.StageTransferReceipt{
			Transfers:  a.transfers,
			Receipts:   a.transferReceipts,
			Exceptions: a.inventoryExceptions,
			Events:     publisher,
			Clock:      clock,
			UnitOfWork: uow,
		},
		StowTransferStock: &usecases.StowTransferStock{
			Receipts:   a.transferReceipts,
			Stock:      stockRepo,
			Locations:  locationRepo,
			Events:     publisher,
			Clock:      clock,
			UnitOfWork: uow,
			Levels:     levels,
		},
		RelocateStock:   newRelocateStock(a, clock),
		GetRelocation:   &usecases.GetRelocation{Relocations: a.relocations},
		Classifications: classificationRepo,
		// IdempotencyPool wires RequireIdempotencyKey onto POST
		// /stock/receive and POST /reservations (see
		// inboundhttp.NewRouter). nil (in-memory/no-DATABASE_URL
		// configuration) leaves those routes unprotected, mirroring
		// every other optional Postgres-backed capability here.
		IdempotencyPool: a.pool,
		// Readiness backs GET /readyz (ADR-0020 §graceful shutdown):
		// flipped to not-ready as the FIRST step of shutdown (see
		// shutdown.go), before anything else stops.
		Readiness: readiness,
	}
}

// newRelocateStock wires the intra-site relocation use case (ADR 0039),
// shared by POST /relocations and the TaskCompleted RELOCATE branch.
func newRelocateStock(a adapterSet, clock ports.Clock) *usecases.RelocateStock {
	return &usecases.RelocateStock{
		Stock: a.stock, Locations: a.locations, Relocations: a.relocations,
		Events: a.publisher, Clock: clock, UnitOfWork: a.uow, Levels: a.levelEmitter(clock),
	}
}
