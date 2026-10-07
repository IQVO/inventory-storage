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
	return &inboundhttp.Server{
		ReceiveStock: &usecases.ReceiveStock{Events: publisher, Clock: clock, UnitOfWork: uow},
		StowStock: &usecases.StowStock{
			Stock: stockRepo, Locations: locationRepo, Events: publisher, Clock: clock,
			Classifications: classificationRepo, LocationLookup: locationLookup, UnitOfWork: uow,
		},
		ReserveStock:               &usecases.ReserveStock{Stock: stockRepo, Reservations: reservationRepo, Events: publisher, Clock: clock, Metrics: reservationMetrics, UnitOfWork: uow},
		RevokeReservation:          &usecases.RevokeReservation{Stock: stockRepo, Reservations: reservationRepo, Events: publisher, Clock: clock, Metrics: reservationMetrics, UnitOfWork: uow},
		ConfirmPick:                &usecases.ConfirmPick{Stock: stockRepo, Locations: locationRepo, Reservations: reservationRepo, Events: publisher, Clock: clock, UnitOfWork: uow},
		GetUsable:                  &usecases.GetUsable{Stock: stockRepo},
		GetReservationsByDemandRef: &usecases.GetReservationsByDemandRef{Stock: stockRepo, Reservations: reservationRepo, Events: publisher, Clock: clock, UnitOfWork: uow},
		RunCycleCount:              &usecases.RunCycleCount{Stock: stockRepo, Events: publisher, Clock: clock, UnitOfWork: uow},
		ClassifyProduct:            &usecases.ClassifyProduct{Classifications: classificationRepo, Events: publisher, Clock: clock, UnitOfWork: uow},
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
		},
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
