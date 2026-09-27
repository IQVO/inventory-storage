// Package ports declares the outbound interfaces the application layer
// depends on. Adapters implement these; the application never imports an
// adapter package.
package ports

import (
	"context"
	"time"

	"github.com/claudioed/inventory-storage/internal/domain/location"
	"github.com/claudioed/inventory-storage/internal/domain/product"
	"github.com/claudioed/inventory-storage/internal/domain/reservation"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
)

// StockRepo persists and retrieves StockUnit aggregates.
type StockRepo interface {
	Save(ctx context.Context, unit *stock.StockUnit) error
	FindByID(ctx context.Context, id string) (*stock.StockUnit, error)
	FindBySKU(ctx context.Context, sku shared.SKU) ([]*stock.StockUnit, error)
	FindByBin(ctx context.Context, binID shared.BinId) ([]*stock.StockUnit, error)
	NextID(ctx context.Context) (string, error)
}

// LocationRepo persists and retrieves Bin aggregates.
type LocationRepo interface {
	Save(ctx context.Context, bin *location.Bin) error
	FindByID(ctx context.Context, id shared.BinId) (*location.Bin, error)
}

// ReservationRepo persists and retrieves Reservation aggregates.
type ReservationRepo interface {
	Save(ctx context.Context, r *reservation.Reservation) error
	FindByID(ctx context.Context, id string) (*reservation.Reservation, error)
	// FindByDemandRef returns every Reservation ever created against the
	// given demandRef (the external system's order+line reference, e.g.
	// from order-management). A demandRef can have MULTIPLE reservations
	// across its lifetime — a revoked reservation followed by a
	// successful retry, for instance — so this returns a slice, not a
	// single result. An unknown demandRef returns an empty slice, not an
	// error. Order is not guaranteed by this interface.
	FindByDemandRef(ctx context.Context, demandRef string) ([]*reservation.Reservation, error)
	NextID(ctx context.Context) (string, error)
}

// EventPublisher publishes domain events. Adapters may log them, buffer
// them, or forward them to a broker (e.g. Kafka).
type EventPublisher interface {
	Publish(ctx context.Context, event shared.DomainEvent) error
}

// ReservationMetrics records reservation lifecycle outcomes so the business
// signal (how much demand is bound, how much comes back) is observable
// independently of HTTP traffic. Use cases treat a nil value as "not
// instrumented", so wiring it is optional.
type ReservationMetrics interface {
	ReservationCreated(ctx context.Context)
	ReservationRevoked(ctx context.Context)
}

// Clock abstracts current time so use cases and tests are deterministic.
type Clock interface {
	Now() time.Time
}

// UnitOfWork brackets a use case's state change and the domain event(s) it
// raises so both commit or neither does (ADR 0017, transactional outbox).
//
// Execute runs fn inside one atomic scope. Every Repo.Save and
// EventPublisher.Publish made with the ctx handed to fn is bound to that
// same scope: if fn returns an error the scope is rolled back and nothing
// — neither the aggregate row nor the outbox row(s) — is visible
// afterwards.
//
// Adapters that have no transactional backing (the in-memory repos, the
// log publisher) satisfy this with a pass-through that simply calls fn;
// the use cases stay adapter-agnostic either way, and a nil UnitOfWork is
// treated identically by every use case (see the usecases package's
// atomically helper).
type UnitOfWork interface {
	Execute(ctx context.Context, fn func(ctx context.Context) error) error
}

// ProductClassificationRepo persists and retrieves ProductClassification
// aggregates, keyed by SKU. This service is the source of truth for this
// master data — see ADR 0009.
type ProductClassificationRepo interface {
	Save(ctx context.Context, c *product.ProductClassification) error
	FindBySKU(ctx context.Context, sku shared.SKU) (*product.ProductClassification, error)
}

// LocationClassificationLookup is the outbound port for the synchronous
// cross-context read from facility-layout's location-classification
// endpoint, used by StowStock to enforce hazmat/temperature placement
// rules. BinId values are treated as directly usable facility-layout
// LocationCode values — a documented cross-context simplification (see
// ADR 0009 and the facility-layout GetSlotAttributes call).
type LocationClassificationLookup interface {
	GetSlotAttributes(ctx context.Context, binID shared.BinId) (product.SlotAttributes, error)
}
