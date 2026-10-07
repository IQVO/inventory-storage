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
	"github.com/claudioed/inventory-storage/internal/domain/transfer"
)

// StockRepo persists and retrieves StockUnit aggregates.
type StockRepo interface {
	Save(ctx context.Context, unit *stock.StockUnit) error
	FindByID(ctx context.Context, id string) (*stock.StockUnit, error)
	FindBySKU(ctx context.Context, sku shared.SKU) ([]*stock.StockUnit, error)
	FindByBin(ctx context.Context, binID shared.BinId) ([]*stock.StockUnit, error)
	// FindBySKUAtSite returns the stock units of sku whose recorded site
	// custody is originSiteID. Units with no recorded site (legacy rows)
	// are NEVER included: site-scoped transfer allocation must fail
	// closed rather than let an unscoped unit donate stock.
	FindBySKUAtSite(ctx context.Context, sku shared.SKU, originSiteID shared.SiteID) ([]*stock.StockUnit, error)
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

// ProductClassificationRepo reads the local copy of a SKU's handling
// classification, keyed by SKU. product-master owns this master data
// (ADR 0034); StowStock and the deprecated GET endpoint read the copy
// exactly as they read this service's own rows before the hand-over.
// FindBySKU returns nil, nil for an unclassified SKU.
type ProductClassificationRepo interface {
	FindBySKU(ctx context.Context, sku shared.SKU) (*product.ProductClassification, error)
}

// ProductClassificationLocalCopy writes the local copy from product-master's
// ProductClassified events (ADR 0034).
type ProductClassificationLocalCopy interface {
	// ApplyIfNewer upserts c for c.SKU() when version is greater than the
	// stored version (an absent row counts as older than any version), and
	// records source as the row's classification_source. It reports
	// whether the row was written; a stale or equal version is a no-op
	// (false, nil).
	ApplyIfNewer(ctx context.Context, c *product.ProductClassification, version int64, source string) (bool, error)
}

// ProductClassificationCatalogue pages through every stored classification
// in SKU order. Only the one-shot republish-product-classifications backfill
// uses it (ADR 0034, stage B).
type ProductClassificationCatalogue interface {
	// ListAfter returns up to limit classifications whose SKU sorts
	// strictly after afterSKU (afterSKU "" starts from the beginning).
	ListAfter(ctx context.Context, afterSKU shared.SKU, limit int) ([]*product.ProductClassification, error)
}

// ProcessedEventRepo records which inbound CloudEvents a consumer has
// already applied, so at-least-once delivery has an exactly-once effect.
type ProcessedEventRepo interface {
	// Claim records (consumer, eventID). It reports false when the pair was
	// already recorded (a redelivery). Called inside the same UnitOfWork as
	// the effect, so a rolled-back effect also un-claims the id.
	Claim(ctx context.Context, consumer, eventID string) (bool, error)
}

// TransferAllocationRepo persists and retrieves the transfer-allocation
// ledger: one row per decided transfer line, DB-unique on transfer_line_id.
type TransferAllocationRepo interface {
	// FindByTransferLineID returns the decided allocation for that line,
	// or nil when this line has never been decided.
	FindByTransferLineID(ctx context.Context, transferLineID string) (*transfer.Allocation, error)
	// Save inserts the allocation. A second Save for an already-decided
	// transfer_line_id must fail with ErrTransferLineAlreadyDecided (the
	// DB unique constraint) so the use case can route the replay to the
	// "return the original outcome" path.
	Save(ctx context.Context, a *transfer.Allocation) error
}

// TransferReceiptRepo persists and retrieves destination transfer
// receipts (ADR 0031): one row per arrived transfer line, DB-unique on
// transfer_line_id — the idempotency anchor for BOTH the stage and the
// stow step.
type TransferReceiptRepo interface {
	// FindByTransferLineID returns the receipt for that line, or nil
	// when no receipt has ever been staged for it.
	FindByTransferLineID(ctx context.Context, transferLineID string) (*transfer.Receipt, error)
	// Save inserts a STAGED receipt. A second Save for an already-
	// staged transfer_line_id must fail with
	// ErrTransferReceiptAlreadyStaged (the DB unique constraint).
	Save(ctx context.Context, r *transfer.Receipt) error
	// SaveStowed updates the receipt to its STOWED terminal state,
	// recording the stow legs. Saving a receipt that is not in STAGED
	// state must fail with ErrTransferReceiptAlreadyStowed.
	SaveStowed(ctx context.Context, r *transfer.Receipt) error
}

// InventoryExceptionRepo persists quarantined destination scans (ADR
// 0031). Writing an exception changes NO stock; it exists so an
// unrecognized arrival is auditable and resolvable by a human.
type InventoryExceptionRepo interface {
	// FindByScan returns the quarantine record for the exact same scan
	// (line, destination, sku, quantity), or nil when this scan was
	// never quarantined — the replay check that keeps a repeated
	// identical scan from writing a second exception.
	FindByScan(ctx context.Context, transferLineID string, destinationSiteID shared.SiteID, sku shared.SKU, receivedQty shared.Quantity) (*transfer.Exception, error)
	Save(ctx context.Context, e *transfer.Exception) error
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
