// Package ports declares the outbound interfaces the application layer
// depends on. Adapters implement these; the application never imports an
// adapter package.
package ports

import (
	"context"
	"time"

	"github.com/claudioed/inventory-storage/internal/domain/container"
	"github.com/claudioed/inventory-storage/internal/domain/location"
	"github.com/claudioed/inventory-storage/internal/domain/packaging"
	"github.com/claudioed/inventory-storage/internal/domain/product"
	"github.com/claudioed/inventory-storage/internal/domain/relocation"
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

// RelocationRepo persists and retrieves Relocation records (ADR 0039).
// FindByID returns nil, nil for an unknown id.
type RelocationRepo interface {
	Save(ctx context.Context, r *relocation.Relocation) error
	FindByID(ctx context.Context, id string) (*relocation.Relocation, error)
	NextID(ctx context.Context) (string, error)
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
	// FindByDemandRefAfter is the keyset-paged form of FindByDemandRef: at
	// most limit reservations of demandRef in ascending (created_at, id)
	// order, strictly after the position (nil = from the start). The caller
	// asks for one more row than it will show to learn whether a next page
	// exists.
	FindByDemandRefAfter(ctx context.Context, demandRef string, after *reservation.PageAfter, limit int) ([]*reservation.Reservation, error)
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

// Outcomes ConfirmPicksForOrder reports to PickConfirmationMetrics, one per
// reservation it looked at (ADR 0035).
const (
	// PickOutcomeConfirmed: an ACTIVE reservation was confirmed as picked.
	PickOutcomeConfirmed = "confirmed"
	// PickOutcomeExpired: the reservation had expired (before, or lazily on
	// this lookup) so the completion could not confirm it. Skipped, never an
	// error: alert on this one, it is stock that went back to usable while the
	// physical pick still happened.
	PickOutcomeExpired = "expired"
	// PickOutcomeAlreadyPicked: already CONFIRMED (a redelivery, or the REST
	// route got there first).
	PickOutcomeAlreadyPicked = "already_picked"
	// PickOutcomeRevoked: REVOKED before the pick completed.
	PickOutcomeRevoked = "revoked"
)

// PickConfirmationMetrics records what the TaskCompleted consumer did with
// each reservation of a completed order, so skipped (expired) confirmations
// are observable. Use cases treat a nil value as "not instrumented".
type PickConfirmationMetrics interface {
	// PickConfirmation adds n to the counter for outcome (one of the
	// PickOutcome* constants).
	PickConfirmation(ctx context.Context, outcome string, n int)
}

// Outcomes BookInboundReceiptLine reports to InboundReceiptMetrics, one per
// handled inbound-receiving ReceiptLineReceived (ADR 0037).
const (
	// InboundReceiptOutcomeBooked: a Good line was booked as a staged receipt.
	InboundReceiptOutcomeBooked = "booked"
	// InboundReceiptOutcomeDamaged: a Damaged line was NOT booked (v1 books
	// Good units only; quarantine is a later ADR).
	InboundReceiptOutcomeDamaged = "damaged_not_booked"
)

// InboundReceiptMetrics records, in units, what the inbound-receiving
// consumer did with each received line, so Damaged units that never reach
// stock stay observable. Use cases treat a nil value as "not instrumented".
type InboundReceiptMetrics interface {
	// InboundReceiptUnits adds units to the counter for outcome (one of the
	// InboundReceiptOutcome* constants).
	InboundReceiptUnits(ctx context.Context, outcome string, units int)
}

// BinLevelLedger advances and reads the per-(bin, sku) level.
type BinLevelLedger interface {
	// Advance bumps the persisted version of (bin, sku) and returns it with
	// the absolute levels read from the ledger AFTER the bump. It must be
	// called inside the UnitOfWork of the ledger change that moved the
	// level: the bump takes a row lock held until that transaction ends, so
	// concurrent changes of the same pair are serialised, the levels read
	// include every change committed before the lock was granted, and a
	// rolled-back change rolls its bump back (versions stay gapless).
	//
	// owner scopes the level to one owner's stock (ADR 0045); the empty owner
	// is fleet-owned stock. Each (bin, sku, owner) has its own version series.
	Advance(ctx context.Context, bin shared.BinId, sku shared.SKU, owner shared.OwnerRef) (shared.BinLevel, error)
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

// ProcessedEventRepo records which inbound CloudEvents a consumer has
// already applied, so at-least-once delivery has an exactly-once effect.
type ProcessedEventRepo interface {
	// Claim records (consumer, eventID). It reports false when the pair was
	// already recorded (a redelivery). Called inside the same UnitOfWork as
	// the effect, so a rolled-back effect also un-claims the id.
	Claim(ctx context.Context, consumer, eventID string) (bool, error)
}

// OrderPickProgressRepo counts, per order (demand_ref), how many PICK tasks
// have completed, so the confirm-pick consumer can confirm the order's
// reservations on the LAST pick (ADR 0035). A fulfillment-execution PICK task
// is per order line and a Reservation has no line identity, so the count is
// the only correlation available.
type OrderPickProgressRepo interface {
	// RecordPick adds one completed pick for demandRef (creating the row at 1)
	// and returns the new count. now stamps updated_at, which the housekeeping
	// sweeper ages out. Called inside the same UnitOfWork as the
	// processed-event claim, so a rolled-back handling also un-counts the pick.
	RecordPick(ctx context.Context, demandRef string, now time.Time) (int, error)
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

// ContainerRepo persists Container aggregates (ADR 0045). FindBy* return nil
// for an unknown container.
type ContainerRepo interface {
	// Add inserts a freshly registered container. It reports false, writing
	// nothing, when the LPN is already taken (a redelivery, or a duplicate
	// SSCC): registration is create-only so it can never overwrite.
	Add(ctx context.Context, c *container.Container) (bool, error)
	// Save updates an existing container, version-guarded (ADR 0019): a stale
	// version fails with usecases.ErrConcurrentModification.
	Save(ctx context.Context, c *container.Container) error
	FindByLPN(ctx context.Context, lpn string) (*container.Container, error)
	FindByStockUnitID(ctx context.Context, stockUnitID string) (*container.Container, error)
	// FindByBin returns the containers stowed in binID (SEALED or OPEN),
	// ordered by LPN.
	FindByBin(ctx context.Context, binID shared.BinId) ([]*container.Container, error)
	// FindByBinAfter is the keyset-paged form of FindByBin: at most limit
	// containers in ascending LPN order whose LPN sorts strictly after
	// afterLPN (empty = from the start). The LPN is unique, so it is the
	// whole keyset.
	FindByBinAfter(ctx context.Context, binID shared.BinId, afterLPN string, limit int) ([]*container.Container, error)
	// FindStowedBySKU returns the SEALED containers of sku and owner that
	// are in a bin, oldest first, then by LPN.
	FindStowedBySKU(ctx context.Context, sku shared.SKU, owner shared.OwnerRef) ([]*container.Container, error)
	// SumQuantity is the eaches held by every container registered for the
	// receipt line, whatever its state.
	SumQuantity(ctx context.Context, receiptID string, lineNo int) (shared.Quantity, error)
}

// PackagingViewRepo reads the local copy of product-master's pack levels
// (the PackagingView, contract R8). Lookups return nil for an unknown level.
type PackagingViewRepo interface {
	// FindByGTIN resolves any GTIN ever declared, including a retired one.
	FindByGTIN(ctx context.Context, gtin string) (*packaging.PackLevel, error)
	// FindActive returns the level of sku in force at instant at.
	FindActive(ctx context.Context, sku shared.SKU, level packaging.Level, at time.Time) (*packaging.PackLevel, error)
}

// PackagingProjection writes the PackagingView from product-master events.
type PackagingProjection interface {
	// ApplyDeclared upserts the level when p.Version is newer than the stored
	// one (an absent row is older than anything). It reports whether the row
	// was written.
	ApplyDeclared(ctx context.Context, p packaging.PackLevel) (bool, error)
	// ApplyRetired end-dates the level of gtin when version is not older than
	// the stored one. It reports whether a row was changed; an unknown GTIN is
	// a no-op (false, nil).
	ApplyRetired(ctx context.Context, gtin string, effectiveTo time.Time, version int64) (bool, error)
}

// ReceiptBookingRepo stores what ReceiptLineReceived booked per receipt line,
// the bound on containers (ADR 0045 section 6).
type ReceiptBookingRepo interface {
	// AddBooking adds qty eaches to the line's booked total (creating it).
	AddBooking(ctx context.Context, receiptID string, lineNo int, sku shared.SKU, owner shared.OwnerRef, qty shared.Quantity) error
	// Booked returns the line's booked eaches, 0 when none.
	Booked(ctx context.Context, receiptID string, lineNo int) (shared.Quantity, error)
}

// PendingLinkRepo stores cartons that could not become Containers yet,
// keyed (receipt id, line no, lpn).
type PendingLinkRepo interface {
	// Add records the link; an existing key is left untouched (idempotent).
	Add(ctx context.Context, l container.PendingLink) error
	// FindPendingByLine returns the PENDING links of a receipt line, oldest first.
	FindPendingByLine(ctx context.Context, receiptID string, lineNo int) ([]container.PendingLink, error)
	// FindPendingBySKU returns the PENDING links of sku, oldest first.
	FindPendingBySKU(ctx context.Context, sku shared.SKU) ([]container.PendingLink, error)
	// Remove deletes a link that became a Container.
	Remove(ctx context.Context, receiptID string, lineNo int, lpn string) error
	// Reject marks a link that can never become a Container, keeping it for audit.
	Reject(ctx context.Context, receiptID string, lineNo int, lpn, reason string) error
}

// BinHandlingUnitLookup answers what a bin accepts, from the event-fed local
// copy of facility-layout's bin attribute (ADR 0045 section 4). A bin with no
// recorded handling unit answers HandlingUnknown.
type BinHandlingUnitLookup interface {
	HandlingUnit(ctx context.Context, bin shared.BinId) (container.HandlingUnit, error)
}
