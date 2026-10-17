package usecases

import (
	"context"
	"sort"

	"github.com/claudioed/inventory-storage/internal/application/ports"
	"github.com/claudioed/inventory-storage/internal/domain/relocation"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// BinLevelEmitter raises BinStockLevelChanged (ADR 0040) from the same
// transaction as every ledger change that moves a (bin, sku) level.
//
// A use case brackets its writes with Atomically instead of the plain
// atomically helper and calls Track for every (bin, sku) it moved. When the
// OUTERMOST bracket finishes its writes, and before the transaction commits,
// the emitter publishes one event per distinct tracked (bin, sku) carrying the
// FINAL absolute level, so a multi-line pick or reserve that touches a bin
// twice still yields one event for it. A bracket nested inside another (a
// ConfirmPick run by ConfirmPicksForOrder, lazy expiry inside a reserve)
// defers to the outermost one.
//
// A nil *BinLevelEmitter is valid and does nothing: it is the "not
// instrumented" configuration every pre-existing test and wiring uses.
type BinLevelEmitter struct {
	// Ledger bumps the persisted per-(bin, sku) version and reads the level.
	Ledger ports.BinLevelLedger
	// Locations resolves the bin's site; a bin whose site cannot be
	// determined emits nothing (see siteOf).
	Locations ports.LocationRepo
	// Events publishes the event through the same outbox/multi publisher as
	// every other domain event.
	Events ports.EventPublisher
	Clock  ports.Clock
}

type levelKey struct {
	bin shared.BinId
	sku shared.SKU
}

// levelBatch collects the (bin, sku) pairs one outermost bracket moved, in
// first-tracked order, with the last reason that touched each.
type levelBatch struct {
	order   []levelKey
	reasons map[levelKey]shared.BinLevelReason
}

func (b *levelBatch) add(reason shared.BinLevelReason, key levelKey) {
	if _, seen := b.reasons[key]; !seen {
		b.order = append(b.order, key)
	}
	b.reasons[key] = reason
}

type levelBatchCtxKey struct{}

func batchFrom(ctx context.Context) (*levelBatch, bool) {
	b, ok := ctx.Value(levelBatchCtxKey{}).(*levelBatch)
	return b, ok
}

// Atomically is the atomically helper plus level emission: fn runs inside uow
// (when one is wired) and, if it succeeds and this is the outermost bracket,
// the tracked levels are published before the transaction commits.
func (e *BinLevelEmitter) Atomically(ctx context.Context, uow ports.UnitOfWork, fn func(ctx context.Context) error) error {
	if e == nil {
		return atomically(ctx, uow, fn)
	}
	return atomically(ctx, uow, func(ctx context.Context) error {
		if _, nested := batchFrom(ctx); nested {
			return fn(ctx)
		}
		batch := &levelBatch{reasons: make(map[levelKey]shared.BinLevelReason)}
		if err := fn(context.WithValue(ctx, levelBatchCtxKey{}, batch)); err != nil {
			return err
		}
		return e.flush(ctx, batch)
	})
}

// Track records that the ledger moved the level of sku in bin for reason. It
// must be called after the write it describes, inside an Atomically bracket;
// called outside one it publishes at once so a change is never silently
// dropped.
func (e *BinLevelEmitter) Track(ctx context.Context, reason shared.BinLevelReason, bin shared.BinId, sku shared.SKU) error {
	if e == nil {
		return nil
	}
	batch, ok := batchFrom(ctx)
	if !ok {
		batch = &levelBatch{reasons: make(map[levelKey]shared.BinLevelReason)}
		batch.add(reason, levelKey{bin: bin, sku: sku})
		return e.flush(ctx, batch)
	}
	batch.add(reason, levelKey{bin: bin, sku: sku})
	return nil
}

// pendingLevel is one tracked pair with its resolved site.
type pendingLevel struct {
	key    levelKey
	site   shared.SiteID
	reason shared.BinLevelReason
}

// flush publishes one event per tracked pair. Versions are advanced in sorted
// (bin, sku) order so two transactions touching the same pairs lock the
// version rows alike (no deadlock); the events are published in tracked order,
// so a relocation raises its source before its destination.
func (e *BinLevelEmitter) flush(ctx context.Context, batch *levelBatch) error {
	pending, err := e.resolve(ctx, batch)
	if err != nil {
		return err
	}
	levels, err := e.advance(ctx, pending)
	if err != nil {
		return err
	}
	now := e.Clock.Now()
	for _, p := range pending {
		level := levels[p.key]
		ev := shared.NewBinStockLevelChanged(now, p.site, p.key.bin, p.key.sku, level.OnHand, level.Reserved, level.Version, p.reason)
		if err := e.Events.Publish(ctx, ev); err != nil {
			return err
		}
	}
	return nil
}

// resolve attaches each tracked pair's site and drops the pairs whose site is
// unknown.
func (e *BinLevelEmitter) resolve(ctx context.Context, batch *levelBatch) ([]pendingLevel, error) {
	pending := make([]pendingLevel, 0, len(batch.order))
	for _, key := range batch.order {
		site, err := e.siteOf(ctx, key.bin)
		if err != nil {
			return nil, err
		}
		if site == "" {
			continue
		}
		pending = append(pending, pendingLevel{key: key, site: site, reason: batch.reasons[key]})
	}
	return pending, nil
}

// siteOf is the bin's effective site: its recorded custody, else the site
// segment of a facility-layout LocationCode id (ADR 0039). A bin that is not
// registered, or whose site cannot be determined, has no site: the event's
// site_code is mandatory and a consumer cannot place a level without it, so
// such a bin raises no event (ADR 0040) and consumes no version.
func (e *BinLevelEmitter) siteOf(ctx context.Context, id shared.BinId) (shared.SiteID, error) {
	bin, err := e.Locations.FindByID(ctx, id)
	if err != nil {
		return "", err
	}
	if bin == nil {
		return "", nil
	}
	return bin.EffectiveSite(), nil
}

// advance bumps the version and reads the level of every pending pair, in
// sorted key order.
func (e *BinLevelEmitter) advance(ctx context.Context, pending []pendingLevel) (map[levelKey]shared.BinLevel, error) {
	keys := make([]levelKey, 0, len(pending))
	for _, p := range pending {
		keys = append(keys, p.key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].bin != keys[j].bin {
			return keys[i].bin < keys[j].bin
		}
		return keys[i].sku < keys[j].sku
	})
	levels := make(map[levelKey]shared.BinLevel, len(keys))
	for _, key := range keys {
		level, err := e.Ledger.Advance(ctx, key.bin, key.sku)
		if err != nil {
			return nil, err
		}
		levels[key] = level
	}
	return levels, nil
}

// replenishmentRelocationReason is the relocation reason the later
// pick-face replenishment change introduces. No relocation can carry it
// today (relocation.ParseReason rejects it), so REPLENISHMENT levels are never
// emitted yet; the mapping below is the code path that change switches on.
const replenishmentRelocationReason = "REPLENISHMENT"

// relocationLevelReason maps a relocation's own reason to the level event's
// reason: REPLENISHMENT stays REPLENISHMENT, every other reason is RELOCATE.
func relocationLevelReason(reason relocation.Reason) shared.BinLevelReason {
	if string(reason) == replenishmentRelocationReason {
		return shared.BinLevelReplenishment
	}
	return shared.BinLevelRelocate
}
