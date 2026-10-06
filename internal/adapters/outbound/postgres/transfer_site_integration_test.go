//go:build integration

// Site custody + transfer allocation ledger proof at the Postgres repo
// layer: the 0030/0031 migrations' shapes, the FindBySKUAtSite scoping
// (legacy site-less rows excluded), and the ledger's unique constraint
// (the idempotency anchor). Boots its own throwaway Postgres via
// testcontainers (outboxDB) — never an external DATABASE_URL.
package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/claudioed/inventory-storage/internal/adapters/outbound/postgres"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/location"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
	"github.com/claudioed/inventory-storage/internal/domain/transfer"
)

func TestPostgres_StockRepo_SiteCustodyRoundTrip(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()

	locations := postgres.NewLocationRepo(pool)
	repo := postgres.NewStockRepo(pool)

	binID, _ := shared.NewBinId("SITE-BIN-1")
	bin, _ := location.NewBin(binID, mustQty(t, 50))
	if err := locations.Save(ctx, bin); err != nil {
		t.Fatalf("save bin: %v", err)
	}

	site, _ := shared.NewSiteID("SITE-CUSTODY-A")
	sku, _ := shared.NewSKU("SITE-SKU-1")
	unit, err := stock.NewStockUnitAtSite("su-site-1", sku, binID, mustQty(t, 5), site)
	if err != nil {
		t.Fatalf("build unit: %v", err)
	}
	if err := repo.Save(ctx, unit); err != nil {
		t.Fatalf("save unit: %v", err)
	}

	back, err := repo.FindByID(ctx, "su-site-1")
	if err != nil || back == nil {
		t.Fatalf("find: %v %v", back, err)
	}
	if back.SiteID() != site {
		t.Fatalf("site round trip = %q, want %q", back.SiteID(), site)
	}
	if !back.IsAtSite(site) {
		t.Fatal("IsAtSite must hold for the round-tripped custody")
	}
}

func TestPostgres_StockRepo_FindBySKUAtSite_ScopesAndExcludesLegacy(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()

	locations := postgres.NewLocationRepo(pool)
	repo := postgres.NewStockRepo(pool)

	siteA, _ := shared.NewSiteID("SITE-SCOPE-A")
	siteB, _ := shared.NewSiteID("SITE-SCOPE-B")
	sku, _ := shared.NewSKU("SITE-SKU-2")

	for _, id := range []string{"su-a1", "su-a2", "su-b1", "su-legacy"} {
		binID, _ := shared.NewBinId("BIN-" + id)
		bin, _ := location.NewBin(binID, mustQty(t, 20))
		if err := locations.Save(ctx, bin); err != nil {
			t.Fatalf("save bin %s: %v", id, err)
		}
		if id == "su-legacy" {
			// Persisted the pre-0030 way: no site custody.
			legacy, err := stock.NewStockUnit(id, sku, binID, mustQty(t, 99))
			if err != nil {
				t.Fatalf("build legacy: %v", err)
			}
			if err := repo.Save(ctx, legacy); err != nil {
				t.Fatalf("save legacy: %v", err)
			}
			continue
		}
		site := siteA
		if id == "su-b1" {
			site = siteB
		}
		unit, err := stock.NewStockUnitAtSite(id, sku, binID, mustQty(t, 7), site)
		if err != nil {
			t.Fatalf("build %s: %v", id, err)
		}
		if err := repo.Save(ctx, unit); err != nil {
			t.Fatalf("save %s: %v", id, err)
		}
	}

	units, err := repo.FindBySKUAtSite(ctx, sku, siteA)
	if err != nil {
		t.Fatalf("FindBySKUAtSite: %v", err)
	}
	if len(units) != 2 {
		t.Fatalf("SITE-A units = %d, want 2", len(units))
	}
	for _, u := range units {
		if !u.IsAtSite(siteA) {
			t.Fatalf("unit %s leaked into SITE-A's scope", u.ID())
		}
	}

	// Legacy site-less stock answers ORIGIN_SITE_UNKNOWN for every site.
	for _, probe := range []shared.SiteID{siteA, siteB, "SITE-NOWHERE"} {
		got, err := repo.FindBySKUAtSite(ctx, sku, probe)
		if err != nil {
			t.Fatalf("FindBySKUAtSite(%s): %v", probe, err)
		}
		for _, u := range got {
			if u.ID() == "su-legacy" {
				t.Fatalf("legacy site-less unit %s must never be returned for any site", u.ID())
			}
		}
	}
}

func TestPostgres_TransferAllocationRepo_LedgerRoundTripAndUniqueLine(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()

	repo := postgres.NewTransferAllocationRepo(pool)
	site, _ := shared.NewSiteID("SITE-LEDGER-A")
	sku, _ := shared.NewSKU("SITE-SKU-3")

	allocated, err := transfer.NewAllocated("tr-1", "tl-1", site, sku, mustQty(t, 5), "res-1", decidedAtTransfer)
	if err != nil {
		t.Fatalf("NewAllocated: %v", err)
	}
	if err := repo.Save(ctx, allocated); err != nil {
		t.Fatalf("save allocated: %v", err)
	}

	back, err := repo.FindByTransferLineID(ctx, "tl-1")
	if err != nil || back == nil {
		t.Fatalf("find: %v %v", back, err)
	}
	if back.Outcome() != transfer.OutcomeAllocated || back.ReservationID() != "res-1" || back.RequestedQuantity().Int() != 5 {
		t.Fatalf("round trip mismatch: %+v", back)
	}
	if !back.IsReplayOf("tr-1", "tl-1", site, sku, mustQty(t, 5)) {
		t.Fatal("round-tripped row must recognize its own replay")
	}

	// The unique constraint: a second row for the same line fails with
	// the sentinel, whichever outcome it claims.
	dup, _ := transfer.NewRejected("tr-1", "tl-1", site, sku, mustQty(t, 5), transfer.ReasonInsufficientUsable, decidedAtTransfer)
	if err := repo.Save(ctx, dup); err != usecases.ErrTransferLineAlreadyDecided {
		t.Fatalf("duplicate line save err = %v, want ErrTransferLineAlreadyDecided", err)
	}

	// A different line is independent.
	other, _ := transfer.NewRejected("tr-2", "tl-2", site, sku, mustQty(t, 3), transfer.ReasonOriginSiteUnknown, decidedAtTransfer)
	if err := repo.Save(ctx, other); err != nil {
		t.Fatalf("save other line: %v", err)
	}
	got, err := repo.FindByTransferLineID(ctx, "tl-2")
	if err != nil || got == nil || got.RejectionReason() != transfer.ReasonOriginSiteUnknown {
		t.Fatalf("rejected round trip: %v %v", got, err)
	}
}

// decidedAtTransfer is the fixed decision timestamp for the ledger tests.
var decidedAtTransfer = time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
