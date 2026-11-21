package memory_test

import (
	"context"
	"errors"
	"testing"

	"github.com/claudioed/inventory-storage/internal/adapters/outbound/memory"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/container"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
)

var bg = context.Background()

func TestToteRepoAddIsCreateOnly(t *testing.T) {
	r := memory.NewToteRepo(nil)
	a, _ := container.RegisterTote("TOTE-1")
	if ok, err := r.Add(bg, a); !ok || err != nil {
		t.Fatalf("first add = %v %v", ok, err)
	}
	b, _ := container.RegisterTote("TOTE-1")
	if ok, _ := r.Add(bg, b); ok {
		t.Fatal("second add of one LPN reported created")
	}
}

func TestToteRepoSaveIsVersionGuarded(t *testing.T) {
	r := memory.NewToteRepo(nil)
	tote, _ := container.RegisterTote("TOTE-1")
	_, _ = r.Add(bg, tote)
	loaded, _ := r.FindByLPN(bg, "TOTE-1")
	_ = loaded.PlaceIn("B-1", "WH1")
	if err := r.Save(bg, loaded); err != nil {
		t.Fatal(err)
	}
	stale := container.RehydrateTote(container.ToteSnapshot{LPN: "TOTE-1", Version: 1})
	if err := r.Save(bg, stale); !errors.Is(err, usecases.ErrConcurrentModification) {
		t.Fatalf("stale save err = %v", err)
	}
	got, _ := r.FindByLPN(bg, "TOTE-1")
	if got.BinID() != "B-1" || got.Version() != 2 {
		t.Fatalf("stored = %+v", got.Snapshot())
	}
}

func TestToteRepoFindByBinAfterPagesInLPNOrder(t *testing.T) {
	r := memory.NewToteRepo(nil)
	for _, lpn := range []string{"T-3", "T-1", "T-2", "T-4"} {
		tote, _ := container.RegisterTote(lpn)
		bin := shared.BinId("B-1")
		if lpn == "T-4" {
			bin = "B-2"
		}
		_ = tote.PlaceIn(bin, "WH1")
		_, _ = r.Add(bg, tote)
	}
	page, _ := r.FindByBinAfter(bg, "B-1", "T-1", 10)
	if len(page) != 2 || page[0].LPN() != "T-2" || page[1].LPN() != "T-3" {
		t.Fatalf("page = %v", page)
	}
	if first, _ := r.FindByBinAfter(bg, "B-1", "", 1); len(first) != 1 || first[0].LPN() != "T-1" {
		t.Fatalf("limit 1 = %v", first)
	}
}

func TestStockRepoFindByContainer(t *testing.T) {
	r := memory.NewStockRepo()
	a, _ := stock.NewToteUnit("su-2", "SKU-1", "B-1", 3, "WH1", "", "TOTE-1")
	b, _ := stock.NewToteUnit("su-1", "SKU-2", "B-1", 4, "WH1", "", "TOTE-1")
	c, _ := stock.NewToteUnit("su-3", "SKU-1", "B-1", 5, "WH1", "", "TOTE-2")
	for _, u := range []*stock.StockUnit{a, b, c} {
		_ = r.Save(bg, u)
	}
	got, _ := r.FindByContainer(bg, "TOTE-1")
	if len(got) != 2 || got[0].ID() != "su-1" || got[1].ID() != "su-2" {
		t.Fatalf("contents = %v", got)
	}
}
