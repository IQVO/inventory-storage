package shared

import (
	"testing"
	"time"
)

func TestEvents_NameAndOccurredAt(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	sku, _ := NewSKU("SKU-1")
	binID, _ := NewBinId("A-1-1")
	qty, _ := NewQuantity(5)

	tests := []struct {
		name  string
		event DomainEvent
		want  string
	}{
		{"StockReceived", NewStockReceived(now, sku, qty), "StockReceived"},
		{"ItemStowed", NewItemStowed(now, sku, binID, qty), "ItemStowed"},
		{"LocationRecorded", NewLocationRecorded(now, "su-1", binID), "LocationRecorded"},
		{"StockReserved", NewStockReserved(now, "res-1", sku, qty, "order-1"), "StockReserved"},
		{"ReservationExpired", NewReservationExpired(now, "res-1"), "ReservationExpired"},
		{"ReservationRevoked", NewReservationRevoked(now, "res-1"), "ReservationRevoked"},
		{"StockPicked", NewStockPicked(now, "res-1", sku, qty), "StockPicked"},
		{"ItemUnlocated", NewItemUnlocated(now, "su-1", sku, binID, qty), "ItemUnlocated"},
		{"CycleCountCompleted", NewCycleCountCompleted(now, binID, qty, qty, false), "CycleCountCompleted"},
		{"DiscrepancyDetected", NewDiscrepancyDetected(now, binID, qty, qty), "DiscrepancyDetected"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.event.EventName(); got != tt.want {
				t.Fatalf("expected EventName()=%q, got %q", tt.want, got)
			}
			if !tt.event.OccurredAt().Equal(now) {
				t.Fatalf("expected OccurredAt()=%v, got %v", now, tt.event.OccurredAt())
			}
		})
	}
}

func TestItemStowed_CarriesFields(t *testing.T) {
	now := time.Now()
	sku, _ := NewSKU("SKU-1")
	binID, _ := NewBinId("A-1-1")
	qty, _ := NewQuantity(5)

	ev := NewItemStowed(now, sku, binID, qty)
	if ev.SKU != sku || ev.BinID != binID || ev.Quantity != qty {
		t.Fatalf("expected fields to round-trip, got %+v", ev)
	}
}

func TestBinStockLevelChanged_CarriesAbsoluteLevelsAndVersion(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	sku, _ := NewSKU("SKU-1")
	binID, _ := NewBinId("WH1-STOR-AMB-A01-01-01-A")

	ev := NewBinStockLevelChanged(now, "WH1", binID, sku, 9, 4, 7, BinLevelPick)

	if ev.EventName() != "BinStockLevelChanged" || !ev.OccurredAt().Equal(now) {
		t.Fatalf("name/time wrong: %q %v", ev.EventName(), ev.OccurredAt())
	}
	if ev.SiteID != "WH1" || ev.BinID != binID || ev.SKU != sku || ev.OnHand != 9 || ev.Reserved != 4 || ev.Version != 7 || ev.Reason != BinLevelPick {
		t.Fatalf("fields did not round-trip: %+v", ev)
	}
}

func TestBinLevelReasons_AreTheWireValues(t *testing.T) {
	want := map[BinLevelReason]string{
		BinLevelStow: "STOW", BinLevelPick: "PICK", BinLevelRelocate: "RELOCATE", BinLevelReplenishment: "REPLENISHMENT",
		BinLevelCount: "COUNT", BinLevelReserve: "RESERVE", BinLevelRevoke: "REVOKE", BinLevelUnlocated: "UNLOCATED",
	}
	for reason, wire := range want {
		if string(reason) != wire {
			t.Errorf("reason %q, want wire value %q", reason, wire)
		}
	}
}

func TestDiscrepancyDetected_CarriesFields(t *testing.T) {
	now := time.Now()
	binID, _ := NewBinId("A-1-1")
	counted, _ := NewQuantity(3)
	system, _ := NewQuantity(5)

	ev := NewDiscrepancyDetected(now, binID, counted, system)
	if ev.BinID != binID || ev.CountedQty != counted || ev.SystemQty != system {
		t.Fatalf("expected fields to round-trip, got %+v", ev)
	}
}
