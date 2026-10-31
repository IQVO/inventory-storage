package kafka_test

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/claudioed/inventory-storage/internal/adapters/outbound/kafka"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/memory"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// Golden exact-JSON tests for the container events and the owner-scoped
// BinStockLevelChanged (ADR 0045, 0046). A change here is a wire-contract
// change: a new .v2 type, never an edit.

const testLPN = "003400000000000017"

func goldenFile(t *testing.T, name string) string {
	t.Helper()
	raw, err := os.ReadFile("testdata/" + name) // #nosec G304 -- fixed testdata path
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	return string(raw)
}

func containerEncoder() *kafka.Publisher {
	pub := kafka.NewPublisher(&fakeWriter{}, memory.NewReservationRepo())
	pub.NewID = fixedID
	return pub
}

func TestGolden_Container_Events(t *testing.T) {
	sku := mustSKU(t, "SKU-1")
	cases := []struct {
		name, file, eventType string
		event                 shared.DomainEvent
	}{
		{"registered", "container_registered.v1.golden.json", "com.warehouse.wms.inventory-storage.container.ContainerRegistered",
			shared.NewContainerRegistered(goldenAt, testLPN, sku, "24006381333935", 12, newQty(t, 12), "seller-a", "L1", "2027-03-01", "R-1", 1)},
		{"stowed", "container_stowed.v1.golden.json", "com.warehouse.wms.inventory-storage.container.ContainerStowed",
			shared.NewContainerStowed(goldenAt, testLPN, sku, newQty(t, 12), "seller-a", "WH1", mustBin(t, "WH1-STOR-AMB-A01-01-01-A"))},
		{"broken", "case_broken.v1.golden.json", "com.warehouse.wms.inventory-storage.container.CaseBroken",
			shared.NewCaseBroken(goldenAt, testLPN, sku, newQty(t, 12), "seller-a", mustBin(t, "WH1-STOR-AMB-A01-01-01-A"), mustBin(t, "WH1-STOR-AMB-A01-01-02-A"))},
		{"emptied", "container_emptied.v1.golden.json", "com.warehouse.wms.inventory-storage.container.ContainerEmptied",
			shared.NewContainerEmptied(goldenAt, testLPN, sku, "seller-a", "WH1", shared.ContainerEmptiedBroken)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			enc := encodeOne(t, containerEncoder(), tc.event)
			assertGolden(t, enc, testLPN, goldenFile(t, tc.file))
			if enc.Topic != "warehouse.inventory.events" || enc.EventType != tc.eventType {
				t.Errorf("topic %q type %q", enc.Topic, enc.EventType)
			}
		})
	}
}

// A fleet-owned container carries no owner_ref and no empty optional fields.
func TestGolden_Container_FleetOwnedOmitsOptionalFields(t *testing.T) {
	ev := shared.NewContainerRegistered(goldenAt, testLPN, mustSKU(t, "SKU-1"), "24006381333935", 12, newQty(t, 12), "", "", "", "R-1", 1)
	var e struct {
		Data map[string]any `json:"data"`
	}
	if err := json.Unmarshal(encodeOne(t, containerEncoder(), ev).Value, &e); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"owner_ref", "lot", "expiry"} {
		if _, present := e.Data[k]; present {
			t.Errorf("%s must be omitted when empty: %v", k, e.Data)
		}
	}
}

func TestGolden_Integration_BinStockLevelChanged_OwnerScoped(t *testing.T) {
	ev := shared.NewBinStockLevelChanged(goldenAt, "WH1", mustBin(t, "WH1-STOR-AMB-A01-01-01-A"), mustSKU(t, "SKU-1"),
		newQty(t, 12), 0, 3, shared.BinLevelStow).ForOwner("seller-a")
	assertGolden(t, encodeOne(t, containerEncoder(), ev), "WH1-STOR-AMB-A01-01-01-A:SKU-1", goldenFile(t, "bin_stock_level_changed.owner.v1.golden.json"))
}

// Fleet-owned stock stays byte-identical to v1 before owners existed.
func TestGolden_Integration_BinStockLevelChanged_FleetUnchanged(t *testing.T) {
	enc := encodeOne(t, containerEncoder(), binLevelEvent(t))
	assertGolden(t, enc, "WH1-STOR-AMB-A01-01-01-A:SKU-1", string(binLevelGoldenBytes(t)))
}

func TestContainerEventsAreOutsideTheAnalyticsContract(t *testing.T) {
	pub := kafka.NewAnalyticsPublisher(nil, memory.NewReservationRepo(), nil)
	for _, ev := range []shared.DomainEvent{
		shared.NewContainerStowed(goldenAt, testLPN, mustSKU(t, "SKU-1"), 12, "", "WH1", "BIN"),
		shared.NewCaseBroken(goldenAt, testLPN, mustSKU(t, "SKU-1"), 12, "", "A", "B"),
	} {
		msgs, err := pub.Encode(t.Context(), ev)
		if err != nil || len(msgs) != 0 {
			t.Errorf("%s on the analytics topic = %d messages, %v; want none", ev.EventName(), len(msgs), err)
		}
	}
}
