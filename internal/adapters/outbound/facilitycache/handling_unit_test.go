package facilitycache

import (
	"context"
	"testing"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/inventory-storage/internal/domain/container"
)

func slotMsg(t *testing.T, offset int64, event string, d slotData) kafkago.Message {
	t.Helper()
	return envelopeMsg(t, 0, offset, "locationslot", event, d)
}

func handlingUnitOf(t *testing.T, c *Consumer, bin string) container.HandlingUnit {
	t.Helper()
	hu, err := c.HandlingUnit(context.Background(), mustBinId(t, bin))
	if err != nil {
		t.Fatal(err)
	}
	return hu
}

func TestHandlingUnit_IsFedByLocationSlotRegistered(t *testing.T) {
	c := newTestConsumer([]kafkago.Message{
		slotMsg(t, 0, "LocationSlotRegistered", slotData{LocationCode: "WH1-STOR-AMB-A01-01-01-A", HandlingUnit: "CASE"}),
		slotMsg(t, 1, "LocationSlotRegistered", slotData{LocationCode: "WH1-STOR-AMB-A01-01-01-B", HandlingUnit: "pallet"}),
		slotMsg(t, 2, "LocationSlotRegistered", slotData{LocationCode: "WH1-STOR-AMB-A01-01-01-C", HandlingUnit: "EACH"}),
		slotMsg(t, 3, "LocationSlotRegistered", slotData{LocationCode: "WH1-STOR-AMB-A01-01-01-D"}),
	}, targetOffsets{0: 4})
	drain(t, c)
	want := map[string]container.HandlingUnit{
		"WH1-STOR-AMB-A01-01-01-A": container.HandlingCase,
		"WH1-STOR-AMB-A01-01-01-B": container.HandlingPallet,
		"WH1-STOR-AMB-A01-01-01-C": container.HandlingEach,
		"WH1-STOR-AMB-A01-01-01-D": container.HandlingUnknown,
		"WH1-NOT-A-BIN":            container.HandlingUnknown,
	}
	for bin, hu := range want {
		if got := handlingUnitOf(t, c, bin); got != hu {
			t.Errorf("%s = %q, want %q", bin, got, hu)
		}
	}
}

func TestHandlingUnit_ReRegistrationUpdatesAndClears(t *testing.T) {
	c := newTestConsumer([]kafkago.Message{
		slotMsg(t, 0, "LocationSlotRegistered", slotData{LocationCode: "WH1-STOR-AMB-A01-01-01-A", HandlingUnit: "CASE"}),
		slotMsg(t, 1, "LocationSlotRegistered", slotData{LocationCode: "WH1-STOR-AMB-A01-01-01-A", HandlingUnit: "EACH"}),
	}, targetOffsets{0: 2})
	drain(t, c)
	if got := handlingUnitOf(t, c, "WH1-STOR-AMB-A01-01-01-A"); got != container.HandlingEach {
		t.Fatalf("after re-registration = %q, want EACH", got)
	}
	c2 := newTestConsumer([]kafkago.Message{
		slotMsg(t, 0, "LocationSlotRegistered", slotData{LocationCode: "WH1-STOR-AMB-A01-01-01-A", HandlingUnit: "CASE"}),
		slotMsg(t, 1, "LocationSlotRegistered", slotData{LocationCode: "WH1-STOR-AMB-A01-01-01-A"}),
	}, targetOffsets{0: 2})
	drain(t, c2)
	if got := handlingUnitOf(t, c2, "WH1-STOR-AMB-A01-01-01-A"); got != container.HandlingUnknown {
		t.Fatalf("a registration without the attribute must clear a stale value, got %q", got)
	}
}

func TestHandlingUnit_DecommissionForgetsTheBin(t *testing.T) {
	c := newTestConsumer([]kafkago.Message{
		slotMsg(t, 0, "LocationSlotRegistered", slotData{LocationCode: "WH1-STOR-AMB-A01-01-01-A", HandlingUnit: "CASE"}),
		slotMsg(t, 1, "LocationSlotDecommissioned", slotData{LocationCode: "WH1-STOR-AMB-A01-01-01-A"}),
	}, targetOffsets{0: 2})
	drain(t, c)
	if got := handlingUnitOf(t, c, "WH1-STOR-AMB-A01-01-01-A"); got != container.HandlingUnknown {
		t.Fatalf("decommissioned bin = %q", got)
	}
}
