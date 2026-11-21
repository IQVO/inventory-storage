package facilitycache

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
)

func slotEvent(t *testing.T, eventType, code, locationType string) []byte {
	t.Helper()
	data := map[string]any{"locationCode": code, "aisleId": "WH1-AUTO-ASRS-ASRS1", "zoneId": "WH1-AUTO-ASRS", "role": "Storage"}
	if locationType != "" {
		data["locationType"] = locationType
	}
	name := eventType[len("com.warehouse.wms.facility-layout.locationslot."):]
	raw, err := json.Marshal(map[string]any{
		"specversion": "1.0", "id": uuid.NewString(),
		"source": "/warehouse/facility-layout", "type": eventType, "subject": code, "time": "2026-11-20T12:00:00Z",
		"datacontenttype": "application/json", "dataschema": "urn:warehouse:facility-layout:events:" + name + ":v1", "data": data,
	})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

const (
	registered     = "com.warehouse.wms.facility-layout.locationslot.LocationSlotRegistered"
	decommissioned = "com.warehouse.wms.facility-layout.locationslot.LocationSlotDecommissioned"
	asrsCode       = "WH1-AUTO-ASRS-ASRS1-00-00-S"
)

func TestLocationTypeComesFromLocationSlotRegistered(t *testing.T) {
	c := newTestConsumer(nil, nil)
	if err := c.apply(slotEvent(t, registered, asrsCode, "AutomatedStorage")); err != nil {
		t.Fatal(err)
	}
	if got, err := c.LocationType(context.Background(), asrsCode); err != nil || got != "AutomatedStorage" {
		t.Fatalf("LocationType = %q %v", got, err)
	}
}

func TestLocationTypeIsUnknownForAnUnseenOrDecommissionedBin(t *testing.T) {
	c := newTestConsumer(nil, nil)
	if got, _ := c.LocationType(context.Background(), asrsCode); got != "" {
		t.Fatalf("unseen = %q", got)
	}
	_ = c.apply(slotEvent(t, registered, asrsCode, "AutomatedStorage"))
	if err := c.apply(slotEvent(t, decommissioned, asrsCode, "")); err != nil {
		t.Fatal(err)
	}
	if got, _ := c.LocationType(context.Background(), asrsCode); got != "" {
		t.Fatalf("after decommission = %q", got)
	}
}

func TestARegistrationWithoutTypeClearsAStaleOne(t *testing.T) {
	c := newTestConsumer(nil, nil)
	_ = c.apply(slotEvent(t, registered, asrsCode, "AutomatedStorage"))
	_ = c.apply(slotEvent(t, registered, asrsCode, ""))
	if got, _ := c.LocationType(context.Background(), asrsCode); got != "" {
		t.Fatalf("stale type kept: %q", got)
	}
}
