package facilitycache

import (
	"context"
	"fmt"
	"strings"

	"github.com/claudioed/inventory-storage/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/inventory-storage/internal/domain/container"
	"github.com/claudioed/inventory-storage/internal/domain/product"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// apply decodes one CloudEvents 1.0 event and updates the cache. Anything
// cloudevents.Decode rejects (the retired flat envelope, bad JSON, missing
// required attributes) is returned as an error so Run dead-letters it —
// it is never parsed as a legacy shape. Every update is an idempotent
// upsert/delete keyed by the aggregate, so a redelivered event (same
// CloudEvents id) re-applies harmlessly and needs no separate id dedupe.
func (c *Consumer) apply(value []byte) error {
	e, err := cloudevents.Decode(value)
	if err != nil {
		return fmt.Errorf("facilitycache: %w", err)
	}

	switch e.Type() {
	case typeZoneRegistered:
		return c.applyZoneRegistered(e.DataAs)
	case typeLocationSlotRegistered:
		return c.applySlotRegistered(e.DataAs)
	case typeLocationSlotHandlingUnitDeclared:
		return c.applyHandlingUnitDeclared(e.DataAs)
	case typeLocationSlotDecommissioned:
		var data slotData
		if err := e.DataAs(&data); err != nil {
			return fmt.Errorf("facilitycache: decode %s: %w", typeLocationSlotDecommissioned, err)
		}
		c.mu.Lock()
		delete(c.slots, data.LocationCode)
		delete(c.handlingUnits, data.LocationCode)
		c.mu.Unlock()
	default:
		// Every other event on this topic (SiteRegistered, AisleRegistered,
		// LocationTypeRegistered, PlacementRuleDefined, FacilityLayoutImported)
		// carries nothing this cache needs. Ignoring them is correct, not a
		// gap: they still count toward the readiness target because observe()
		// runs for every message regardless.
	}
	return nil
}

// applyZoneRegistered caches a zone's classification attributes.
func (c *Consumer) applyZoneRegistered(dataAs func(any) error) error {
	var data zoneData
	if err := dataAs(&data); err != nil {
		return fmt.Errorf("facilitycache: decode %s: %w", typeZoneRegistered, err)
	}
	if data.ZoneID == "" {
		return fmt.Errorf("facilitycache: %s with empty zoneId", typeZoneRegistered)
	}
	temperatureClass, err := product.ParseTemperatureClass(data.TemperatureClass)
	if err != nil {
		temperatureClass = ""
	}
	c.mu.Lock()
	c.zones[data.ZoneID] = product.SlotAttributes{
		Hazmat:           data.Hazmat,
		TemperatureClass: temperatureClass,
		Known:            true,
	}
	c.mu.Unlock()
	return nil
}

// applySlotRegistered caches a location slot -> zone binding.
func (c *Consumer) applySlotRegistered(dataAs func(any) error) error {
	var data slotData
	if err := dataAs(&data); err != nil {
		return fmt.Errorf("facilitycache: decode %s: %w", typeLocationSlotRegistered, err)
	}
	if data.LocationCode == "" {
		return fmt.Errorf("facilitycache: %s with empty locationCode", typeLocationSlotRegistered)
	}
	zoneID := data.ZoneID
	if zoneID == "" {
		// Derive the zone identity from the code itself, mirroring
		// facility-layout's own LocationCode.ZoneID() (site-area-zone,
		// the first three of seven hyphen-separated segments), so a
		// payload that omits the field still resolves.
		zoneID = zoneIDFromLocationCode(data.LocationCode)
	}
	c.mu.Lock()
	c.slots[data.LocationCode] = zoneID
	c.setHandlingUnit(data.LocationCode, container.ParseHandlingUnit(data.HandlingUnit))
	c.mu.Unlock()
	return nil
}

// applyHandlingUnitDeclared updates a bin's handling unit from a
// LocationSlotHandlingUnitDeclared event. It is an idempotent last-write-wins
// upsert on the bin's code, so a redelivery re-applies harmlessly. A value
// that is not EACH/CASE/PALLET is stored as "none recorded", the fail-closed
// answer for a sealed case (the same reading ParseHandlingUnit gives a
// registration), rather than poisoning the partition.
func (c *Consumer) applyHandlingUnitDeclared(dataAs func(any) error) error {
	var data slotData
	if err := dataAs(&data); err != nil {
		return fmt.Errorf("facilitycache: decode %s: %w", typeLocationSlotHandlingUnitDeclared, err)
	}
	if data.LocationCode == "" {
		return fmt.Errorf("facilitycache: %s with empty locationCode", typeLocationSlotHandlingUnitDeclared)
	}
	c.mu.Lock()
	c.setHandlingUnit(data.LocationCode, container.ParseHandlingUnit(data.HandlingUnit))
	c.mu.Unlock()
	return nil
}

// setHandlingUnit records (or clears) a bin's handling unit. The caller holds
// c.mu. A registration that carries none clears a stale value.
func (c *Consumer) setHandlingUnit(code string, hu container.HandlingUnit) {
	if hu == container.HandlingUnknown {
		delete(c.handlingUnits, code)
		return
	}
	if c.handlingUnits == nil {
		c.handlingUnits = make(map[string]container.HandlingUnit)
	}
	c.handlingUnits[code] = hu
}

// HandlingUnit satisfies ports.BinHandlingUnitLookup from the local copy. It
// never performs I/O. A bin with no recorded handling unit (every bin before
// facility-layout ships the attribute, or an unknown bin) answers
// HandlingUnknown, which fails closed for a sealed case.
func (c *Consumer) HandlingUnit(_ context.Context, bin shared.BinId) (container.HandlingUnit, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.handlingUnits[bin.String()], nil
}

// zoneIDFromLocationCode mirrors facility-layout's LocationCode.ZoneID():
// the site, area and zone segments joined, e.g.
// "WH1-STOR-AMB-A07-03-02-B" -> "WH1-STOR-AMB". A code that does not have
// at least three segments yields "", which resolves to Known=false.
func zoneIDFromLocationCode(code string) string {
	parts := strings.Split(code, "-")
	if len(parts) < 3 {
		return ""
	}
	return strings.Join(parts[:3], "-")
}
