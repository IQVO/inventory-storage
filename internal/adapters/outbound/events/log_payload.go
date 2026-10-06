package events

import (
	"time"

	"github.com/claudioed/inventory-storage/internal/domain/product"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// The structs below are the LogPublisher's wire shape. They are owned by
// this adapter, not by the domain: domain events carry no serialisation
// tags (the architecture fitness test enforces it), so the JSON field names
// live here and are pinned byte-for-byte by
// TestLogPublisher_PayloadMatchesGolden.

type eventHeaderDTO struct {
	EventName  string    `json:"eventName"`
	OccurredAt time.Time `json:"occurredAt"`
}

type stockReceivedDTO struct {
	eventHeaderDTO
	SKU      string
	Quantity int
}

type itemStowedDTO struct {
	eventHeaderDTO
	SKU      string
	BinID    string
	Quantity int
}

type locationRecordedDTO struct {
	eventHeaderDTO
	StockUnitID string
	BinID       string
}

type stockReservedDTO struct {
	eventHeaderDTO
	ReservationID string
	SKU           string
	Quantity      int
	DemandRef     string
}

type reservationDTO struct {
	eventHeaderDTO
	ReservationID string
}

type stockPickedDTO struct {
	eventHeaderDTO
	ReservationID string
	SKU           string
	Quantity      int
}

type itemUnlocatedDTO struct {
	eventHeaderDTO
	StockUnitID string
	SKU         string
	BinID       string
	Quantity    int
}

type cycleCountCompletedDTO struct {
	eventHeaderDTO
	BinID       string
	CountedQty  int
	SystemQty   int
	Discrepancy bool
}

type discrepancyDetectedDTO struct {
	eventHeaderDTO
	BinID      string
	CountedQty int
	SystemQty  int
}

// productClassifiedDTO deliberately has no eventName/occurredAt header: the
// log shape of ProductClassified has always been its bare field set.
type productClassifiedDTO struct {
	SKU              string
	HandlingTags     []string
	TemperatureClass string
	DOTHazardClass   int
	At               time.Time
}

func headerOf(e shared.DomainEvent) eventHeaderDTO {
	return eventHeaderDTO{EventName: e.EventName(), OccurredAt: e.OccurredAt()}
}

// logPayload maps a domain event to its adapter-owned JSON DTO. An event
// type this adapter does not know yet degrades to the bare header, so a new
// domain event is still logged (name + time) before it earns a DTO.
func logPayload(event shared.DomainEvent) any {
	switch e := event.(type) {
	case shared.StockReceived:
		return stockReceivedDTO{headerOf(e), e.SKU.String(), e.Quantity.Int()}
	case shared.ItemStowed:
		return itemStowedDTO{headerOf(e), e.SKU.String(), e.BinID.String(), e.Quantity.Int()}
	case shared.LocationRecorded:
		return locationRecordedDTO{headerOf(e), e.StockUnitID, e.BinID.String()}
	case shared.StockReserved:
		return stockReservedDTO{headerOf(e), e.ReservationID, e.SKU.String(), e.Quantity.Int(), e.DemandRef}
	case shared.ReservationExpired:
		return reservationDTO{headerOf(e), e.ReservationID}
	case shared.ReservationRevoked:
		return reservationDTO{headerOf(e), e.ReservationID}
	case shared.StockPicked:
		return stockPickedDTO{headerOf(e), e.ReservationID, e.SKU.String(), e.Quantity.Int()}
	case shared.ItemUnlocated:
		return itemUnlocatedDTO{headerOf(e), e.StockUnitID, e.SKU.String(), e.BinID.String(), e.Quantity.Int()}
	case shared.CycleCountCompleted:
		return cycleCountCompletedDTO{headerOf(e), e.BinID.String(), e.CountedQty.Int(), e.SystemQty.Int(), e.Discrepancy}
	case shared.DiscrepancyDetected:
		return discrepancyDetectedDTO{headerOf(e), e.BinID.String(), e.CountedQty.Int(), e.SystemQty.Int()}
	case product.ProductClassified:
		return productClassifiedFor(e)
	default:
		return headerOf(event)
	}
}

func productClassifiedFor(e product.ProductClassified) productClassifiedDTO {
	// Preserve nil-ness: the original encoding rendered a nil slice as null.
	var tags []string
	if e.HandlingTags != nil {
		tags = make([]string, len(e.HandlingTags))
		for i, t := range e.HandlingTags {
			tags[i] = string(t)
		}
	}
	return productClassifiedDTO{
		SKU:              e.SKU.String(),
		HandlingTags:     tags,
		TemperatureClass: string(e.TemperatureClass),
		DOTHazardClass:   int(e.DOTHazardClass),
		At:               e.At,
	}
}
