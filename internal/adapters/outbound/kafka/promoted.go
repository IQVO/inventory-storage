package kafka

import "github.com/claudioed/inventory-storage/internal/domain/shared"

// binEntity is the `<entity>` segment for the cycle-count facts the Bin
// aggregate raises (DiscrepancyDetected).
const binEntity = "bin"

// Promoted exception facts (ADR 0043). DiscrepancyDetected, ItemUnlocated and
// ItemStowed were analytics-only until exception-management (the first two)
// and returns-processing (ItemStowed) needed them as a cross-context
// contract. They now ALSO reach the integration topic with the same `type`,
// `subject`, Kafka key and byte-identical `data` the analytics publisher
// emits (TestPromoted_DataIsByteIdenticalAcrossTopics pins that); only the
// `dataschema` stream differs (`...:events:<Event>:v1`). The three payloads
// below must stay in lock-step with analytics_publisher.go: a change here is
// a wire-contract change (new `.v2` type + dataschema), never an edit.

// encodePromotedEvent maps one promoted event onto its wire data payload,
// Kafka key, CloudEvents subject and entity segment. A nil data return means
// the event is not promoted (the caller treats it as outside the contract).
func encodePromotedEvent(event shared.DomainEvent) (data any, key, subject, entity string) {
	switch e := event.(type) {
	case shared.ItemStowed:
		// Key = subject = SKU, as on the analytics topic.
		return itemStowedData(e), e.SKU.String(), e.SKU.String(), stockEntity
	case shared.ItemUnlocated:
		// Key = SKU (every fact about one SKU's units stays on one
		// partition); subject = the stock unit that was lost.
		return itemUnlocatedData(e), e.SKU.String(), e.StockUnitID, stockEntity
	case shared.DiscrepancyDetected:
		// Key = subject = bin id: the Bin is the aggregate the count is about.
		return discrepancyDetectedData(e), e.BinID.String(), e.BinID.String(), binEntity
	default:
		return nil, "", "", ""
	}
}

// itemStowedData is the ItemStowed `data` (v1, frozen): {sku, bin_id, quantity}.
func itemStowedData(e shared.ItemStowed) map[string]any {
	return map[string]any{
		"sku":      e.SKU.String(),
		"bin_id":   e.BinID.String(),
		"quantity": e.Quantity.Int(),
	}
}

// itemUnlocatedData is the ItemUnlocated `data` (v1, frozen):
// {sku, bin_id, stock_unit_id, quantity}.
func itemUnlocatedData(e shared.ItemUnlocated) map[string]any {
	return map[string]any{
		"sku":           e.SKU.String(),
		"bin_id":        e.BinID.String(),
		"stock_unit_id": e.StockUnitID,
		"quantity":      e.Quantity.Int(),
	}
}

// discrepancyDetectedData is the DiscrepancyDetected `data` (v1, frozen):
// {bin_id, counted, system}.
func discrepancyDetectedData(e shared.DiscrepancyDetected) map[string]any {
	return map[string]any{
		"bin_id":  e.BinID.String(),
		"counted": e.CountedQty.Int(),
		"system":  e.SystemQty.Int(),
	}
}
