package main_test

// Wave-2 step definitions for the godog suite. They reuse the world/server
// plumbing in features_test.go and add: the placement lookup fake behind
// stow-time hazmat/temperature rules, the transfer-saga fixtures, and the
// Kafka-consumer use cases (inbound-receiving, fulfillment-execution), which
// are driven through the SAME use case the consumer adapter calls — the
// scenarios assert only through REST or published domain events.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"

	"github.com/cucumber/godog"

	inboundhttp "github.com/claudioed/inventory-storage/internal/adapters/inbound/http"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/memory"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/location"
	"github.com/claudioed/inventory-storage/internal/domain/product"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
)

// wave2State is the per-scenario state added by the wave-2 steps.
type wave2State struct {
	slots            *fakeSlotLookup
	bookInbound      *usecases.BookInboundReceiptLine
	confirmPicks     *usecases.ConfirmPicksForOrder
	allocateTransfer *usecases.AllocateTransferStock
	// allocateErr is the outcome of the last allocation command in the
	// transfer-saga scenarios (features_transfer_saga_test.go).
	allocateErr error
}

// fakeSlotLookup stands in for the synchronous facility-layout read behind
// stow-time placement rules. A bin nobody described comes back Known=false
// (the documented fail-open default); unavailable simulates a transport error.
type fakeSlotLookup struct {
	slots       map[shared.BinId]product.SlotAttributes
	unavailable bool
}

func (f *fakeSlotLookup) GetSlotAttributes(_ context.Context, binID shared.BinId) (product.SlotAttributes, error) {
	if f.unavailable {
		return product.SlotAttributes{}, errors.New("facility-layout is unreachable")
	}
	return f.slots[binID], nil
}

// wireWave2 adds the use cases the wave-1 composition root leaves out
// (transfer custody, placement rules) and builds the consumer-side use cases
// over the same repositories and publisher the REST server uses.
func (w *world) wireWave2(s *inboundhttp.Server, reservations *memory.ReservationRepo) {
	allocations := memory.NewTransferAllocationRepo()
	receipts := memory.NewTransferReceiptRepo()
	slots := &fakeSlotLookup{slots: map[shared.BinId]product.SlotAttributes{}}

	s.StowStock.Classifications = w.classifications
	s.StowStock.LocationLookup = slots
	s.StageTransferReceipt = &usecases.StageTransferReceipt{
		Transfers: allocations, Receipts: receipts, Exceptions: memory.NewInventoryExceptionRepo(),
		Events: w.publisher, Clock: w.clock,
	}
	s.StowTransferStock = &usecases.StowTransferStock{
		Receipts: receipts, Stock: w.stock, Locations: w.locations, Events: w.publisher, Clock: w.clock,
	}

	w.wave2 = wave2State{
		slots: slots,
		bookInbound: &usecases.BookInboundReceiptLine{
			Receive:         &usecases.ReceiveStock{Events: w.publisher, Clock: w.clock},
			ProcessedEvents: memory.NewProcessedEventRepo(),
		},
		confirmPicks: &usecases.ConfirmPicksForOrder{
			Stock: w.stock, Reservations: reservations, Events: w.publisher, Clock: w.clock,
			Confirm: &usecases.ConfirmPick{
				Stock: w.stock, Locations: w.locations, Reservations: reservations, Events: w.publisher, Clock: w.clock,
			},
			ProcessedEvents: memory.NewProcessedEventRepo(),
			PickProgress:    memory.NewOrderPickProgressRepo(),
		},
		allocateTransfer: &usecases.AllocateTransferStock{
			Stock: w.stock, Reservations: reservations, Transfers: allocations, Events: w.publisher, Clock: w.clock,
		},
	}
}

// getJSON reads an endpoint out of band, so a Given or a sanity check never
// clobbers the response the surrounding Then steps assert against.
func (w *world) getJSON(ctx context.Context, path string, dest any) error {
	status, body, _, err := w.call(ctx, http.MethodGet, path, nil)
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return fmt.Errorf("GET %s: expected 200, got %d: %s", path, status, string(body))
	}
	return decodeBody(body, dest)
}

func decodeBody(body []byte, dest any) error {
	if err := json.Unmarshal(body, dest); err != nil {
		return fmt.Errorf("response body is not valid JSON (%w): %s", err, string(body))
	}
	return nil
}

// -------------------------------------------------------- stow placement --

// classify applies a product-master classification through the consumer's
// use case (ADR 0034).
func (w *world) classify(ctx context.Context, sku string, tags []string, temperature string, dot, version int) error {
	_, err := w.applyClassification.Execute(ctx, usecases.ProductClassificationUpdate{
		EventID:              fmt.Sprintf("pm-%s-v%d", sku, version),
		SKU:                  sku,
		HandlingTags:         tags,
		TemperatureClass:     temperature,
		DOTHazardClass:       dot,
		ClassificationSource: "native",
		Version:              int64(version),
	})
	return err
}

func (w *world) skuIsClassifiedHazmat(ctx context.Context, sku string, dot int) error {
	return w.classify(ctx, sku, []string{"Hazmat"}, "", dot, 1)
}

func (w *world) skuIsClassifiedTemperatureSensitive(ctx context.Context, sku, class string) error {
	return w.classify(ctx, sku, []string{"TemperatureSensitive"}, class, 0, 1)
}

func (w *world) binSitsInHazmatZone(binID string) error {
	w.wave2.slots.slots[shared.BinId(binID)] = product.SlotAttributes{Hazmat: true, TemperatureClass: product.Ambient, Known: true}
	return nil
}

func (w *world) binSitsInNonHazmatZone(binID, class string) error {
	w.wave2.slots.slots[shared.BinId(binID)] = product.SlotAttributes{TemperatureClass: product.TemperatureClass(class), Known: true}
	return nil
}

func (w *world) lookupIsUnavailable() error {
	w.wave2.slots.unavailable = true
	return nil
}

// ------------------------------------------------------------ reservations --

func (w *world) iReserveOnLine(ctx context.Context, qty int, sku, demandRef string, lineNo int) error {
	err := w.record(ctx, http.MethodPost, "/reservations", map[string]any{"sku": sku, "quantity": qty, "demandRef": demandRef, "lineNo": lineNo})
	if err != nil {
		return err
	}
	if w.status == http.StatusCreated {
		var res struct {
			ID string `json:"id"`
		}
		if err := w.decode(&res); err != nil {
			return err
		}
		w.reservationID = res.ID
	}
	return nil
}

func (w *world) aReservationOnLine(ctx context.Context, qty int, sku, demandRef string, lineNo int) error {
	if err := w.iReserveOnLine(ctx, qty, sku, demandRef, lineNo); err != nil {
		return err
	}
	if w.status != http.StatusCreated {
		return fmt.Errorf("reserving %d of %q on line %d: expected 201, got %d: %s", qty, sku, lineNo, w.status, string(w.body))
	}
	return nil
}

func (w *world) iRevokeReservationWithID(ctx context.Context, id string) error {
	return w.record(ctx, http.MethodDelete, "/reservations/"+url.PathEscape(id), nil)
}

func (w *world) iConfirmPickForReservationWithID(ctx context.Context, id string) error {
	return w.record(ctx, http.MethodPost, "/reservations/"+url.PathEscape(id)+"/pick-confirmations", nil)
}

func (w *world) theReservationResponseHasAllocations(expected int) error {
	var res struct {
		Allocations []allocationBody `json:"allocations"`
	}
	if err := w.decode(&res); err != nil {
		return err
	}
	if len(res.Allocations) != expected {
		return fmt.Errorf("expected %d allocations, got %d: %s", expected, len(res.Allocations), string(w.body))
	}
	return nil
}

func (w *world) theReservationResponseReportsLine(expected int) error {
	var res struct {
		LineNo *int `json:"lineNo"`
	}
	if err := w.decode(&res); err != nil {
		return err
	}
	if res.LineNo == nil || *res.LineNo != expected {
		return fmt.Errorf("expected the Reservation to report line %d, got %v", expected, res.LineNo)
	}
	return nil
}

// reservationView is the slice of the Reservation DTO the wave-2 assertions read.
type reservationView struct {
	ID     string `json:"id"`
	LineNo *int   `json:"lineNo"`
	Status string `json:"status"`
}

func (w *world) reservationsFor(ctx context.Context, demandRef string) ([]reservationView, error) {
	var page struct {
		Items []reservationView `json:"items"`
	}
	if err := w.getJSON(ctx, "/reservations?demandRef="+url.QueryEscape(demandRef), &page); err != nil {
		return nil, err
	}
	return page.Items, nil
}

func (w *world) theReservationOnLineHasStatus(ctx context.Context, demandRef string, lineNo int, status string) error {
	list, err := w.reservationsFor(ctx, demandRef)
	if err != nil {
		return err
	}
	for _, res := range list {
		if res.LineNo != nil && *res.LineNo == lineNo {
			if res.Status != status {
				return fmt.Errorf("expected line %d of demand %q to be %s, got %s", lineNo, demandRef, status, res.Status)
			}
			return nil
		}
	}
	return fmt.Errorf("demand %q has no Reservation on line %d (%d reservations)", demandRef, lineNo, len(list))
}

func (w *world) everyReservationForDemandHasStatus(ctx context.Context, demandRef, status string) error {
	list, err := w.reservationsFor(ctx, demandRef)
	if err != nil {
		return err
	}
	if len(list) == 0 {
		return fmt.Errorf("demand %q has no Reservations", demandRef)
	}
	for _, res := range list {
		if res.Status != status {
			return fmt.Errorf("expected every Reservation of demand %q to be %s, but %s is %s", demandRef, status, res.ID, res.Status)
		}
	}
	return nil
}

// ------------------------------------------------------------- cycle count --

func (w *world) iRunACycleCountWithNegativeQuantity(ctx context.Context, binID string, counted int) error {
	return w.record(ctx, http.MethodPost, "/bins/"+url.PathEscape(binID)+"/cycle-counts", map[string]any{"countedQuantity": -counted})
}

func (w *world) iRunACycleCountWithoutQuantity(ctx context.Context, binID string) error {
	return w.record(ctx, http.MethodPost, "/bins/"+url.PathEscape(binID)+"/cycle-counts", map[string]any{})
}

func (w *world) iRegisterBinWithoutCapacity(ctx context.Context, binID string) error {
	return w.record(ctx, http.MethodPut, "/bins/"+url.PathEscape(binID), map[string]any{})
}

// ------------------------------------------------------------------ events --

func (w *world) theDomainEventWasPublishedTimes(name string, expected int) error {
	published := 0
	for _, e := range w.publisher.Events() {
		if e.EventName() == name {
			published++
		}
	}
	if published != expected {
		return fmt.Errorf("expected domain event %q to be published %d time(s), got %d", name, expected, published)
	}
	return nil
}

// ------------------------------------------------------ inbound-receiving --

// inboundReportsLine hands a ReceiptLineReceived to the consumer's use case.
// The line id doubles as the CloudEvents id, so repeating a step redelivers.
func (w *world) inboundReportsLine(ctx context.Context, lineID string, qty int, sku, condition string) error {
	_, err := w.wave2.bookInbound.Execute(ctx, usecases.InboundReceiptLine{
		EventID: "evt-" + lineID, ReceiptID: "rcpt-1", ASNNumber: "ASN-1", LineNo: 1,
		SKU: sku, Quantity: qty, Condition: condition,
	})
	return err
}

// ---------------------------------------------------- fulfillment-execution --

// fulfillmentReportsTask hands a TaskCompleted to the consumer's use case.
// The task id doubles as the CloudEvents id, so repeating a step redelivers.
func (w *world) fulfillmentReportsTask(ctx context.Context, taskType, taskID, orderRef, line string) error {
	in := usecases.PickCompletion{EventID: "evt-" + taskID, TaskID: taskID, TaskType: taskType, OrderRef: orderRef}
	if line != "" {
		n, err := strconv.Atoi(line)
		if err != nil {
			return err
		}
		in.LineNo = &n
	}
	_, err := w.wave2.confirmPicks.Execute(ctx, in)
	return err
}

// ---------------------------------------------------------------- transfers --

// siteHoldsUnits seeds origin-site custody directly: stock only gets a site
// through a transfer stow, so this is fixture setup like "a Bin with capacity".
func (w *world) siteHoldsUnits(ctx context.Context, siteID string, qty int, sku, binID string) error {
	id, err := w.stock.NextID(ctx)
	if err != nil {
		return err
	}
	q, err := shared.NewQuantity(qty)
	if err != nil {
		return err
	}
	unit, err := stock.NewStockUnitAtSite(id, shared.SKU(sku), shared.BinId(binID), q, shared.SiteID(siteID))
	if err != nil {
		return err
	}
	return w.stock.Save(ctx, unit)
}

func (w *world) binBelongsToSite(ctx context.Context, binID string, capacity int, siteID string) error {
	c, err := shared.NewQuantity(capacity)
	if err != nil {
		return err
	}
	return w.locations.Save(ctx, location.RehydrateBinAtSite(shared.BinId(binID), c, 0, shared.SiteID(siteID), 1))
}

func (w *world) transferManagementAsksSiteToHold(ctx context.Context, siteID string, qty int, sku, lineID, transferID string) error {
	_, err := w.wave2.allocateTransfer.Execute(ctx, usecases.TransferCommand{
		TransferID: transferID, TransferLineID: lineID, OriginSiteID: shared.SiteID(siteID),
		SKU: shared.SKU(sku), Quantity: shared.Quantity(qty),
	})
	return err
}

func (w *world) stageReceipt(ctx context.Context, lineID, transferID, siteID string, qty int, sku string) error {
	return w.record(ctx, http.MethodPost, "/transfers/"+url.PathEscape(lineID)+"/receipt", map[string]any{
		"transferId": transferID, "destinationSiteId": siteID, "sku": sku, "receivedQuantity": qty,
	})
}

func (w *world) destinationHasCounted(ctx context.Context, siteID string, qty int, sku, lineID, transferID string) error {
	if err := w.stageReceipt(ctx, lineID, transferID, siteID, qty, sku); err != nil {
		return err
	}
	if w.status != http.StatusCreated {
		return fmt.Errorf("staging line %q: expected 201, got %d: %s", lineID, w.status, string(w.body))
	}
	return nil
}

func (w *world) iStowTransferLine(ctx context.Context, lineID string, table *godog.Table) error {
	bins := make([]map[string]any, 0, len(table.Rows))
	for i, row := range table.Rows {
		if i == 0 {
			continue // header
		}
		qty, err := strconv.Atoi(row.Cells[1].Value)
		if err != nil {
			return err
		}
		bins = append(bins, map[string]any{"binId": row.Cells[0].Value, "quantity": qty})
	}
	return w.record(ctx, http.MethodPost, "/transfers/"+url.PathEscape(lineID)+"/stowages", map[string]any{"bins": bins})
}

// transferReceiptView is the TransferReceipt / StowedTransfer wire shape.
type transferReceiptView struct {
	State            string           `json:"state"`
	ExpectedQuantity int              `json:"expectedQuantity"`
	ReceivedQuantity int              `json:"receivedQuantity"`
	Variance         int              `json:"variance"`
	Replay           bool             `json:"replay"`
	StowedQuantity   int              `json:"stowedQuantity"`
	Allocations      []allocationBody `json:"allocations"`
}

func (w *world) theTransferReceiptIs(state string, expected, received, variance int) error {
	var r transferReceiptView
	if err := w.decode(&r); err != nil {
		return err
	}
	if r.State != state || r.ExpectedQuantity != expected || r.ReceivedQuantity != received || r.Variance != variance {
		return fmt.Errorf("expected %s receipt expected=%d received=%d variance=%d, got %+v", state, expected, received, variance, r)
	}
	return nil
}

func (w *world) theTransferReceiptResponseIsReplay(replay bool) error {
	var r transferReceiptView
	if err := w.decode(&r); err != nil {
		return err
	}
	if r.Replay != replay {
		return fmt.Errorf("expected replay=%t, got %t", replay, r.Replay)
	}
	return nil
}

func (w *world) theStowPlacesUnitsAcrossAllocations(units, allocations int) error {
	var r transferReceiptView
	if err := w.decode(&r); err != nil {
		return err
	}
	if r.StowedQuantity != units || len(r.Allocations) != allocations {
		return fmt.Errorf("expected %d units across %d allocations, got %d across %d", units, allocations, r.StowedQuantity, len(r.Allocations))
	}
	return nil
}

func (w *world) theQuarantineExceptionIsOfKind(kind string) error {
	var p struct {
		Exception struct {
			Kind string `json:"kind"`
		} `json:"exception"`
	}
	if err := w.decode(&p); err != nil {
		return err
	}
	if p.Exception.Kind != kind {
		return fmt.Errorf("expected quarantine exception kind %q, got %q (%s)", kind, p.Exception.Kind, string(w.body))
	}
	return nil
}

func (w *world) theTransferAllocationWasRejectedWithReason(reason string) error {
	for _, e := range w.publisher.Events() {
		if rejected, ok := e.(shared.TransferStockAllocationRejected); ok {
			if rejected.Reason != reason {
				return fmt.Errorf("expected rejection reason %q, got %q", reason, rejected.Reason)
			}
			return nil
		}
	}
	return errors.New("no TransferStockAllocationRejected event was published")
}

// -------------------------------------------------------------- registration --

func (w *world) registerWave2Steps(sc *godog.ScenarioContext) {
	w.registerPlacementSteps(sc)
	w.registerReservationSteps(sc)
	w.registerConsumerSteps(sc)
	w.registerTransferSteps(sc)
	w.registerMiscSteps(sc)
}

func (w *world) registerPlacementSteps(sc *godog.ScenarioContext) {
	sc.Step(`^SKU "([^"]*)" is classified by product-master as Hazmat with DOT hazard class (\d+)$`, w.skuIsClassifiedHazmat)
	sc.Step(`^SKU "([^"]*)" is classified by product-master as TemperatureSensitive requiring "([^"]*)" storage$`, w.skuIsClassifiedTemperatureSensitive)
	sc.Step(`^Bin "([^"]*)" sits in a hazmat-rated zone$`, w.binSitsInHazmatZone)
	sc.Step(`^Bin "([^"]*)" sits in a non-hazmat "([^"]*)" zone$`, w.binSitsInNonHazmatZone)
	sc.Step(`^the facility-layout classification lookup is unavailable$`, w.lookupIsUnavailable)
}

func (w *world) registerReservationSteps(sc *godog.ScenarioContext) {
	sc.Step(`^I Reserve (\d+) units? of SKU "([^"]*)" for demand "([^"]*)" on line (-?\d+)$`, w.iReserveOnLine)
	sc.Step(`^a Reservation of (\d+) units? of SKU "([^"]*)" for demand "([^"]*)" on line (\d+)$`, w.aReservationOnLine)
	sc.Step(`^I Revoke the Reservation with id "([^"]*)"$`, w.iRevokeReservationWithID)
	sc.Step(`^I Confirm the pick for the Reservation with id "([^"]*)"$`, w.iConfirmPickForReservationWithID)
	sc.Step(`^the Reservation response has (\d+) allocations$`, w.theReservationResponseHasAllocations)
	sc.Step(`^the Reservation response reports line (\d+)$`, w.theReservationResponseReportsLine)
	sc.Step(`^the Reservation for demand "([^"]*)" on line (\d+) is (\w+)$`, w.theReservationOnLineHasStatus)
	sc.Step(`^every Reservation for demand "([^"]*)" is (\w+)$`, w.everyReservationForDemandHasStatus)
}

func (w *world) registerConsumerSteps(sc *godog.ScenarioContext) {
	sc.Step(`^inbound-receiving reports line "([^"]*)" of (\d+) units? of SKU "([^"]*)" in (Good|Damaged) condition$`, w.inboundReportsLine)
	sc.Step(`^fulfillment-execution reports (\w+) task "([^"]*)" completed for order "([^"]*)"(?: line (\d+))?$`, w.fulfillmentReportsTask)
}

func (w *world) registerTransferSteps(sc *godog.ScenarioContext) {
	sc.Step(`^site "([^"]*)" holds (\d+) units? of SKU "([^"]*)" in Bin "([^"]*)"$`, w.siteHoldsUnits)
	sc.Step(`^Bin "([^"]*)" with capacity (\d+) belongs to site "([^"]*)"$`, w.binBelongsToSite)
	sc.Step(`^transfer-management asks site "([^"]*)" to hold (\d+) units? of SKU "([^"]*)" for line "([^"]*)" of transfer "([^"]*)"$`, w.transferManagementAsksSiteToHold)
	sc.Step(`^the destination site "([^"]*)" has counted (\d+) units? of SKU "([^"]*)" for line "([^"]*)" of transfer "([^"]*)"$`, w.destinationHasCounted)
	sc.Step(`^I stage a receipt for line "([^"]*)" of transfer "([^"]*)" at site "([^"]*)" counting (\d+) units? of SKU "([^"]*)"$`, w.stageReceipt)
	sc.Step(`^I stow transfer line "([^"]*)" into bins:$`, w.iStowTransferLine)
	sc.Step(`^the transfer receipt is (STAGED|STOWED) with expected (\d+), received (\d+) and variance (-?\d+)$`, w.theTransferReceiptIs)
	sc.Step(`^the transfer receipt response is a replay$`, func() error { return w.theTransferReceiptResponseIsReplay(true) })
	sc.Step(`^the transfer receipt response is not a replay$`, func() error { return w.theTransferReceiptResponseIsReplay(false) })
	sc.Step(`^the stow places (\d+) units across (\d+) allocations$`, w.theStowPlacesUnitsAcrossAllocations)
	sc.Step(`^the quarantine exception is of kind "([^"]*)"$`, w.theQuarantineExceptionIsOfKind)
	sc.Step(`^the transfer allocation was rejected with reason "([^"]*)"$`, w.theTransferAllocationWasRejectedWithReason)
}

func (w *world) registerMiscSteps(sc *godog.ScenarioContext) {
	sc.Step(`^I run a Cycle count on Bin "([^"]*)" with a negative counted quantity of (\d+)$`, w.iRunACycleCountWithNegativeQuantity)
	sc.Step(`^I run a Cycle count on Bin "([^"]*)" without a counted quantity$`, w.iRunACycleCountWithoutQuantity)
	sc.Step(`^I register Bin "([^"]*)" without a capacity$`, w.iRegisterBinWithoutCapacity)
	sc.Step(`^the domain event "([^"]*)" was published exactly (\d+) times?$`, w.theDomainEventWasPublishedTimes)
}
