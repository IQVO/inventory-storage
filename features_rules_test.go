package main_test

// Documentation-derived rule steps (wave 3). They reuse the world/server
// plumbing in features_test.go and add what the earlier waves left unwired:
// the relocation REST surface (POST/GET /relocations), the RELOCATE and
// REPLENISH TaskCompleted consumers, the forward pick-face map behind
// reservation allocation order, and raw-input variants of the Kafka-fed use
// cases (product-master, inbound-receiving, fulfillment-execution) that
// record the refusal instead of failing the step. Everything is asserted
// through REST, published domain events, or the in-memory repositories the
// use cases write.

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/cucumber/godog"

	inboundhttp "github.com/claudioed/inventory-storage/internal/adapters/inbound/http"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/memory"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/allocation"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// rulesState is the per-scenario state added by the wave-3 steps.
type rulesState struct {
	relocationID  string
	lastGet       string
	relocateTask  *usecases.ApplyRelocationTask
	replenishTask *usecases.ApplyReplenishTask
	forward       *memory.ForwardFaceRepo

	taskOutcomes []string
	taskErr      error
	pickErr      error
	inboundErr   error
	classOutcome usecases.ApplyOutcome
	classErr     error
}

// wireRules adds the relocation use cases (with the bin-level emitter, so a
// move raises its two BinStockLevelChanged) and the forward pick-face map the
// wave-1 composition root leaves out.
func (w *world) wireRules(s *inboundhttp.Server) {
	relocations := memory.NewRelocationRepo()
	levels := &usecases.BinLevelEmitter{
		Ledger: memory.NewBinLevelLedger(w.stock), Locations: w.locations, Events: w.publisher, Clock: w.clock,
	}
	relocate := &usecases.RelocateStock{
		Stock: w.stock, Locations: w.locations, Relocations: relocations,
		Events: w.publisher, Clock: w.clock, Levels: levels,
	}
	s.RelocateStock = relocate
	s.GetRelocation = &usecases.GetRelocation{Relocations: relocations}

	forward := memory.NewForwardFaceRepo()
	s.ReserveStock.Forward = forward

	processed := memory.NewProcessedEventRepo()
	w.rules = rulesState{
		forward:       forward,
		relocateTask:  &usecases.ApplyRelocationTask{Relocate: relocate, ProcessedEvents: processed},
		replenishTask: &usecases.ApplyReplenishTask{Relocate: relocate, ProcessedEvents: processed},
	}
}

// ------------------------------------------------------------ generic REST --

func (w *world) aGetRequestIsSentTo(ctx context.Context, path string) error {
	w.rules.lastGet = path
	return w.record(ctx, http.MethodGet, path, nil)
}

func (w *world) noDomainEventsWerePublished() error {
	if n := len(w.publisher.Events()); n != 0 {
		return fmt.Errorf("expected no domain events, got %d (first: %s)", n, w.publisher.Events()[0].EventName())
	}
	return nil
}

// ---------------------------------------------------------------- paging ----

func (w *world) theResponseListsReservations(count int, cursor string) error {
	var page struct {
		Items      []map[string]any `json:"items"`
		NextCursor string           `json:"nextCursor"`
	}
	if err := w.decode(&page); err != nil {
		return err
	}
	if len(page.Items) != count {
		return fmt.Errorf("expected %d reservations on the page, got %d: %s", count, len(page.Items), string(w.body))
	}
	if cursor == "a" && page.NextCursor == "" {
		return fmt.Errorf("expected a next cursor, got none: %s", string(w.body))
	}
	if cursor == "no" && page.NextCursor != "" {
		return fmt.Errorf("expected no next cursor, got %q", page.NextCursor)
	}
	return nil
}

func (w *world) iFollowTheNextCursor(ctx context.Context) error {
	var page struct {
		NextCursor string `json:"nextCursor"`
	}
	if err := w.decode(&page); err != nil {
		return err
	}
	if page.NextCursor == "" {
		return errors.New("the last response carried no next cursor to follow")
	}
	return w.aGetRequestIsSentTo(ctx, w.rules.lastGet+"&cursor="+url.QueryEscape(page.NextCursor))
}

// ---------------------------------------------------- reservation details ----

func (w *world) theReservationAllocationsAre(spec string) error {
	var res struct {
		Allocations []allocationBody `json:"allocations"`
	}
	if err := w.decode(&res); err != nil {
		return err
	}
	got := map[string]int{}
	for _, a := range res.Allocations {
		got[a.BinID] += a.Quantity
	}
	want, err := parseBinQuantities(spec)
	if err != nil {
		return err
	}
	if !sameQuantities(got, want) {
		return fmt.Errorf("expected reservation allocations %v, got %v", want, got)
	}
	return nil
}

// binIsThePickFaceOfSKU records a slotting-optimization forward pick face
// (ADR 0050) the way ApplySlotPlan stores it.
func (w *world) binIsThePickFaceOfSKU(ctx context.Context, binID, sku string) error {
	_, err := w.rules.forward.ReplaceSiteMap(ctx, allocation.SiteMap{
		SiteID: "S1", PlanID: "plan-1", ApprovedAt: time.Date(2025, 12, 1, 0, 0, 0, 0, time.UTC),
		Assignments: []allocation.Assignment{{SKU: shared.SKU(sku), Bin: shared.BinId(binID)}},
	})
	return err
}

func (w *world) stockUnitIs(ctx context.Context, id, state string, qty int) error {
	unit, err := w.stock.FindByID(ctx, id)
	if err != nil {
		return err
	}
	if unit == nil {
		return fmt.Errorf("StockUnit %q does not exist", id)
	}
	if string(unit.State()) != state || unit.Quantity().Int() != qty {
		return fmt.Errorf("expected StockUnit %s to be %s with quantity %d, got %s with %d", id, state, qty, unit.State(), unit.Quantity().Int())
	}
	return nil
}

// ------------------------------------------------------------- relocations --

func (w *world) postRelocation(ctx context.Context, body map[string]any) error {
	if err := w.record(ctx, http.MethodPost, "/relocations", body); err != nil {
		return err
	}
	if w.status == http.StatusCreated {
		var rel struct {
			ID string `json:"id"`
		}
		if err := w.decode(&rel); err != nil {
			return err
		}
		w.rules.relocationID = rel.ID
	}
	return nil
}

func relocationBody(qty int, sku, from, to, site string) map[string]any {
	return map[string]any{"siteCode": site, "sku": sku, "fromBin": from, "toBin": to, "quantity": qty}
}

func (w *world) iRelocate(ctx context.Context, qty int, sku, from, to, site string) error {
	return w.postRelocation(ctx, relocationBody(qty, sku, from, to, site))
}

func (w *world) iRelocateForReason(ctx context.Context, qty int, sku, from, to, site, reason string) error {
	body := relocationBody(qty, sku, from, to, site)
	body["reason"] = reason
	return w.postRelocation(ctx, body)
}

func (w *world) iRelocateWithSourceRef(ctx context.Context, qty int, sku, from, to, site, ref string) error {
	body := relocationBody(qty, sku, from, to, site)
	body["sourceRef"] = ref
	return w.postRelocation(ctx, body)
}

func (w *world) iRelocateWithSourceRefOfLength(ctx context.Context, qty int, sku, from, to, site string, length int) error {
	body := relocationBody(qty, sku, from, to, site)
	body["sourceRef"] = strings.Repeat("r", length)
	return w.postRelocation(ctx, body)
}

func (w *world) iLookUpTheRelocation(ctx context.Context) error {
	if w.rules.relocationID == "" {
		return errors.New("no Relocation has been created in this scenario")
	}
	return w.record(ctx, http.MethodGet, "/relocations/"+w.rules.relocationID, nil)
}

func (w *world) iLookUpRelocation(ctx context.Context, id string) error {
	return w.record(ctx, http.MethodGet, "/relocations/"+url.PathEscape(id), nil)
}

type relocationView struct {
	ID                     string `json:"id"`
	SiteCode               string `json:"siteCode"`
	SKU                    string `json:"sku"`
	FromBin                string `json:"fromBin"`
	ToBin                  string `json:"toBin"`
	Quantity               int    `json:"quantity"`
	Reason                 string `json:"reason"`
	SourceRef              string `json:"sourceRef"`
	DestinationStockUnitID string `json:"destinationStockUnitId"`
	Legs                   []struct {
		StockUnitID string `json:"stockUnitId"`
		Quantity    int    `json:"quantity"`
	} `json:"legs"`
}

func (w *world) theRelocationResponseReports(qty int, sku, from, to, site, reason string) error {
	var rel relocationView
	if err := w.decode(&rel); err != nil {
		return err
	}
	if rel.ID == "" || rel.Quantity != qty || rel.SKU != sku || rel.FromBin != from || rel.ToBin != to || rel.SiteCode != site || rel.Reason != reason {
		return fmt.Errorf("expected relocation of %d %s from %s to %s at %s for %s, got %+v", qty, sku, from, to, site, reason, rel)
	}
	if rel.DestinationStockUnitID == "" {
		return errors.New("expected the relocation to name its destination StockUnit")
	}
	return nil
}

func (w *world) theRelocationResponseReportsSourceRef(ref string) error {
	var rel relocationView
	if err := w.decode(&rel); err != nil {
		return err
	}
	if rel.SourceRef != ref {
		return fmt.Errorf("expected source reference %q, got %q", ref, rel.SourceRef)
	}
	return nil
}

func (w *world) theRelocationDrewLegs(spec string) error {
	var rel relocationView
	if err := w.decode(&rel); err != nil {
		return err
	}
	var got []string
	for _, leg := range rel.Legs {
		got = append(got, strconv.Itoa(leg.Quantity))
	}
	if strings.Join(got, ",") != spec {
		return fmt.Errorf("expected source legs %s in draw order, got %s", spec, strings.Join(got, ","))
	}
	return nil
}

func (w *world) theRelocationLocationHeader() error {
	loc := w.headers.Get("Location")
	if !strings.HasPrefix(loc, "/relocations/") || loc == "/relocations/" {
		return fmt.Errorf("expected a Location header of the form /relocations/{id}, got %q", loc)
	}
	return nil
}

func (w *world) theStockRelocatedEventReports(qty int, sku, from, to, reason, ref string) error {
	evs := w.eventsNamed("StockRelocated")
	if len(evs) != 1 {
		return fmt.Errorf("expected one StockRelocated event, got %d", len(evs))
	}
	ev, ok := evs[0].(shared.StockRelocated)
	if !ok {
		return fmt.Errorf("unexpected event type %T", evs[0])
	}
	if ev.Quantity.Int() != qty || ev.SKU.String() != sku || ev.FromBin.String() != from || ev.ToBin.String() != to || ev.Reason != reason || ev.SourceRef != ref {
		return fmt.Errorf("expected StockRelocated %d %s %s->%s reason %s ref %q, got %+v", qty, sku, from, to, reason, ref, ev)
	}
	return nil
}

func (w *world) theBinStockLevelsPublishedAre(table *godog.Table) error {
	var got []shared.BinStockLevelChanged
	for _, e := range w.eventsNamed("BinStockLevelChanged") {
		ev, ok := e.(shared.BinStockLevelChanged)
		if !ok {
			return fmt.Errorf("unexpected event type %T", e)
		}
		got = append(got, ev)
	}
	rows := table.Rows[1:] // header
	if len(got) != len(rows) {
		return fmt.Errorf("expected %d BinStockLevelChanged events, got %d: %+v", len(rows), len(got), got)
	}
	for i, row := range rows {
		onHand, _ := strconv.Atoi(row.Cells[1].Value)
		reserved, _ := strconv.Atoi(row.Cells[2].Value)
		ev := got[i]
		if ev.BinID.String() != row.Cells[0].Value || ev.OnHand.Int() != onHand || ev.Reserved.Int() != reserved || string(ev.Reason) != row.Cells[3].Value {
			return fmt.Errorf("event %d: expected bin %s onHand %d reserved %d reason %s, got bin %s onHand %d reserved %d reason %s",
				i+1, row.Cells[0].Value, onHand, reserved, row.Cells[3].Value, ev.BinID, ev.OnHand.Int(), ev.Reserved.Int(), ev.Reason)
		}
	}
	return nil
}

// ------------------------------------------------- RELOCATE / REPLENISH ----

// fulfillmentReportsMove hands one TaskCompleted of the given type to BOTH
// move consumers, as the shared fulfillment consumer would dispatch by type:
// the one whose type matches applies it, the other must ignore it. The
// CloudEvents id is the first argument verbatim ("" = missing). An empty
// quantity means the field was absent; an empty ref means the additive
// relocation_ref / request_ref was absent.
func (w *world) fulfillmentReportsMove(ctx context.Context, taskType, eventID, sku, from, to, quantity, ref string) error {
	return w.reportMove(ctx, taskType, eventID, "task-"+eventID, sku, from, to, quantity, ref)
}

func (w *world) reportMove(ctx context.Context, taskType, eventID, taskID, sku, from, to, quantity, ref string) error {
	var qty *int
	if quantity != "" {
		n, err := strconv.Atoi(quantity)
		if err != nil {
			return err
		}
		qty = &n
	}
	w.rules.taskOutcomes, w.rules.taskErr = nil, nil

	rel, relErr := w.rules.relocateTask.Execute(ctx, usecases.RelocationTaskCompletion{
		EventID: eventID, TaskID: taskID, TaskType: taskType, SKU: sku, FromBin: from, ToBin: to, Quantity: qty, RelocationRef: ref,
	})
	rep, repErr := w.rules.replenishTask.Execute(ctx, usecases.ReplenishTaskCompletion{
		EventID: eventID, TaskID: taskID, TaskType: taskType, SKU: sku, FromBin: from, ToBin: to, Quantity: qty, RequestRef: ref,
	})
	w.rules.taskErr = errors.Join(relErr, repErr)
	w.rules.taskOutcomes = []string{string(rel.Outcome), string(rep.Outcome)}
	return nil
}

func (w *world) fulfillmentReportsMoveOf(ctx context.Context, taskType, taskID string, qty int, sku, from, to string) error {
	return w.reportMove(ctx, taskType, "evt-"+taskID, taskID, sku, from, to, strconv.Itoa(qty), "")
}

func (w *world) fulfillmentReportsMoveWithRef(ctx context.Context, taskType, taskID string, qty int, sku, from, to, ref string) error {
	return w.reportMove(ctx, taskType, "evt-"+taskID, taskID, sku, from, to, strconv.Itoa(qty), ref)
}

func (w *world) theMoveCompletionIs(outcome string) error {
	if w.rules.taskErr != nil {
		return fmt.Errorf("expected the completion to be processed, got %w", w.rules.taskErr)
	}
	for _, got := range w.rules.taskOutcomes {
		if got == outcome {
			return nil
		}
	}
	return fmt.Errorf("expected one consumer to report %s, got %v", outcome, w.rules.taskOutcomes)
}

func (w *world) theMoveCompletionIsIgnored() error {
	if w.rules.taskErr != nil {
		return fmt.Errorf("expected the completion to be ignored, got %w", w.rules.taskErr)
	}
	for _, got := range w.rules.taskOutcomes {
		if got != string(usecases.RelocationIgnored) {
			return fmt.Errorf("expected both consumers to ignore the completion, got %v", w.rules.taskOutcomes)
		}
	}
	return nil
}

func (w *world) theMoveCompletionIsRefusedAsMalformed() error {
	if !errors.Is(w.rules.taskErr, usecases.ErrMalformedRelocationTask) {
		return fmt.Errorf("expected a malformed relocation task refusal, got %v", w.rules.taskErr)
	}
	return nil
}

// --------------------------------------------------- fulfillment: PICK -----

// fulfillmentReportsPick hands a PICK TaskCompleted with an explicit event id
// ("" = missing) and line ("" = the event carries none) to the consumer's use
// case and records the refusal instead of failing the step.
func (w *world) fulfillmentReportsPick(ctx context.Context, eventID, orderRef, line string) error {
	in := usecases.PickCompletion{EventID: eventID, TaskID: "task-" + eventID, TaskType: "PICK", OrderRef: orderRef}
	if line != "" {
		n, err := strconv.Atoi(line)
		if err != nil {
			return err
		}
		in.LineNo = &n
	}
	_, w.rules.pickErr = w.wave2.confirmPicks.Execute(ctx, in)
	return nil
}

func (w *world) thePickCompletionIsRefusedAsMalformed() error {
	if !errors.Is(w.rules.pickErr, usecases.ErrMalformedPickCompletion) {
		return fmt.Errorf("expected a malformed pick completion refusal, got %v", w.rules.pickErr)
	}
	return nil
}

func (w *world) thePickCompletionIsProcessed() error {
	if w.rules.pickErr != nil {
		return fmt.Errorf("expected the pick completion to be processed, got %w", w.rules.pickErr)
	}
	return nil
}

// ------------------------------------------------------ inbound-receiving ---

func (w *world) inboundReportsRawLine(ctx context.Context, eventID, sku string, qty int, condition string) error {
	_, w.rules.inboundErr = w.wave2.bookInbound.Execute(ctx, usecases.InboundReceiptLine{
		EventID: eventID, ReceiptID: "rcpt-9", ASNNumber: "ASN-9", LineNo: 1, SKU: sku, Quantity: qty, Condition: condition,
	})
	return nil
}

func (w *world) theInboundLineIsRefusedAsMalformed() error {
	if !errors.Is(w.rules.inboundErr, usecases.ErrMalformedInboundReceiptLine) {
		return fmt.Errorf("expected a malformed inbound receipt line refusal, got %v", w.rules.inboundErr)
	}
	return nil
}

// ---------------------------------------------------- product-master (#8) ---

func splitTags(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	return strings.Split(raw, ", ")
}

func (w *world) productMasterSendsClassification(ctx context.Context, eventID, sku, tags, temperature string, dot, version int) error {
	w.rules.classOutcome, w.rules.classErr = w.applyClassification.Execute(ctx, usecases.ProductClassificationUpdate{
		EventID: eventID, SKU: sku, HandlingTags: splitTags(tags), TemperatureClass: temperature,
		DOTHazardClass: dot, ClassificationSource: "native", Version: int64(version),
	})
	return nil
}

func (w *world) theClassificationUpdateIs(outcome string) error {
	if w.rules.classErr != nil {
		return fmt.Errorf("expected the update to be processed, got %w", w.rules.classErr)
	}
	if string(w.rules.classOutcome) != outcome {
		return fmt.Errorf("expected the update to be %s, got %s", outcome, w.rules.classOutcome)
	}
	return nil
}

func (w *world) theClassificationUpdateIsRefusedAsMalformed() error {
	if !errors.Is(w.rules.classErr, usecases.ErrMalformedProductClassification) {
		return fmt.Errorf("expected a malformed classification refusal, got %v", w.rules.classErr)
	}
	return nil
}

func (w *world) theLocalClassificationOfSKUIs(ctx context.Context, sku, tags string, version int) error {
	c, err := w.classifications.FindBySKU(ctx, shared.SKU(sku))
	if err != nil {
		return err
	}
	if c == nil {
		return fmt.Errorf("SKU %q has no local classification", sku)
	}
	var got []string
	for _, t := range c.HandlingTags() {
		got = append(got, string(t))
	}
	stored, _, _ := w.classifications.VersionOf(shared.SKU(sku))
	if strings.Join(got, ", ") != tags || stored != int64(version) {
		return fmt.Errorf("expected local classification %q at version %d, got %q at version %d", tags, version, strings.Join(got, ", "), stored)
	}
	return nil
}

func (w *world) skuHasNoLocalClassification(ctx context.Context, sku string) error {
	c, err := w.classifications.FindBySKU(ctx, shared.SKU(sku))
	if err != nil {
		return err
	}
	if c != nil {
		return fmt.Errorf("expected SKU %q to have no local classification", sku)
	}
	return nil
}

// ----------------------------------------------------------------- wiring ---

func (w *world) registerRuleSteps(sc *godog.ScenarioContext) {
	sc.Step(`^a GET request is sent to "([^"]*)"$`, w.aGetRequestIsSentTo)
	sc.Step(`^no domain events were published$`, w.noDomainEventsWerePublished)
	sc.Step(`^the response lists (\d+) reservations? and (a|no) next cursor$`, w.theResponseListsReservations)
	sc.Step(`^I follow the next cursor$`, w.iFollowTheNextCursor)
	sc.Step(`^the Reservation allocations are "([^"]*)"$`, w.theReservationAllocationsAre)
	sc.Step(`^Bin "([^"]*)" is the pick face of SKU "([^"]*)"$`, w.binIsThePickFaceOfSKU)
	sc.Step(`^StockUnit "([^"]*)" is (\w+) with quantity (\d+)$`, w.stockUnitIs)

	sc.Step(`^I Relocate (-?\d+) units? of SKU "([^"]*)" from Bin "([^"]*)" to Bin "([^"]*)" at site "([^"]*)"$`, w.iRelocate)
	sc.Step(`^I Relocate (-?\d+) units? of SKU "([^"]*)" from Bin "([^"]*)" to Bin "([^"]*)" at site "([^"]*)" for reason "([^"]*)"$`, w.iRelocateForReason)
	sc.Step(`^I Relocate (-?\d+) units? of SKU "([^"]*)" from Bin "([^"]*)" to Bin "([^"]*)" at site "([^"]*)" with source reference "([^"]*)"$`, w.iRelocateWithSourceRef)
	sc.Step(`^I Relocate (-?\d+) units? of SKU "([^"]*)" from Bin "([^"]*)" to Bin "([^"]*)" at site "([^"]*)" with a source reference of (\d+) characters$`, w.iRelocateWithSourceRefOfLength)
	sc.Step(`^I look up the Relocation$`, w.iLookUpTheRelocation)
	sc.Step(`^I look up Relocation "([^"]*)"$`, w.iLookUpRelocation)
	sc.Step(`^the Relocation response reports (\d+) units? of SKU "([^"]*)" moved from Bin "([^"]*)" to Bin "([^"]*)" at site "([^"]*)" for reason "([^"]*)"$`, w.theRelocationResponseReports)
	sc.Step(`^the Relocation response reports source reference "([^"]*)"$`, w.theRelocationResponseReportsSourceRef)
	sc.Step(`^the Relocation drew source legs "([^"]*)" in order$`, w.theRelocationDrewLegs)
	sc.Step(`^the response has a Location header pointing at the Relocation$`, w.theRelocationLocationHeader)
	sc.Step(`^the StockRelocated event reports (\d+) units? of SKU "([^"]*)" from Bin "([^"]*)" to Bin "([^"]*)" for reason "([^"]*)" with source reference "([^"]*)"$`, w.theStockRelocatedEventReports)
	sc.Step(`^the BinStockLevelChanged events published are:$`, w.theBinStockLevelsPublishedAre)

	sc.Step(`^fulfillment-execution reports (RELOCATE|REPLENISH|PACK) task "([^"]*)" completed moving (\d+) units? of SKU "([^"]*)" from Bin "([^"]*)" to Bin "([^"]*)"$`, w.fulfillmentReportsMoveOf)
	sc.Step(`^fulfillment-execution reports (RELOCATE|REPLENISH|PACK) task "([^"]*)" completed moving (\d+) units? of SKU "([^"]*)" from Bin "([^"]*)" to Bin "([^"]*)" with reference "([^"]*)"$`, w.fulfillmentReportsMoveWithRef)
	sc.Step(`^fulfillment-execution reports (RELOCATE|REPLENISH|PACK) completion with event id "([^"]*)", SKU "([^"]*)", from "([^"]*)", to "([^"]*)", quantity "([^"]*)" and reference "([^"]*)"$`, w.fulfillmentReportsMove)
	sc.Step(`^the move completion is (APPLIED|DUPLICATE)$`, w.theMoveCompletionIs)
	sc.Step(`^the move completion is ignored$`, w.theMoveCompletionIsIgnored)
	sc.Step(`^the move completion is refused as malformed$`, w.theMoveCompletionIsRefusedAsMalformed)

	sc.Step(`^fulfillment-execution reports PICK completion with event id "([^"]*)" for order "([^"]*)" and line "([^"]*)"$`, w.fulfillmentReportsPick)
	sc.Step(`^the pick completion is refused as malformed$`, w.thePickCompletionIsRefusedAsMalformed)
	sc.Step(`^the pick completion is processed$`, w.thePickCompletionIsProcessed)

	sc.Step(`^inbound-receiving reports a line with event id "([^"]*)", SKU "([^"]*)", quantity (-?\d+) and condition "([^"]*)"$`, w.inboundReportsRawLine)
	sc.Step(`^the inbound line is refused as malformed$`, w.theInboundLineIsRefusedAsMalformed)

	sc.Step(`^product-master sends classification event "([^"]*)" for SKU "([^"]*)" with tags "([^"]*)", temperature class "([^"]*)", DOT class (\d+) and version (\d+)$`, w.productMasterSendsClassification)
	sc.Step(`^the classification update is (APPLIED|STALE|DUPLICATE)$`, w.theClassificationUpdateIs)
	sc.Step(`^the classification update is refused as malformed$`, w.theClassificationUpdateIsRefusedAsMalformed)
	sc.Step(`^the local classification of SKU "([^"]*)" is "([^"]*)" at version (\d+)$`, w.theLocalClassificationOfSKUIs)
	sc.Step(`^SKU "([^"]*)" has no local classification$`, w.skuHasNoLocalClassification)
}
