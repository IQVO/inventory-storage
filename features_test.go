// Package main_test hosts the godog (Cucumber for Go) acceptance suite. It
// drives the REAL chi router over HTTP — same wiring the service uses in
// production, but with the in-memory outbound adapters — so every scenario in
// features/*.feature is a true black-box test of the REST API.
package main_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/cucumber/godog"

	inboundhttp "github.com/claudioed/inventory-storage/internal/adapters/inbound/http"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/events"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/memory"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/location"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// TestFeatures runs every Gherkin feature under features/ against a freshly
// wired HTTP server.
func TestFeatures(t *testing.T) {
	suite := godog.TestSuite{
		ScenarioInitializer: InitializeScenario,
		Options: &godog.Options{
			Format:   "pretty",
			Paths:    []string{"features"},
			Strict:   true,
			TestingT: t,
		},
	}
	if suite.Run() != 0 {
		t.Fatal("non-zero status returned, failed to run feature tests")
	}
}

// world is the per-scenario state: a running server over fresh in-memory
// repositories, plus whatever the last HTTP call returned.
type world struct {
	server          *httptest.Server
	stock           *memory.StockRepo
	locations       *memory.LocationRepo
	clock           *memory.FixedClock
	classifications *memory.ProductClassificationRepo
	publisher       *events.BufferedPublisher

	status  int
	body    []byte
	headers http.Header

	reservationID string
}

// start builds the composition root the way cmd/inventory does, but with the
// memory adapters and a fixed clock, and exposes it over a real TCP listener.
func (w *world) start() {
	stockRepo := memory.NewStockRepo()
	locationRepo := memory.NewLocationRepo()
	reservationRepo := memory.NewReservationRepo()
	classificationRepo := memory.NewProductClassificationRepo()
	publisher := events.NewBufferedPublisher()
	clock := memory.NewFixedClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))

	s := &inboundhttp.Server{
		ReceiveStock:               &usecases.ReceiveStock{Events: publisher, Clock: clock},
		StowStock:                  &usecases.StowStock{Stock: stockRepo, Locations: locationRepo, Events: publisher, Clock: clock},
		ReserveStock:               &usecases.ReserveStock{Stock: stockRepo, Reservations: reservationRepo, Events: publisher, Clock: clock},
		RevokeReservation:          &usecases.RevokeReservation{Stock: stockRepo, Reservations: reservationRepo, Events: publisher, Clock: clock},
		ConfirmPick:                &usecases.ConfirmPick{Stock: stockRepo, Locations: locationRepo, Reservations: reservationRepo, Events: publisher, Clock: clock},
		GetUsable:                  &usecases.GetUsable{Stock: stockRepo},
		GetReservationsByDemandRef: &usecases.GetReservationsByDemandRef{Stock: stockRepo, Reservations: reservationRepo, Events: publisher, Clock: clock},
		RunCycleCount:              &usecases.RunCycleCount{Stock: stockRepo, Events: publisher, Clock: clock},
		ClassifyProduct:            &usecases.ClassifyProduct{Classifications: classificationRepo, Events: publisher, Clock: clock},
		Classifications:            classificationRepo,
	}

	w.server = httptest.NewServer(inboundhttp.NewRouter(s, nil, ""))
	w.stock = stockRepo
	w.locations = locationRepo
	w.clock = clock
	w.classifications = classificationRepo
	w.publisher = publisher
	w.status = 0
	w.body = nil
	w.headers = nil
	w.reservationID = ""
}

func (w *world) stop() {
	if w.server != nil {
		w.server.Close()
		w.server = nil
	}
}

// call issues a real net/http request against the test server and returns the
// status, body and headers without touching the recorded "last response".
func (w *world) call(ctx context.Context, method, path string, payload any) (int, []byte, http.Header, error) {
	var reader io.Reader = http.NoBody
	if payload != nil {
		encoded, err := json.Marshal(payload)
		if err != nil {
			return 0, nil, nil, err
		}
		reader = bytes.NewReader(encoded)
	}

	req, err := http.NewRequestWithContext(ctx, method, w.server.URL+path, reader)
	if err != nil {
		return 0, nil, nil, err
	}
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := w.server.Client().Do(req)
	if err != nil {
		return 0, nil, nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, nil, nil, err
	}
	return resp.StatusCode, body, resp.Header, nil
}

// record issues a request and remembers it as the response the Then steps
// assert against.
func (w *world) record(ctx context.Context, method, path string, payload any) error {
	status, body, headers, err := w.call(ctx, method, path, payload)
	if err != nil {
		return err
	}
	w.status, w.body, w.headers = status, body, headers
	return nil
}

func (w *world) decode(dest any) error {
	if err := json.Unmarshal(w.body, dest); err != nil {
		return fmt.Errorf("response body is not valid JSON (%w): %s", err, string(w.body))
	}
	return nil
}

// ---------------------------------------------------------------- Given ----

func (w *world) anEmptyWarehouse() error {
	if published := len(w.publisher.Events()); published != 0 {
		return fmt.Errorf("expected a clean warehouse, but %d domain events were already published", published)
	}
	return nil
}

// aBinWithCapacity seeds a Bin directly through the LocationRepo: bins are
// warehouse topology, and the REST API deliberately exposes no endpoint that
// creates them.
func (w *world) aBinWithCapacity(ctx context.Context, id string, capacity int) error {
	binID, err := shared.NewBinId(id)
	if err != nil {
		return err
	}
	cap, err := shared.NewQuantity(capacity)
	if err != nil {
		return err
	}
	bin, err := location.NewBin(binID, cap)
	if err != nil {
		return err
	}
	return w.locations.Save(ctx, bin)
}

func (w *world) unitsHaveBeenReceived(ctx context.Context, qty int, sku string) error {
	status, body, _, err := w.call(ctx, http.MethodPost, "/stock/receive", map[string]any{"sku": sku, "quantity": qty})
	if err != nil {
		return err
	}
	if status != http.StatusAccepted {
		return fmt.Errorf("receiving %d of %q: expected 202, got %d: %s", qty, sku, status, string(body))
	}
	return nil
}

func (w *world) unitsAreStowedIntoBin(ctx context.Context, qty int, sku, binID string) error {
	status, body, _, err := w.call(ctx, http.MethodPost, "/stock/stow", map[string]any{"sku": sku, "quantity": qty, "binId": binID})
	if err != nil {
		return err
	}
	if status != http.StatusCreated {
		return fmt.Errorf("stowing %d of %q into %q: expected 201, got %d: %s", qty, sku, binID, status, string(body))
	}
	return nil
}

func (w *world) aReservationOfUnitsForDemand(ctx context.Context, qty int, sku, demandRef string) error {
	status, body, _, err := w.call(ctx, http.MethodPost, "/reservations", map[string]any{"sku": sku, "quantity": qty, "demandRef": demandRef})
	if err != nil {
		return err
	}
	if status != http.StatusCreated {
		return fmt.Errorf("reserving %d of %q: expected 201, got %d: %s", qty, sku, status, string(body))
	}
	var res struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(body, &res); err != nil {
		return err
	}
	w.reservationID = res.ID
	return nil
}

// timeAdvancesByMinutes moves the scenario's fixed clock forward — the
// documented test seam for crossing a Reservation's expiresAt (the default
// timeout is 30 minutes) without sleeping.
func (w *world) timeAdvancesByMinutes(minutes int) error {
	w.clock.Advance(time.Duration(minutes) * time.Minute)
	return nil
}

// ----------------------------------------------------------------- When ----

func (w *world) iStowUnitsIntoBin(ctx context.Context, qty int, sku, binID string) error {
	return w.record(ctx, http.MethodPost, "/stock/stow", map[string]any{"sku": sku, "quantity": qty, "binId": binID})
}

func (w *world) iReserveUnitsForDemand(ctx context.Context, qty int, sku, demandRef string) error {
	if err := w.record(ctx, http.MethodPost, "/reservations", map[string]any{"sku": sku, "quantity": qty, "demandRef": demandRef}); err != nil {
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

func (w *world) iRevokeTheReservation(ctx context.Context) error {
	if w.reservationID == "" {
		return fmt.Errorf("no Reservation has been created in this scenario")
	}
	return w.record(ctx, http.MethodDelete, "/reservations/"+w.reservationID, nil)
}

func (w *world) iConfirmThePickForTheReservation(ctx context.Context) error {
	if w.reservationID == "" {
		return fmt.Errorf("no Reservation has been created in this scenario")
	}
	return w.record(ctx, http.MethodPost, "/reservations/"+w.reservationID+"/confirm-pick", nil)
}

func (w *world) iRunACycleCountOnBin(ctx context.Context, binID string, counted int) error {
	return w.record(ctx, http.MethodPost, "/bins/"+binID+"/cycle-count", map[string]any{"countedQuantity": counted})
}

func (w *world) iRequestTheUsableInventoryFor(ctx context.Context, sku string) error {
	return w.record(ctx, http.MethodGet, "/inventory/"+sku+"/usable", nil)
}

func (w *world) iReceiveUnitsOfSKU(ctx context.Context, qty int, sku string) error {
	return w.record(ctx, http.MethodPost, "/stock/receive", map[string]any{"sku": sku, "quantity": qty})
}

func (w *world) iLookUpTheReservationsForDemand(ctx context.Context, demandRef string) error {
	return w.record(ctx, http.MethodGet, "/reservations?demandRef="+url.QueryEscape(demandRef), nil)
}

func (w *world) iLookUpTheReservationsWithoutADemandRef(ctx context.Context) error {
	return w.record(ctx, http.MethodGet, "/reservations", nil)
}

func (w *world) iClassifySKUWithHandlingTags(ctx context.Context, sku, tags string) error {
	return w.record(ctx, http.MethodPut, "/products/"+sku+"/classification", map[string]any{
		"handlingTags": strings.Split(tags, ", "),
	})
}

func (w *world) iClassifySKUAsTemperatureSensitiveWithoutATemperatureClass(ctx context.Context, sku string) error {
	return w.record(ctx, http.MethodPut, "/products/"+sku+"/classification", map[string]any{
		"handlingTags": []string{"TemperatureSensitive"},
	})
}

func (w *world) iRequestTheClassificationForSKU(ctx context.Context, sku string) error {
	return w.record(ctx, http.MethodGet, "/products/"+sku+"/classification", nil)
}

// ----------------------------------------------------------------- Then ----

func (w *world) theResponseStatusIs(expected int) error {
	if w.status != expected {
		return fmt.Errorf("expected status %d, got %d: %s", expected, w.status, string(w.body))
	}
	return nil
}

func (w *world) theStockUnitResponseReports(sku, binID string, qty int) error {
	var unit struct {
		SKU      string `json:"sku"`
		BinID    string `json:"binId"`
		Quantity int    `json:"quantity"`
	}
	if err := w.decode(&unit); err != nil {
		return err
	}
	if unit.SKU != sku || unit.BinID != binID || unit.Quantity != qty {
		return fmt.Errorf("expected StockUnit %s in bin %s with quantity %d, got %s in bin %s with quantity %d",
			sku, binID, qty, unit.SKU, unit.BinID, unit.Quantity)
	}
	return nil
}

func (w *world) theReservationResponseReports(qty int, demandRef string) error {
	var res struct {
		ID        string `json:"id"`
		Quantity  int    `json:"quantity"`
		DemandRef string `json:"demandRef"`
		Status    string `json:"status"`
	}
	if err := w.decode(&res); err != nil {
		return err
	}
	if res.Quantity != qty || res.DemandRef != demandRef {
		return fmt.Errorf("expected Reservation of %d for demand %q, got %d for %q", qty, demandRef, res.Quantity, res.DemandRef)
	}
	if res.ID == "" {
		return fmt.Errorf("expected the Reservation response to carry an id")
	}
	return nil
}

func (w *world) theUsableInventoryResponseReports(sku string, usable int) error {
	var body struct {
		SKU    string `json:"sku"`
		Usable int    `json:"usable"`
	}
	if err := w.decode(&body); err != nil {
		return err
	}
	if body.SKU != sku || body.Usable != usable {
		return fmt.Errorf("expected %d usable for SKU %q, got %d for %q", usable, sku, body.Usable, body.SKU)
	}
	return nil
}

func (w *world) theLocationHeaderPointsAt(prefix string) error {
	loc := w.headers.Get("Location")
	if !strings.HasPrefix(loc, prefix) || strings.TrimPrefix(loc, prefix) == "" {
		return fmt.Errorf("expected a Location header of the form %s{id}, got %q", prefix, loc)
	}
	return nil
}

func (w *world) theResponseHasALocationHeaderForTheStockUnit() error {
	return w.theLocationHeaderPointsAt("/stock/")
}

func (w *world) theResponseHasALocationHeaderForTheReservation() error {
	return w.theLocationHeaderPointsAt("/reservations/")
}

// theProblemDetailTypeIs asserts the RFC 7807 body: correct content type, and
// a "type" URI whose last segment identifies the error category.
func (w *world) theProblemDetailTypeIs(slug string) error {
	if ct := w.headers.Get("Content-Type"); ct != "application/problem+json" {
		return fmt.Errorf("expected Content-Type application/problem+json, got %q", ct)
	}
	var problem struct {
		Type   string `json:"type"`
		Title  string `json:"title"`
		Status int    `json:"status"`
	}
	if err := w.decode(&problem); err != nil {
		return err
	}
	if got := problem.Type[strings.LastIndex(problem.Type, "/")+1:]; got != slug {
		return fmt.Errorf("expected problem type %q, got %q (from %q)", slug, got, problem.Type)
	}
	if problem.Status != w.status {
		return fmt.Errorf("problem body status %d does not match HTTP status %d", problem.Status, w.status)
	}
	return nil
}

func (w *world) theCycleCountReportsQuantities(systemQty, countedQty int) error {
	var body struct {
		SystemQty  int `json:"systemQuantity"`
		CountedQty int `json:"countedQuantity"`
	}
	if err := w.decode(&body); err != nil {
		return err
	}
	if body.SystemQty != systemQty || body.CountedQty != countedQty {
		return fmt.Errorf("expected system quantity %d and counted quantity %d, got %d and %d",
			systemQty, countedQty, body.SystemQty, body.CountedQty)
	}
	return nil
}

func (w *world) theCycleCountDiscrepancyIs(expected bool) error {
	var body struct {
		Discrepancy bool `json:"discrepancy"`
	}
	if err := w.decode(&body); err != nil {
		return err
	}
	if body.Discrepancy != expected {
		return fmt.Errorf("expected discrepancy=%t, got %t", expected, body.Discrepancy)
	}
	return nil
}

func (w *world) theCycleCountReportsNoDiscrepancy() error { return w.theCycleCountDiscrepancyIs(false) }

func (w *world) theCycleCountReportsADiscrepancy() error { return w.theCycleCountDiscrepancyIs(true) }

func (w *world) theStagedReceiptResponseReports(sku string, qty int) error {
	var body struct {
		SKU      string `json:"sku"`
		Quantity int    `json:"quantity"`
	}
	if err := w.decode(&body); err != nil {
		return err
	}
	if body.SKU != sku || body.Quantity != qty {
		return fmt.Errorf("expected staged receipt of %d of SKU %q, got %d of %q", qty, sku, body.Quantity, body.SKU)
	}
	return nil
}

func (w *world) theReservationsListContainsEntries(expected int) error {
	list, err := w.decodeReservationsList()
	if err != nil {
		return err
	}
	if len(list) != expected {
		return fmt.Errorf("expected the Reservations list to contain %d entries, got %d", expected, len(list))
	}
	return nil
}

func (w *world) theReservationsListContainsAReservationWithStatus(status string) error {
	list, err := w.decodeReservationsList()
	if err != nil {
		return err
	}
	got := make([]string, 0, len(list))
	for _, res := range list {
		if res.Status == status {
			return nil
		}
		got = append(got, res.Status)
	}
	return fmt.Errorf("expected the Reservations list to contain a Reservation with status %q, got %v", status, got)
}

// decodeReservationsList decodes the recorded GET /reservations?demandRef=
// response: an array of Reservation DTOs.
func (w *world) decodeReservationsList() ([]struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}, error) {
	var list []struct {
		ID     string `json:"id"`
		Status string `json:"status"`
	}
	if err := w.decode(&list); err != nil {
		return nil, err
	}
	return list, nil
}

func (w *world) theProductClassificationResponseReports(sku, tags string) error {
	var body struct {
		SKU          string   `json:"sku"`
		HandlingTags []string `json:"handlingTags"`
	}
	if err := w.decode(&body); err != nil {
		return err
	}
	expected := strings.Split(tags, ", ")
	if body.SKU != sku || len(body.HandlingTags) != len(expected) {
		return fmt.Errorf("expected ProductClassification for SKU %q with handling tags %v, got %q with %v",
			sku, expected, body.SKU, body.HandlingTags)
	}
	for _, tag := range expected {
		found := false
		for _, got := range body.HandlingTags {
			if got == tag {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("expected ProductClassification handling tags to include %q, got %v", tag, body.HandlingTags)
		}
	}
	return nil
}

func (w *world) theDomainEventWasPublished(name string) error {
	published := make([]string, 0, len(w.publisher.Events()))
	for _, e := range w.publisher.Events() {
		if e.EventName() == name {
			return nil
		}
		published = append(published, e.EventName())
	}
	return fmt.Errorf("expected domain event %q to be published, got %v", name, published)
}

func (w *world) theDomainEventWasNotPublished(name string) error {
	for _, e := range w.publisher.Events() {
		if e.EventName() == name {
			return fmt.Errorf("expected domain event %q NOT to be published, but it was", name)
		}
	}
	return nil
}

// theUsableInventoryForSKUIs queries GET /inventory/{sku}/usable out of band,
// so using it as a Given (or as a sanity check between steps) never clobbers
// the response the surrounding Then steps assert against.
func (w *world) theUsableInventoryForSKUIs(ctx context.Context, sku string, expected int) error {
	status, body, _, err := w.call(ctx, http.MethodGet, "/inventory/"+sku+"/usable", nil)
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return fmt.Errorf("querying usable inventory for %q: expected 200, got %d: %s", sku, status, string(body))
	}
	var usable struct {
		Usable int `json:"usable"`
	}
	if err := json.Unmarshal(body, &usable); err != nil {
		return err
	}
	if usable.Usable != expected {
		return fmt.Errorf("expected Usable inventory of %d for SKU %q, got %d", expected, sku, usable.Usable)
	}
	return nil
}

// ------------------------------------------------------------- wiring ------

// InitializeScenario registers the step definitions and gives every scenario
// its own server and its own in-memory state.
func InitializeScenario(sc *godog.ScenarioContext) {
	w := &world{}

	sc.Before(func(ctx context.Context, _ *godog.Scenario) (context.Context, error) {
		w.start()
		return ctx, nil
	})
	sc.After(func(ctx context.Context, _ *godog.Scenario, _ error) (context.Context, error) {
		w.stop()
		return ctx, nil
	})

	sc.Step(`^an empty warehouse$`, w.anEmptyWarehouse)
	sc.Step(`^a Bin "([^"]*)" with capacity (\d+)$`, w.aBinWithCapacity)
	sc.Step(`^(\d+) units? of SKU "([^"]*)" have been Received$`, w.unitsHaveBeenReceived)
	sc.Step(`^(\d+) units? of SKU "([^"]*)" are Stowed into Bin "([^"]*)"$`, w.unitsAreStowedIntoBin)
	sc.Step(`^a Reservation of (\d+) units? of SKU "([^"]*)" for demand "([^"]*)"$`, w.aReservationOfUnitsForDemand)
	sc.Step(`^time advances by (\d+) minutes$`, w.timeAdvancesByMinutes)

	sc.Step(`^I Stow (\d+) units? of SKU "([^"]*)" into Bin "([^"]*)"$`, w.iStowUnitsIntoBin)
	sc.Step(`^I Reserve (\d+) units? of SKU "([^"]*)" for demand "([^"]*)"$`, w.iReserveUnitsForDemand)
	sc.Step(`^I Revoke the Reservation$`, w.iRevokeTheReservation)
	sc.Step(`^I Confirm the pick for the Reservation$`, w.iConfirmThePickForTheReservation)
	sc.Step(`^I run a Cycle count on Bin "([^"]*)" with counted quantity (\d+)$`, w.iRunACycleCountOnBin)
	sc.Step(`^I request the Usable inventory for SKU "([^"]*)"$`, w.iRequestTheUsableInventoryFor)
	sc.Step(`^I Receive (\d+) units? of SKU "([^"]*)"$`, w.iReceiveUnitsOfSKU)
	sc.Step(`^I look up the Reservations for demand "([^"]*)"$`, w.iLookUpTheReservationsForDemand)
	sc.Step(`^I look up the Reservations without a demandRef$`, w.iLookUpTheReservationsWithoutADemandRef)
	sc.Step(`^I Classify SKU "([^"]*)" with handling tags "([^"]*)"$`, w.iClassifySKUWithHandlingTags)
	sc.Step(`^I Classify SKU "([^"]*)" as TemperatureSensitive without a temperature class$`, w.iClassifySKUAsTemperatureSensitiveWithoutATemperatureClass)
	sc.Step(`^I request the classification for SKU "([^"]*)"$`, w.iRequestTheClassificationForSKU)

	sc.Step(`^the response status is (\d+)$`, w.theResponseStatusIs)
	sc.Step(`^the StockUnit response reports SKU "([^"]*)" in Bin "([^"]*)" with quantity (\d+)$`, w.theStockUnitResponseReports)
	sc.Step(`^the Reservation response reports quantity (\d+) for demand "([^"]*)"$`, w.theReservationResponseReports)
	sc.Step(`^the Usable inventory response reports SKU "([^"]*)" with (\d+) usable$`, w.theUsableInventoryResponseReports)
	sc.Step(`^the response has a Location header pointing at the created StockUnit$`, w.theResponseHasALocationHeaderForTheStockUnit)
	sc.Step(`^the response has a Location header pointing at the created Reservation$`, w.theResponseHasALocationHeaderForTheReservation)
	sc.Step(`^the problem detail type is "([^"]*)"$`, w.theProblemDetailTypeIs)
	sc.Step(`^the Cycle count reports system quantity (\d+) and counted quantity (\d+)$`, w.theCycleCountReportsQuantities)
	sc.Step(`^the Cycle count reports no discrepancy$`, w.theCycleCountReportsNoDiscrepancy)
	sc.Step(`^the Cycle count reports a discrepancy$`, w.theCycleCountReportsADiscrepancy)
	sc.Step(`^the Staged receipt response reports SKU "([^"]*)" with quantity (\d+)$`, w.theStagedReceiptResponseReports)
	sc.Step(`^the Reservations list contains (\d+) (?:entry|entries)$`, w.theReservationsListContainsEntries)
	sc.Step(`^the Reservations list contains a Reservation with status "([^"]*)"$`, w.theReservationsListContainsAReservationWithStatus)
	sc.Step(`^the ProductClassification response reports SKU "([^"]*)" with handling tags "([^"]*)"$`, w.theProductClassificationResponseReports)
	sc.Step(`^the domain event "([^"]*)" was published$`, w.theDomainEventWasPublished)
	sc.Step(`^the domain event "([^"]*)" was not published$`, w.theDomainEventWasNotPublished)
	sc.Step(`^the Usable inventory for SKU "([^"]*)" is (\d+)$`, w.theUsableInventoryForSKUIs)
}
