package main_test

// Step definitions for features/totes.feature (ADR 0054): the putaway of a
// tote into the AS/RS logical location, the goods-to-person allocation that
// names one container, and the guard that keeps loose stock out of an
// AutomatedStorage bin. Every action and assertion goes through REST, so a
// refusal is read from the problem catalogue the HTTP adapter answers with;
// the event-order assertion reads the buffered publisher.

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"github.com/cucumber/godog"

	inboundhttp "github.com/claudioed/inventory-storage/internal/adapters/inbound/http"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/memory"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/location"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// toteDecantBin is the decant station the "tote X in BIN holds N of SKU"
// fixture fills a tote at before stowing it into BIN.
const toteDecantBin = "WH1-RCV-DEC-D01-01-01-A"

// toteWorld is the per-scenario tote wiring: the bins' LocationTypes (the
// facility cache's copy in production) the guard reads.
type toteWorld struct {
	types *memory.BinLocationTypes
}

// wireTotes adds the tote use cases to the REST server over the same
// repositories, publisher and clock, and gives every loose-stock write into a
// bin the same LocationTypes lookup (D9), as cmd/inventory does.
func (w *world) wireTotes(s *inboundhttp.Server) {
	cases, ok := s.GetContainer.Containers.(*memory.ContainerRepo)
	if !ok {
		panic("wireTotes: the container slice must be wired over a memory.ContainerRepo")
	}
	totes := memory.NewToteRepo(cases)
	types := memory.NewBinLocationTypes()
	placement := &usecases.StowStock{Stock: w.stock, Classifications: w.classifications, LocationLookup: w.wave2.slots}

	s.RegisterTote = &usecases.RegisterTote{Totes: totes, Containers: cases, Events: w.publisher, Clock: w.clock}
	s.GetTote = &usecases.GetTote{Totes: totes, Stock: w.stock}
	// carry (Task 6 review): the bin-level emitter, so a fill raises its STOW
	// and a stow its two RELOCATE BinStockLevelChanged (D12).
	levels := &usecases.BinLevelEmitter{Ledger: memory.NewBinLevelLedger(w.stock), Locations: w.locations, Events: w.publisher, Clock: w.clock}
	s.FillTote = &usecases.FillTote{
		Totes: totes, Stock: w.stock, Locations: w.locations, Events: w.publisher, Clock: w.clock, Placement: placement, Levels: levels,
	}
	s.StowTote = &usecases.StowTote{
		Totes: totes, Contents: w.stock, Stock: w.stock, Locations: w.locations, Events: w.publisher, Clock: w.clock, Placement: placement,
		Levels: levels,
	}
	s.ListBinTotes = &usecases.ListBinTotes{Totes: totes, Locations: w.locations}
	s.StowStock.LocationTypes = types
	s.BreakCase.LocationTypes = types
	s.StowTransferStock.LocationTypes = types
	s.RelocateStock.LocationTypes = types

	w.totes = toteWorld{types: types}
}

// ---------------------------------------------------------------- Given ----

func (w *world) toteBinOfSite(ctx context.Context, id, site string, capacity int) error {
	return w.locations.Save(ctx, location.RehydrateBinAtSite(shared.BinId(id), shared.Quantity(capacity), 0, shared.SiteID(site), 1))
}

func (w *world) toteBinOfSiteOfType(ctx context.Context, id, site string, capacity int, locationType string) error {
	if err := w.toteBinOfSite(ctx, id, site, capacity); err != nil {
		return err
	}
	w.totes.types.Set(shared.BinId(id), locationType)
	return nil
}

// toteHolds registers the tote (idempotent PUT), stows it at the decant
// station, fills it and stows it into bin; any refusal fails the fixture.
func (w *world) toteHolds(ctx context.Context, lpn, bin string, qty int, sku string) error {
	path := "/totes/" + url.PathEscape(lpn)
	steps := []struct {
		method, path string
		body         map[string]any
		want         int
	}{
		{http.MethodPut, path, nil, 0},
		{http.MethodPost, path + "/stowages", map[string]any{"bin": toteDecantBin}, http.StatusOK},
		{http.MethodPost, path + "/fills", map[string]any{"sku": sku, "quantity": qty}, http.StatusCreated},
		{http.MethodPost, path + "/stowages", map[string]any{"bin": bin}, http.StatusOK},
	}
	for _, st := range steps {
		if err := w.mustCall(ctx, st.method, st.path, st.body, st.want); err != nil {
			return err
		}
	}
	return nil
}

// mustCall issues a fixture request and fails unless it answered want (0
// accepts any 2xx).
func (w *world) mustCall(ctx context.Context, method, path string, body map[string]any, want int) error {
	var payload any
	if body != nil {
		payload = body
	}
	status, raw, _, err := w.call(ctx, method, path, payload)
	if err != nil {
		return err
	}
	if (want == 0 && status/100 != 2) || (want != 0 && status != want) {
		return fmt.Errorf("%s %s: expected %d, got %d: %s", method, path, want, status, string(raw))
	}
	return nil
}

func (w *world) aSealedCaseOfWasReceived(ctx context.Context, lpn string, units int, sku string) error {
	if err := w.productMasterDeclaredCase(ctx, units, sku, bddCaseGTIN); err != nil {
		return err
	}
	receipt := "R-" + lpn
	if err := w.lineReceived(ctx, 1, receipt, units, sku, "", "Good"); err != nil {
		return err
	}
	if err := w.deliverCarton(ctx, receipt, cartonEvent("evt-carton-"+lpn, lpn, true, "", goodContent(1, sku, units))); err != nil {
		return err
	}
	return w.mustCall(ctx, http.MethodGet, "/containers/"+url.PathEscape(lpn), nil, http.StatusOK)
}

// ----------------------------------------------------------------- When ----

// toteIsRegistered and toteIsStowedInto record the response: the same
// sentence is a Given (a later Then proves it took effect) and a When whose
// refusal the next Then asserts.
func (w *world) toteIsRegistered(ctx context.Context, lpn string) error {
	return w.recordNoServerError(ctx, http.MethodPut, "/totes/"+url.PathEscape(lpn), nil)
}

func (w *world) toteIsStowedInto(ctx context.Context, lpn, bin string) error {
	return w.recordNoServerError(ctx, http.MethodPost, "/totes/"+url.PathEscape(lpn)+"/stowages", map[string]any{"bin": bin})
}

func (w *world) recordNoServerError(ctx context.Context, method, path string, payload any) error {
	if err := w.record(ctx, method, path, payload); err != nil {
		return err
	}
	if w.status >= http.StatusInternalServerError {
		return fmt.Errorf("%s %s: server error %d: %s", method, path, w.status, string(w.body))
	}
	return nil
}

func (w *world) twoSKUsAreFilledIntoTote(ctx context.Context, qtyA int, skuA string, qtyB int, skuB, lpn string) error {
	if err := w.fillTote(ctx, qtyA, skuA, lpn); err != nil {
		return err
	}
	return w.fillTote(ctx, qtyB, skuB, lpn)
}

func (w *world) fillTote(ctx context.Context, qty int, sku, lpn string) error {
	return w.mustCall(ctx, http.MethodPost, "/totes/"+url.PathEscape(lpn)+"/fills", map[string]any{"sku": sku, "quantity": qty}, http.StatusCreated)
}

// carry (Task 6 review): one fill whose answer the next Then asserts (a
// placement refusal included).
func (w *world) unitsAreFilledIntoTote(ctx context.Context, qty int, sku, lpn string) error {
	return w.recordNoServerError(ctx, http.MethodPost, "/totes/"+url.PathEscape(lpn)+"/fills", map[string]any{"sku": sku, "quantity": qty})
}

func (w *world) unitsAreReservedFor(ctx context.Context, qty int, sku, demand string) error {
	if err := w.record(ctx, http.MethodPost, "/reservations", map[string]any{"sku": sku, "quantity": qty, "demandRef": demand}); err != nil {
		return err
	}
	if w.status != http.StatusCreated {
		return fmt.Errorf("reserving %d of %q: expected 201, got %d: %s", qty, sku, w.status, string(w.body))
	}
	return nil
}

func (w *world) unitsAreStowedLooseInto(ctx context.Context, qty int, sku, bin string) error {
	return w.record(ctx, http.MethodPost, "/stowages", map[string]any{"sku": sku, "quantity": qty, "binId": bin})
}

// ----------------------------------------------------------------- Then ----

type toteBody struct {
	LPN      string `json:"lpn"`
	BinID    string `json:"binId"`
	Contents []struct {
		SKU      string `json:"sku"`
		Quantity int    `json:"quantity"`
	} `json:"contents"`
}

var toteHoldingPattern = regexp.MustCompile(`(\d+) of "([^"]*)"`)

// toteIsInHolding reads GET /totes/{lpn} and compares its bin and its
// contents summed per SKU with the "N of "SKU" and M of "SKU"" list.
func (w *world) toteIsInHolding(ctx context.Context, lpn, bin, holding string) error {
	var t toteBody
	if err := w.getJSON(ctx, "/totes/"+url.PathEscape(lpn), &t); err != nil {
		return err
	}
	got := map[string]int{}
	for _, c := range t.Contents {
		got[c.SKU] += c.Quantity
	}
	want := map[string]int{}
	for _, m := range toteHoldingPattern.FindAllStringSubmatch(holding, -1) {
		n, _ := strconv.Atoi(m[1])
		want[m[2]] += n
	}
	if t.BinID != bin || fmt.Sprint(got) != fmt.Sprint(want) {
		return fmt.Errorf("tote %s is in %q holding %v; want %q holding %v", lpn, t.BinID, got, bin, want)
	}
	return nil
}

func (w *world) binsHoldEaches(ctx context.Context, binA string, qtyA int, binB string, qtyB int) error {
	for bin, want := range map[string]int{binA: qtyA, binB: qtyB} {
		var body struct {
			Occupied int `json:"occupied"`
		}
		if err := w.getJSON(ctx, "/bins/"+url.PathEscape(bin), &body); err != nil {
			return err
		}
		if body.Occupied != want {
			return fmt.Errorf("bin %s holds %d eaches, want %d", bin, body.Occupied, want)
		}
	}
	return nil
}

// toteEventLPN returns the LPN of a tote event, "" for any other event.
func toteEventLPN(e shared.DomainEvent) string {
	switch ev := e.(type) {
	case shared.ToteRegistered:
		return ev.LPN
	case shared.ToteFilled:
		return ev.LPN
	case shared.ToteStowed:
		return ev.LPN
	}
	return ""
}

func (w *world) toteEventsRaisedInOrder(names, lpn string) error {
	var got []string
	for _, e := range w.publisher.Events() {
		if toteEventLPN(e) == lpn {
			got = append(got, e.EventName())
		}
	}
	if strings.Join(got, ", ") != names {
		return fmt.Errorf("tote events for %s: got %q, want %q", lpn, strings.Join(got, ", "), names)
	}
	return nil
}

type reservationAllocationsBody struct {
	Allocations []struct {
		BinID       string `json:"binId"`
		Quantity    int    `json:"quantity"`
		ContainerID string `json:"containerId"`
	} `json:"allocations"`
}

func (w *world) theReservationHasOneAllocationFromContainer(qty int, lpn, bin string) error {
	var res reservationAllocationsBody
	if err := w.decode(&res); err != nil {
		return err
	}
	if len(res.Allocations) != 1 {
		return fmt.Errorf("expected one allocation, got %+v", res.Allocations)
	}
	a := res.Allocations[0]
	if a.Quantity != qty || a.ContainerID != lpn || a.BinID != bin {
		return fmt.Errorf("allocation is %d from container %q in %q; want %d from %q in %q", a.Quantity, a.ContainerID, a.BinID, qty, lpn, bin)
	}
	return nil
}

// toteRefusalStatus is the status each tote refusal answers with (ADR 0054
// items 5-8, and the placement rule a fill and a stow apply).
// carry (Task 11 review): the refusal step pins the exact status, not any 4xx.
var toteRefusalStatus = map[string]int{
	"license-plate-taken":                  http.StatusConflict,
	"tote-reserved":                        http.StatusConflict,
	"automated-storage-requires-container": http.StatusConflict,
	"hazmat-zone-required":                 http.StatusConflict,
}

// theToteActionIsRefusedWith asserts a problem+json answer with the status
// toteRefusalStatus pins for the slug, whose type slug is the one named (the
// HTTP adapter's problem catalogue).
func (w *world) theToteActionIsRefusedWith(_, slug string) error {
	want, ok := toteRefusalStatus[slug]
	if !ok {
		return fmt.Errorf("no status pinned for refusal %q", slug)
	}
	if w.status != want {
		return fmt.Errorf("expected a %d refusal, got %d: %s", want, w.status, string(w.body))
	}
	return w.theProblemDetailTypeIs(slug)
}

// ------------------------------------------------------------- registration --

func (w *world) registerToteSteps(sc *godog.ScenarioContext) {
	sc.Step(`^bin "([^"]*)" of site "([^"]*)" with capacity (\d+)$`, w.toteBinOfSite)
	sc.Step(`^bin "([^"]*)" of site "([^"]*)" with capacity (\d+) of location type "([^"]*)"$`, w.toteBinOfSiteOfType)
	sc.Step(`^tote "([^"]*)" in "([^"]*)" holds (\d+) of "([^"]*)"$`, w.toteHolds)
	sc.Step(`^a sealed case "([^"]*)" of (\d+) "([^"]*)" was received$`, w.aSealedCaseOfWasReceived)
	sc.Step(`^tote "([^"]*)" is registered$`, w.toteIsRegistered)
	sc.Step(`^tote "([^"]*)" is stowed into "([^"]*)"$`, w.toteIsStowedInto)
	sc.Step(`^(\d+) of "([^"]*)" and (\d+) of "([^"]*)" are filled into tote "([^"]*)"$`, w.twoSKUsAreFilledIntoTote)
	sc.Step(`^(\d+) of "([^"]*)" are filled into tote "([^"]*)"$`, w.unitsAreFilledIntoTote)
	sc.Step(`^(\d+) of "([^"]*)" are reserved for "([^"]*)"$`, w.unitsAreReservedFor)
	sc.Step(`^(\d+) of "([^"]*)" are stowed loose into "([^"]*)"$`, w.unitsAreStowedLooseInto)
	sc.Step(`^tote "([^"]*)" is in "([^"]*)" holding (.+)$`, w.toteIsInHolding)
	sc.Step(`^bin "([^"]*)" holds (\d+) eaches and bin "([^"]*)" holds (\d+)$`, w.binsHoldEaches)
	sc.Step(`^the events "([^"]*)" were raised for "([^"]*)" in that order$`, w.toteEventsRaisedInOrder)
	sc.Step(`^the reservation has one allocation of (\d+) from container "([^"]*)" in "([^"]*)"$`, w.theReservationHasOneAllocationFromContainer)
	sc.Step(`^the (stow|registration|fill) is refused with "([^"]*)"$`, w.theToteActionIsRefusedWith)
}
