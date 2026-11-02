package main_test

// Step definitions for features/containers.feature (ADR 0045, 0046). The two
// inbound-receiving events and product-master's pack levels arrive from
// Kafka, so they are seeded through the SAME use cases the consumers call;
// every assertion goes through REST or the published domain events.

import (
	"context"
	"fmt"
	"net/http"
	"net/url"

	"github.com/cucumber/godog"

	inboundhttp "github.com/claudioed/inventory-storage/internal/adapters/inbound/http"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/memory"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/container"
	"github.com/claudioed/inventory-storage/internal/domain/location"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

const bddCaseGTIN = "24006381333935"

// containerState is the per-scenario wiring the container steps drive.
type containerState struct {
	handling *memory.BinHandlingUnits
	book     *usecases.BookInboundReceiptLine
	link     *usecases.LinkReceiptCarton
	pack     *usecases.ApplyPackLevel
}

// wireContainers adds the container use cases to the REST server and builds the
// consumer-side use cases over the same repositories and publisher.
func (w *world) wireContainers(s *inboundhttp.Server, reservations *memory.ReservationRepo) {
	containers := memory.NewContainerRepo()
	pending := memory.NewPendingLinkRepo()
	bookings := memory.NewReceiptBookingRepo()
	packaging := memory.NewPackagingView()
	handling := memory.NewBinHandlingUnits()
	linker := &usecases.ContainerLinker{
		Containers: containers, Pending: pending, Bookings: bookings, Packaging: packaging, Events: w.publisher, Clock: w.clock,
	}
	placement := &usecases.StowStock{Stock: w.stock, Classifications: w.classifications, LocationLookup: w.wave2.slots}

	s.StowContainer = &usecases.StowContainer{
		Containers: containers, Stock: w.stock, Locations: w.locations, HandlingUnits: handling,
		Events: w.publisher, Clock: w.clock, Placement: placement,
	}
	s.BreakCase = &usecases.BreakCase{
		Containers: containers, Stock: w.stock, Locations: w.locations, HandlingUnits: handling,
		Events: w.publisher, Clock: w.clock, Placement: placement,
	}
	s.GetContainer = &usecases.GetContainer{Containers: containers}
	s.ListBinContainers = &usecases.ListBinContainers{Containers: containers, Locations: w.locations}
	s.ReserveFullCases = &usecases.ReserveFullCases{Reserve: s.ReserveStock, Containers: containers}
	s.ConfirmPick.Containers = containers
	s.RunCycleCount.Containers = containers

	w.containers = containerState{
		handling: handling,
		book: &usecases.BookInboundReceiptLine{
			Receive: &usecases.ReceiveStock{Events: w.publisher, Clock: w.clock}, ProcessedEvents: memory.NewProcessedEventRepo(),
			Bookings: bookings, Linker: linker,
		},
		link: &usecases.LinkReceiptCarton{ProcessedEvents: memory.NewProcessedEventRepo(), Pending: pending, Linker: linker},
		pack: &usecases.ApplyPackLevel{Projection: packaging, ProcessedEvents: memory.NewProcessedEventRepo(), Linker: linker},
	}
}

// ---------------------------------------------------------------- Given ----

func (w *world) productMasterDeclaredCase(ctx context.Context, units int, sku, gtin string) error {
	_, err := w.containers.pack.Declared(ctx, usecases.PackLevelDeclared{
		EventID: "pm-" + sku + "-" + gtin, SKU: sku, Level: "CASE", GTIN: gtin, UnitsPerPack: units,
		EffectiveFrom: w.clock.Now().Add(-24 * 3600e9), Version: 1,
	})
	return err
}

func (w *world) aBinAtSiteThatAccepts(ctx context.Context, id string, capacity int, site, unit string) error {
	bin := location.RehydrateBinAtSite(shared.BinId(id), shared.Quantity(capacity), 0, shared.SiteID(site), 1)
	if err := w.locations.Save(ctx, bin); err != nil {
		return err
	}
	w.containers.handling.Set(shared.BinId(id), container.ParseHandlingUnit(unit))
	return nil
}

func (w *world) aBinAtSiteWithNoHandlingUnit(ctx context.Context, id string, capacity int, site string) error {
	return w.aBinAtSiteThatAccepts(ctx, id, capacity, site, "")
}

func (w *world) aSealedCaseIsReceived(ctx context.Context, lpn, sku, owner string) error {
	receipt := "R-" + lpn
	if err := w.lineReceived(ctx, 1, receipt, 12, sku, owner, "Good"); err != nil {
		return err
	}
	return w.sealedCartonReported(ctx, lpn, 12, sku, 1, receipt, owner)
}

func (w *world) aSealedCaseIsStowed(ctx context.Context, lpn, sku, owner, bin string) error {
	if err := w.aSealedCaseIsReceived(ctx, lpn, sku, owner); err != nil {
		return err
	}
	return w.iStowContainer(ctx, lpn, bin)
}

// ----------------------------------------------------------------- When ----

func (w *world) lineReceived(ctx context.Context, lineNo int, receipt string, qty int, sku, owner, condition string) error {
	_, err := w.containers.book.Execute(ctx, usecases.InboundReceiptLine{
		EventID: fmt.Sprintf("evt-line-%s-%d-%d", receipt, lineNo, qty), ReceiptID: receipt, ASNNumber: "ASN-1", LineNo: lineNo,
		SKU: sku, Quantity: qty, Condition: condition, OwnerRef: owner,
	})
	return err
}

func (w *world) lineReceivedForOwner(ctx context.Context, lineNo int, receipt string, qty int, sku, owner, condition string) error {
	return w.lineReceived(ctx, lineNo, receipt, qty, sku, owner, condition)
}

func cartonEvent(eventID, lpn string, sealed bool, owner string, contents ...usecases.ReceiptCartonContent) usecases.ReceiptCarton {
	return usecases.ReceiptCarton{
		EventID: eventID, ReceiptID: "", ASNNumber: "ASN-1", SSCC: lpn, LPN: lpn, Sealed: sealed, OwnerRef: owner, Contents: contents,
	}
}

func goodContent(lineNo int, sku string, qty int) usecases.ReceiptCartonContent {
	return usecases.ReceiptCartonContent{LineNo: lineNo, SKU: sku, GTIN: bddCaseGTIN, Quantity: qty, Condition: "Good"}
}

func (w *world) deliverCarton(ctx context.Context, receipt string, c usecases.ReceiptCarton) error {
	c.ReceiptID = receipt
	_, _, err := w.containers.link.Execute(ctx, c)
	return err
}

func (w *world) sealedCartonReported(ctx context.Context, lpn string, qty int, sku string, lineNo int, receipt, owner string) error {
	return w.deliverCarton(ctx, receipt, cartonEvent("evt-carton-"+lpn, lpn, true, owner, goodContent(lineNo, sku, qty)))
}

func (w *world) sealedCartonReportedAgain(ctx context.Context, lpn string, lineNo int, receipt string) error {
	return w.deliverCarton(ctx, receipt, cartonEvent("evt-carton-"+lpn+"-redelivery", lpn, true, "seller-a", goodContent(lineNo, "SKU-C", 12)))
}

func (w *world) openedCartonReported(ctx context.Context, lpn string, qty int, sku string, lineNo int, receipt string) error {
	return w.deliverCarton(ctx, receipt, cartonEvent("evt-carton-"+lpn, lpn, false, "seller-a", goodContent(lineNo, sku, qty)))
}

func (w *world) damagedCartonReported(ctx context.Context, lpn string, qty int, sku string, lineNo int, receipt string) error {
	damaged := goodContent(lineNo, sku, qty)
	damaged.Condition = "Damaged"
	return w.deliverCarton(ctx, receipt, cartonEvent("evt-carton-"+lpn, lpn, true, "seller-a", damaged))
}

func (w *world) mixedCartonReported(ctx context.Context, lpn, skuA, skuB string, lineNo int, receipt string) error {
	return w.deliverCarton(ctx, receipt, cartonEvent("evt-carton-"+lpn, lpn, true, "seller-a", goodContent(lineNo, skuA, 6), goodContent(lineNo+1, skuB, 6)))
}

func (w *world) iLookUpContainer(ctx context.Context, lpn string) error {
	return w.record(ctx, http.MethodGet, "/containers/"+url.PathEscape(lpn), nil)
}

func (w *world) iStowContainer(ctx context.Context, lpn, bin string) error {
	return w.record(ctx, http.MethodPost, "/containers/"+url.PathEscape(lpn)+"/stowages", map[string]any{"bin": bin})
}

func (w *world) iBreakContainer(ctx context.Context, lpn, looseBin string) error {
	return w.record(ctx, http.MethodPost, "/containers/"+url.PathEscape(lpn)+"/case-breaks", map[string]any{"looseBin": looseBin})
}

func (w *world) iReserveCases(ctx context.Context, cases int, sku, demand string) error {
	return w.iReserveCasesForOwner(ctx, cases, sku, demand, "")
}

func (w *world) iReserveCasesForOwner(ctx context.Context, cases int, sku, demand, owner string) error {
	body := map[string]any{"sku": sku, "cases": cases, "demandRef": demand}
	if owner != "" {
		body["ownerRef"] = owner
	}
	return w.record(ctx, http.MethodPost, "/reservations", body)
}

func (w *world) iReserveUnitsForOwner(ctx context.Context, qty int, sku, demand, owner string) error {
	return w.record(ctx, http.MethodPost, "/reservations", map[string]any{"sku": sku, "quantity": qty, "demandRef": demand, "ownerRef": owner})
}

func (w *world) iReserveCasesAndUnits(ctx context.Context, cases, qty int, sku, demand, owner string) error {
	return w.record(ctx, http.MethodPost, "/reservations", map[string]any{"sku": sku, "cases": cases, "quantity": qty, "demandRef": demand, "ownerRef": owner})
}

// ----------------------------------------------------------------- Then ----

type containerBody struct {
	LPN       string `json:"lpn"`
	SKU       string `json:"sku"`
	QtyEaches int    `json:"qtyEaches"`
	OwnerRef  string `json:"ownerRef"`
	SiteID    string `json:"siteId"`
	BinID     string `json:"binId"`
	State     string `json:"state"`
}

func (w *world) readContainer(ctx context.Context, lpn string) (containerBody, error) {
	var c containerBody
	if err := w.getJSON(ctx, "/containers/"+url.PathEscape(lpn), &c); err != nil {
		return c, err
	}
	return c, nil
}

func (w *world) containerIsSealedAndHolds(ctx context.Context, lpn, state string, qty int, sku, owner string) error {
	c, err := w.readContainer(ctx, lpn)
	if err != nil {
		return err
	}
	if c.State != state || c.QtyEaches != qty || c.SKU != sku || c.OwnerRef != owner {
		return fmt.Errorf("container %s is %s with %d eaches of %s for %q; want %s, %d, %s, %q", lpn, c.State, c.QtyEaches, c.SKU, c.OwnerRef, state, qty, sku, owner)
	}
	return nil
}

func (w *world) containerIsStowedIn(ctx context.Context, lpn, bin, site string) error {
	c, err := w.readContainer(ctx, lpn)
	if err != nil {
		return err
	}
	if c.BinID != bin || c.SiteID != site || c.State != "SEALED" {
		return fmt.Errorf("container %s is %s in %q at %q; want SEALED in %q at %q", lpn, c.State, c.BinID, c.SiteID, bin, site)
	}
	return nil
}

func (w *world) containerIsNotStowed(ctx context.Context, lpn string) error {
	c, err := w.readContainer(ctx, lpn)
	if err != nil {
		return err
	}
	if c.BinID != "" {
		return fmt.Errorf("container %s is stowed in %q, want no bin", lpn, c.BinID)
	}
	return nil
}

func (w *world) containerIsState(ctx context.Context, lpn, state string) error {
	c, err := w.readContainer(ctx, lpn)
	if err != nil {
		return err
	}
	if c.State != state {
		return fmt.Errorf("container %s is %s, want %s", lpn, c.State, state)
	}
	return nil
}

func (w *world) stagedQuantityPublished(sku string, expected int) error {
	total := 0
	for _, e := range w.publisher.Events() {
		if r, ok := e.(shared.StockReceived); ok && r.SKU.String() == sku {
			total += r.Quantity.Int()
		}
	}
	if total != expected {
		return fmt.Errorf("staged quantity published for %s = %d eaches, want %d", sku, total, expected)
	}
	return nil
}

func (w *world) usableForOwner(ctx context.Context, sku, owner string, field string, expected int) error {
	var usable map[string]any
	path := "/inventory/" + url.PathEscape(sku) + "/usable?ownerRef=" + url.QueryEscape(owner)
	if err := w.getJSON(ctx, path, &usable); err != nil {
		return err
	}
	if got, _ := usable[field].(float64); int(got) != expected {
		return fmt.Errorf("%s for %s owner %q = %v, want %d", field, sku, owner, usable[field], expected)
	}
	return nil
}

func (w *world) theUsableForOwnerIs(ctx context.Context, sku, owner string, expected int) error {
	return w.usableForOwner(ctx, sku, owner, "usable", expected)
}

func (w *world) theLooseUsableForOwnerIs(ctx context.Context, sku, owner string, expected int) error {
	return w.usableForOwner(ctx, sku, owner, "looseUsable", expected)
}

// ------------------------------------------------------------- registration --

func (w *world) registerContainerSteps(sc *godog.ScenarioContext) {
	w.registerContainerGivens(sc)
	w.registerContainerWhens(sc)
	w.registerContainerThens(sc)
}

func (w *world) registerContainerGivens(sc *godog.ScenarioContext) {
	sc.Step(`^product-master has declared a CASE of (\d+) eaches for SKU "([^"]*)" with GTIN "([^"]*)"$`, w.productMasterDeclaredCase)
	sc.Step(`^a Bin "([^"]*)" with capacity (\d+) at site "([^"]*)" that accepts "([^"]*)"$`, w.aBinAtSiteThatAccepts)
	sc.Step(`^a Bin "([^"]*)" with capacity (\d+) at site "([^"]*)" that records no handling unit$`, w.aBinAtSiteWithNoHandlingUnit)
	sc.Step(`^a sealed case "([^"]*)" of SKU "([^"]*)" for owner "([^"]*)" is received$`, w.aSealedCaseIsReceived)
	sc.Step(`^a sealed case "([^"]*)" of SKU "([^"]*)" for owner "([^"]*)" is stowed in Bin "([^"]*)"$`, w.aSealedCaseIsStowed)
}

func (w *world) registerContainerWhens(sc *godog.ScenarioContext) {
	sc.Step(`^inbound-receiving reports line (\d+) of receipt "([^"]*)" of (\d+) units of SKU "([^"]*)" for owner "([^"]*)" in (Good|Damaged) condition$`, w.lineReceivedForOwner)
	sc.Step(`^inbound-receiving reports sealed carton "([^"]*)" of (\d+) units of SKU "([^"]*)" on line (\d+) of receipt "([^"]*)" for owner "([^"]*)"$`, w.sealedCartonReported)
	sc.Step(`^inbound-receiving reports sealed carton "([^"]*)" again under a new event id on line (\d+) of receipt "([^"]*)"$`, w.sealedCartonReportedAgain)
	sc.Step(`^inbound-receiving reports an opened carton "([^"]*)" of (\d+) units of SKU "([^"]*)" on line (\d+) of receipt "([^"]*)"$`, w.openedCartonReported)
	sc.Step(`^inbound-receiving reports a damaged carton "([^"]*)" of (\d+) units of SKU "([^"]*)" on line (\d+) of receipt "([^"]*)"$`, w.damagedCartonReported)
	sc.Step(`^inbound-receiving reports a mixed carton "([^"]*)" of SKU "([^"]*)" and SKU "([^"]*)" on line (\d+) of receipt "([^"]*)"$`, w.mixedCartonReported)
	sc.Step(`^I look up Container "([^"]*)"$`, w.iLookUpContainer)
	sc.Step(`^I Stow Container "([^"]*)" into Bin "([^"]*)"$`, w.iStowContainer)
	sc.Step(`^I Break Container "([^"]*)" into loose Bin "([^"]*)"$`, w.iBreakContainer)
	sc.Step(`^I Reserve (\d+) full cases? of SKU "([^"]*)" for demand "([^"]*)"$`, w.iReserveCases)
	sc.Step(`^I Reserve (\d+) full cases? of SKU "([^"]*)" for demand "([^"]*)" for owner "([^"]*)"$`, w.iReserveCasesForOwner)
	sc.Step(`^I Reserve (\d+) units? of SKU "([^"]*)" for demand "([^"]*)" for owner "([^"]*)"$`, w.iReserveUnitsForOwner)
	sc.Step(`^I Reserve (\d+) full cases? and (\d+) units? of SKU "([^"]*)" for demand "([^"]*)" for owner "([^"]*)"$`, w.iReserveCasesAndUnits)
}

func (w *world) registerContainerThens(sc *godog.ScenarioContext) {
	sc.Step(`^the staged quantity published for SKU "([^"]*)" is (\d+) eaches$`, w.stagedQuantityPublished)
	sc.Step(`^Container "([^"]*)" is (SEALED|OPEN|EMPTY) and holds (\d+) eaches of SKU "([^"]*)" for owner "([^"]*)"$`, w.containerIsSealedAndHolds)
	sc.Step(`^Container "([^"]*)" is stowed in Bin "([^"]*)" at site "([^"]*)"$`, w.containerIsStowedIn)
	sc.Step(`^Container "([^"]*)" is not stowed$`, w.containerIsNotStowed)
	sc.Step(`^Container "([^"]*)" is (SEALED|OPEN|EMPTY)$`, w.containerIsState)
	sc.Step(`^the Usable inventory for SKU "([^"]*)" and owner "([^"]*)" is (\d+)$`, w.theUsableForOwnerIs)
	sc.Step(`^the loose usable inventory for SKU "([^"]*)" and owner "([^"]*)" is (\d+)$`, w.theLooseUsableForOwnerIs)
}
