package usecases_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/claudioed/inventory-storage/internal/adapters/outbound/memory"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/container"
	"github.com/claudioed/inventory-storage/internal/domain/location"
	"github.com/claudioed/inventory-storage/internal/domain/packaging"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

const (
	caseGTIN = "24006381333935"
	eachGTIN = "04006381333931"
)

// cw (container world) wires every container use case over the in-memory
// adapters, sharing one stock repo, outbox and clock.
type cw struct {
	env
	Containers *memory.ContainerRepo
	Pending    *memory.PendingLinkRepo
	Bookings   *memory.ReceiptBookingRepo
	Packaging  *memory.PackagingView
	Handling   *memory.BinHandlingUnits
	Processed  *memory.ProcessedEventRepo

	Linker  *usecases.ContainerLinker
	Link    *usecases.LinkReceiptCarton
	Book    *usecases.BookInboundReceiptLine
	Pack    *usecases.ApplyPackLevel
	Stow    *usecases.StowContainer
	Break   *usecases.BreakCase
	Reserve *usecases.ReserveStock
	Cases   *usecases.ReserveFullCases
	Confirm *usecases.ConfirmPick
	Usable  *usecases.GetUsable
}

// assertFacts compares labelled facts in one place, so a test can check many
// outcomes without a branch per fact.
func assertFacts(t *testing.T, got, want map[string]any) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("facts differ:\n got  %v\n want %v", got, want)
	}
}

func newCW() *cw {
	e := newEnv()
	w := &cw{
		env: e, Containers: memory.NewContainerRepo(), Pending: memory.NewPendingLinkRepo(), Bookings: memory.NewReceiptBookingRepo(),
		Packaging: memory.NewPackagingView(), Handling: memory.NewBinHandlingUnits(), Processed: memory.NewProcessedEventRepo(),
	}
	w.Linker = &usecases.ContainerLinker{Containers: w.Containers, Pending: w.Pending, Bookings: w.Bookings, Packaging: w.Packaging, Events: e.Events, Clock: e.Clock}
	w.Link = &usecases.LinkReceiptCarton{ProcessedEvents: w.Processed, Pending: w.Pending, Linker: w.Linker}
	w.Book = &usecases.BookInboundReceiptLine{
		Receive: &usecases.ReceiveStock{Events: e.Events, Clock: e.Clock}, ProcessedEvents: memory.NewProcessedEventRepo(),
		Bookings: w.Bookings, Linker: w.Linker,
	}
	w.Pack = &usecases.ApplyPackLevel{Projection: w.Packaging, ProcessedEvents: memory.NewProcessedEventRepo(), Linker: w.Linker}
	w.Stow = &usecases.StowContainer{Containers: w.Containers, Stock: e.Stock, Locations: e.Locations, HandlingUnits: w.Handling, Events: e.Events, Clock: e.Clock}
	w.Break = &usecases.BreakCase{Containers: w.Containers, Stock: e.Stock, Locations: e.Locations, HandlingUnits: w.Handling, Events: e.Events, Clock: e.Clock}
	w.Reserve = &usecases.ReserveStock{Stock: e.Stock, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock}
	w.Cases = &usecases.ReserveFullCases{Reserve: w.Reserve, Containers: w.Containers}
	w.Confirm = &usecases.ConfirmPick{Stock: e.Stock, Locations: e.Locations, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock, Containers: w.Containers}
	w.Usable = &usecases.GetUsable{Stock: e.Stock}
	return w
}

// bin registers a bin at site with the given handling unit.
func (w *cw) bin(t *testing.T, id, site string, capacity int, hu container.HandlingUnit) {
	t.Helper()
	b := location.RehydrateBinAtSite(shared.BinId(id), shared.Quantity(capacity), 0, shared.SiteID(site), 1)
	if err := w.Locations.Save(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	w.Handling.Set(shared.BinId(id), hu)
}

// declareCase projects a CASE level of 12 for SKU-1.
func (w *cw) declareCase(t *testing.T, units int) {
	t.Helper()
	_, err := w.Pack.Declared(context.Background(), usecases.PackLevelDeclared{
		EventID: "pm-case", SKU: "SKU-1", Level: "CASE", GTIN: caseGTIN, ChildGTIN: eachGTIN, UnitsPerPack: units,
		EffectiveFrom: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).Add(-time.Hour), Version: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
}

// carton is a sealed, Good, single-SKU carton of 12 on line 1 of receipt R-1.
func carton(id, lpn string) usecases.ReceiptCarton {
	return usecases.ReceiptCarton{
		EventID: id, ReceiptID: "R-1", ASNNumber: "ASN-1", SSCC: lpn, LPN: lpn, Sealed: true, OwnerRef: "seller-a",
		Contents: []usecases.ReceiptCartonContent{{LineNo: 1, SKU: "SKU-1", GTIN: caseGTIN, Quantity: 12, Lot: "L1", Expiry: "2027-03-01", Condition: "Good"}},
	}
}

// bookLine books qty eaches of SKU-1 on line 1 of receipt R-1.
func (w *cw) bookLine(t *testing.T, eventID string, qty int) {
	t.Helper()
	out, err := w.Book.Execute(context.Background(), usecases.InboundReceiptLine{
		EventID: eventID, ReceiptID: "R-1", ASNNumber: "ASN-1", LineNo: 1, SKU: "SKU-1", Quantity: qty, Condition: "Good", OwnerRef: "seller-a",
	})
	if err != nil || out != usecases.LineBooked {
		t.Fatalf("book line: %s %v", out, err)
	}
}

func (w *cw) link(t *testing.T, c usecases.ReceiptCarton) usecases.CartonOutcome {
	t.Helper()
	out, _, err := w.Link.Execute(context.Background(), c)
	if err != nil {
		t.Fatalf("link %s: %v", c.EventID, err)
	}
	return out
}

// registered registers a container on line 1 (declaring the case, booking 12
// and linking one carton) and returns its LPN.
func (w *cw) registered(t *testing.T, lpn string) {
	t.Helper()
	w.declareCase(t, 12)
	w.bookLine(t, "ev-line-"+lpn, 12)
	w.link(t, carton("ev-carton-"+lpn, lpn))
	if c, _ := w.Containers.FindByLPN(context.Background(), lpn); c == nil {
		t.Fatalf("container %s was not registered", lpn)
	}
}

func countEvents(e env, name string) int {
	n := 0
	for _, ev := range e.Events.Events() {
		if ev.EventName() == name {
			n++
		}
	}
	return n
}

func (w *cw) containerOf(t *testing.T, lpn string) *container.Container {
	t.Helper()
	c, err := w.Containers.FindByLPN(context.Background(), lpn)
	if err != nil || c == nil {
		t.Fatalf("container %s: %v %v", lpn, c, err)
	}
	return c
}

var _ = packaging.LevelCase
