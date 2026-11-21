package usecases_test

import (
	"context"
	"errors"
	"testing"

	"github.com/claudioed/inventory-storage/internal/adapters/outbound/memory"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/container"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// tw (tote world) wires the tote use cases over the in-memory adapters of the
// container world, so a sealed case and a tote share one LPN namespace.
type tw struct {
	*cw
	Totes    *memory.ToteRepo
	Types    *memory.BinLocationTypes
	Register *usecases.RegisterTote
	Get      *usecases.GetTote
	List     *usecases.ListBinTotes
	Fill     *usecases.FillTote
	Stow     *usecases.StowTote
}

func newTW() *tw {
	w := &tw{cw: newCW()}
	w.Totes = memory.NewToteRepo(w.Containers)
	w.Types = memory.NewBinLocationTypes()
	w.Register = &usecases.RegisterTote{Totes: w.Totes, Containers: w.Containers, Events: w.Events, Clock: w.Clock}
	w.Get = &usecases.GetTote{Totes: w.Totes, Stock: w.Stock}
	w.List = &usecases.ListBinTotes{Totes: w.Totes, Locations: w.Locations}
	w.Fill = &usecases.FillTote{Totes: w.Totes, Stock: w.Stock, Locations: w.Locations, Events: w.Events, Clock: w.Clock}
	w.Stow = &usecases.StowTote{Totes: w.Totes, Contents: w.Stock, Stock: w.Stock, Locations: w.Locations, Events: w.Events, Clock: w.Clock}
	w.Linker.Totes = w.Totes
	return w
}

func (w *tw) eventNames() []string {
	var out []string
	for _, e := range w.Events.Events() {
		out = append(out, e.EventName())
	}
	return out
}

func TestRegisterToteCreatesItOnceAndAnnouncesIt(t *testing.T) {
	w := newTW()
	tote, created, err := w.Register.Execute(context.Background(), "TOTE-1")
	if err != nil || !created || tote.LPN() != "TOTE-1" || tote.IsPlaced() {
		t.Fatalf("first: tote %+v created %v err %v", tote, created, err)
	}
	again, created, err := w.Register.Execute(context.Background(), "TOTE-1")
	if err != nil || created || again.LPN() != "TOTE-1" {
		t.Fatalf("repeat: created %v err %v", created, err)
	}
	assertFacts(t, map[string]any{"events": w.eventNames()}, map[string]any{"events": []string{"ToteRegistered"}})
}

func TestRegisterToteRefusesTheLicensePlateOfASealedCase(t *testing.T) {
	w := newTW()
	w.registered(t, "003400000000000017")
	before := len(w.Events.Events())
	if _, _, err := w.Register.Execute(context.Background(), "003400000000000017"); !errors.Is(err, usecases.ErrLicensePlateTaken) {
		t.Fatalf("err = %v, want ErrLicensePlateTaken", err)
	}
	if got, _ := w.Totes.FindByLPN(context.Background(), "003400000000000017"); got != nil || len(w.Events.Events()) != before {
		t.Fatal("a refused registration wrote a tote or an event")
	}
}

func TestRegisterToteRefusesAnInvalidLicensePlate(t *testing.T) {
	w := newTW()
	if _, _, err := w.Register.Execute(context.Background(), "TOTE/1"); !errors.Is(err, container.ErrInvalidLPN) {
		t.Fatalf("err = %v", err)
	}
}

func TestGetToteUnknownIsNotFound(t *testing.T) {
	w := newTW()
	if _, err := w.Get.Execute(context.Background(), "NOPE"); !errors.Is(err, usecases.ErrToteNotFound) {
		t.Fatalf("err = %v", err)
	}
}

func TestListBinTotesUnknownBinIsNotFound(t *testing.T) {
	w := newTW()
	if _, err := w.List.Page(context.Background(), shared.BinId("NOPE"), "", 10); !errors.Is(err, usecases.ErrBinNotFound) {
		t.Fatalf("err = %v", err)
	}
}
