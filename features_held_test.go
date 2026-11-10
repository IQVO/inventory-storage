package main_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"

	"github.com/cucumber/godog"
)

// Steps of features/held_stock.feature (ADR 0052). They read and decide over
// the real REST routes; the held repository is the in-memory one the world
// wires into the server.

type heldListBody struct {
	Items []struct {
		ID       string `json:"id"`
		SKU      string `json:"sku"`
		Quantity int    `json:"quantity"`
		Reason   string `json:"reason"`
	} `json:"items"`
}

func (w *world) listHeld(ctx context.Context, sku string) (heldListBody, error) {
	var list heldListBody
	err := w.getJSON(ctx, "/held-stock?sku="+url.QueryEscape(sku), &list)
	return list, err
}

// theHeldStockOfSKUIs asserts the count and the summed quantity of the units
// still held for sku, all for one reason.
func (w *world) theHeldStockOfSKUIs(ctx context.Context, sku string, units, eaches int, reason string) error {
	list, err := w.listHeld(ctx, sku)
	if err != nil {
		return err
	}
	total := 0
	for _, item := range list.Items {
		if item.Reason != reason {
			continue
		}
		total += item.Quantity
	}
	if len(list.Items) != units || total != eaches {
		return fmt.Errorf("expected %d held unit(s) of %d eaches for %q with reason %q, got %d unit(s) of %d eaches (%+v)",
			units, eaches, sku, reason, len(list.Items), total, list.Items)
	}
	return nil
}

// iDecideForTheHeldUnit decides the one unit still held for sku.
func (w *world) iDecideForTheHeldUnit(ctx context.Context, disposition, sku string) error {
	list, err := w.listHeld(ctx, sku)
	if err != nil {
		return err
	}
	if len(list.Items) != 1 {
		return fmt.Errorf("expected exactly one held unit of %q, got %d", sku, len(list.Items))
	}
	return w.record(ctx, http.MethodPost, "/held-stock/"+url.PathEscape(list.Items[0].ID)+"/dispositions",
		map[string]any{"disposition": disposition})
}

// iDecideForTheSettledHeldUnit tries a second decision on a unit that no longer
// is in the held list: it finds the unit through the repository.
func (w *world) iDecideForTheSettledHeldUnit(ctx context.Context, disposition, sku string) error {
	for _, h := range w.held.All() {
		if h.SKU().String() == sku {
			return w.record(ctx, http.MethodPost, "/held-stock/"+url.PathEscape(h.ID())+"/dispositions",
				map[string]any{"disposition": disposition})
		}
	}
	return errors.New("no held unit of " + sku + " exists")
}

func (w *world) registerHeldSteps(sc *godog.ScenarioContext) {
	sc.Step(`^the held stock of SKU "([^"]*)" is (\d+) units? of (\d+) eaches for reason "([^"]*)"$`, w.theHeldStockOfSKUIs)
	sc.Step(`^I decide (RELEASE|RETURN_TO_VENDOR|SCRAP|REWORK) for the held unit of SKU "([^"]*)"$`, w.iDecideForTheHeldUnit)
	sc.Step(`^I decide (RELEASE|RETURN_TO_VENDOR|SCRAP|REWORK) for the settled held unit of SKU "([^"]*)"$`, w.iDecideForTheSettledHeldUnit)
}
