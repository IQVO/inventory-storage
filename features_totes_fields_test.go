package main_test

import (
	"fmt"
	"strings"

	"github.com/cucumber/godog"

	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// carry (Task 11 review): the tote events carry the fields ADR 0054 item 9
// lists, not only their names. One row per ToteFilled/ToteStowed of the tote,
// in publish order: event|site|fromBin|bin|contents (sku=qty, comma-joined).
func toteEventRow(e shared.DomainEvent) (lpn, row string) {
	switch ev := e.(type) {
	case shared.ToteFilled:
		return ev.LPN, fmt.Sprintf("ToteFilled|%s||%s|%s=%d", ev.SiteID, ev.BinID, ev.SKU, ev.Quantity.Int())
	case shared.ToteStowed:
		lines := make([]string, 0, len(ev.Contents))
		for _, l := range ev.Contents {
			lines = append(lines, fmt.Sprintf("%s=%d", l.SKU, l.Quantity.Int()))
		}
		return ev.LPN, fmt.Sprintf("ToteStowed|%s|%s|%s|%s", ev.SiteID, ev.FromBin, ev.ToBin, strings.Join(lines, ","))
	}
	return "", ""
}

func (w *world) theToteEventsCarry(lpn string, table *godog.Table) error {
	var got, want []string
	for _, e := range w.publisher.Events() {
		if l, row := toteEventRow(e); l == lpn {
			got = append(got, row)
		}
	}
	for _, r := range table.Rows[1:] {
		cells := make([]string, len(r.Cells))
		for i, c := range r.Cells {
			cells[i] = c.Value
		}
		want = append(want, strings.Join(cells, "|"))
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		return fmt.Errorf("tote events for %s:\n got  %q\n want %q", lpn, got, want)
	}
	return nil
}

func (w *world) registerToteFieldSteps(sc *godog.ScenarioContext) {
	sc.Step(`^the fill and stow events of tote "([^"]*)" carry:$`, w.theToteEventsCarry)
}
