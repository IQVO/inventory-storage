package main_test

// Transfer-saga step definitions (ADR 0031) that complement the wave-2 ones in
// features_wave2_test.go: the allocation command with its refusal outcome,
// published-event counts and leg assertions, raw request bodies for
// validation, and the compact "BIN=qty" stow notation. Origin-side allocation
// is a Kafka command, so it is driven through the real use case; everything
// else goes over REST and is asserted from responses and published events.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/cucumber/godog"

	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// anAllocationIsRequested submits the command and remembers the outcome so a
// scenario can assert a refusal; it never fails the step by itself.
func (w *world) anAllocationIsRequested(ctx context.Context, qty int, sku, site, transferID, lineID string) error {
	_, w.wave2.allocateErr = w.wave2.allocateTransfer.Execute(ctx, usecases.TransferCommand{
		TransferID:     transferID,
		TransferLineID: lineID,
		OriginSiteID:   shared.SiteID(site),
		SKU:            shared.SKU(sku),
		Quantity:       shared.Quantity(qty),
	})
	return nil
}

func (w *world) theAllocationCommandIsRefusedAsMalformed() error {
	if !errors.Is(w.wave2.allocateErr, usecases.ErrMalformedTransferCommand) {
		return fmt.Errorf("expected a malformed-command refusal, got %v", w.wave2.allocateErr)
	}
	return nil
}

func (w *world) theAllocationCommandIsProcessed() error {
	if w.wave2.allocateErr != nil {
		return fmt.Errorf("expected the allocation command to be processed, got %w", w.wave2.allocateErr)
	}
	return nil
}

func (w *world) eventsNamed(name string) []shared.DomainEvent {
	var out []shared.DomainEvent
	for _, e := range w.publisher.Events() {
		if e.EventName() == name {
			out = append(out, e)
		}
	}
	return out
}

func (w *world) exactlyNEventsPublished(n int, name string) error {
	if got := len(w.eventsNamed(name)); got != n {
		return fmt.Errorf("expected %d %q events, got %d", n, name, got)
	}
	return nil
}

// parseBinQuantities reads the "BIN-1=3,BIN-2=2" notation.
func parseBinQuantities(spec string) (map[string]int, error) {
	out := map[string]int{}
	for _, part := range strings.Split(spec, ",") {
		kv := strings.SplitN(strings.TrimSpace(part), "=", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("bad bin spec %q", part)
		}
		q, err := strconv.Atoi(kv[1])
		if err != nil {
			return nil, err
		}
		out[kv[0]] = q
	}
	return out, nil
}

func sameQuantities(got, want map[string]int) bool {
	if len(got) != len(want) {
		return false
	}
	for bin, q := range want {
		if got[bin] != q {
			return false
		}
	}
	return true
}

func (w *world) theAllocationReservedLegs(spec string) error {
	evs := w.eventsNamed("TransferStockAllocated")
	if len(evs) != 1 {
		return fmt.Errorf("expected one TransferStockAllocated event, got %d", len(evs))
	}
	alloc, ok := evs[0].(shared.TransferStockAllocated)
	if !ok {
		return fmt.Errorf("unexpected event type %T", evs[0])
	}
	got := map[string]int{}
	for _, leg := range alloc.Allocations {
		got[leg.BinID.String()] += leg.Quantity.Int()
	}
	want, err := parseBinQuantities(spec)
	if err != nil {
		return err
	}
	if !sameQuantities(got, want) {
		return fmt.Errorf("expected allocation legs %v, got %v", want, got)
	}
	return nil
}

func (w *world) aPostIsSentWithBody(ctx context.Context, path, body string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.server.URL+path, bytes.NewReader([]byte(body)))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := w.server.Client().Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	w.status, w.body, w.headers = resp.StatusCode, raw, resp.Header
	return nil
}

func (w *world) theDestinationSiteScans(ctx context.Context, site string, qty int, sku, transferID, lineID string) error {
	return w.stageReceipt(ctx, lineID, transferID, site, qty, sku)
}

func (w *world) theReceiptIsStowedInto(ctx context.Context, lineID, spec string) error {
	var bins []map[string]any
	for _, part := range strings.Split(spec, ",") {
		kv := strings.SplitN(strings.TrimSpace(part), "=", 2)
		if len(kv) != 2 {
			return fmt.Errorf("bad bin spec %q", part)
		}
		q, err := strconv.Atoi(kv[1])
		if err != nil {
			return err
		}
		bins = append(bins, map[string]any{"binId": kv[0], "quantity": q})
	}
	return w.record(ctx, http.MethodPost, "/transfers/"+lineID+"/stow", map[string]any{"bins": bins})
}

func (w *world) theStowAllocationsAre(spec string) error {
	var m struct {
		Allocations []struct {
			BinID    string `json:"binId"`
			Quantity int    `json:"quantity"`
		} `json:"allocations"`
	}
	if err := w.decode(&m); err != nil {
		return err
	}
	got := map[string]int{}
	for _, a := range m.Allocations {
		got[a.BinID] += a.Quantity
	}
	want, err := parseBinQuantities(spec)
	if err != nil {
		return err
	}
	if !sameQuantities(got, want) {
		return fmt.Errorf("expected stow allocations %v, got %v", want, got)
	}
	return nil
}

func (w *world) binReportsOccupied(ctx context.Context, binID string, occupied int) error {
	status, body, _, err := w.call(ctx, http.MethodGet, "/bins/"+binID, nil)
	if err != nil {
		return err
	}
	var b struct {
		Occupied int `json:"occupied"`
	}
	if status != http.StatusOK || json.Unmarshal(body, &b) != nil || b.Occupied != occupied {
		return fmt.Errorf("expected Bin %s occupied %d, got %d %s", binID, occupied, status, body)
	}
	return nil
}

func (w *world) registerTransferSagaSteps(sc *godog.ScenarioContext) {
	sc.Step(`^an allocation of (-?\d+) units? of SKU "([^"]*)" from Site "([^"]*)" is requested for transfer "([^"]*)" line "([^"]*)"$`, w.anAllocationIsRequested)
	sc.Step(`^the allocation command is refused as malformed$`, w.theAllocationCommandIsRefusedAsMalformed)
	sc.Step(`^the allocation command is processed$`, w.theAllocationCommandIsProcessed)
	sc.Step(`^exactly (\d+) "([^"]*)" domain events? (?:is|are) published$`, w.exactlyNEventsPublished)
	sc.Step(`^the allocation reserved "([^"]*)"$`, w.theAllocationReservedLegs)
	sc.Step(`^a POST is sent to "([^"]*)" with the body (.*)$`, w.aPostIsSentWithBody)
	sc.Step(`^the destination Site "([^"]*)" scans (\d+) units? of SKU "([^"]*)" for transfer "([^"]*)" line "([^"]*)"$`, w.theDestinationSiteScans)
	sc.Step(`^the receipt for line "([^"]*)" is stowed into "([^"]*)"$`, w.theReceiptIsStowedInto)
	sc.Step(`^the stow allocations are "([^"]*)"$`, w.theStowAllocationsAre)
	sc.Step(`^Bin "([^"]*)" reports (\d+) occupied$`, w.binReportsOccupied)
}
