package facilitylayout

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/claudioed/inventory-storage/internal/domain/product"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/resilience"
)

// ErrUnexpectedStatus wraps a facility-layout response status this client
// does not have specific handling for (anything other than 200 or 404).
var ErrUnexpectedStatus = errors.New("facility-layout: unexpected response status")

// HTTPDoer is the subset of *http.Client this adapter depends on, so unit
// tests can substitute a fake transport without a real server.
type HTTPDoer interface {
	Do(req *http.Request) (*http.Response, error)
}

// Client is a plain net/http implementation of
// ports.LocationClassificationLookup, calling facility-layout's
// GET /locations/{locationCode}/classification.
//
// BinId values are treated as directly usable facility-layout LocationCode
// values — a documented cross-context simplification, in the same spirit
// as fulfillment-execution's documented path_id-prefix simplification (see
// ADR 0009 and INTEGRATION.md conventions elsewhere in this platform).
type Client struct {
	baseURL string
	doer    HTTPDoer
}

// NewClient builds a Client against baseURL (e.g. from FACILITY_LAYOUT_BASE_URL).
//
// A nil doer defaults to a plain *http.Client WITHOUT http.Client.Timeout:
// a client-level timeout is a fixed, per-request wall clock that silently
// overrides the caller's context (ADR-0020 §3 — a call must honor the
// inbound request's remaining deadline, not outlast or undercut it). The
// per-attempt bound is instead derived from ctx in GetSlotAttributes via
// resilience.CallTimeout.
func NewClient(baseURL string, doer HTTPDoer) *Client {
	if doer == nil {
		doer = &http.Client{}
	}
	return &Client{baseURL: strings.TrimRight(baseURL, "/"), doer: doer}
}

type classificationResponse struct {
	Hazmat           bool   `json:"hazmat"`
	TemperatureClass string `json:"temperatureClass"`
}

// GetSlotAttributes calls facility-layout's location-classification
// endpoint for binID (treated as a LocationCode).
//
//   - A 404 is treated as Known=false (fail-open): that location is not
//     modeled in facility-layout yet.
//   - Any transport error or non-2xx/404 status returns an error, which
//     StowStock's caller normalizes to usecases.ErrLocationClassificationUnavailable.
func (c *Client) GetSlotAttributes(ctx context.Context, binID shared.BinId) (product.SlotAttributes, error) {
	// Per-attempt deadline: ctx's own deadline when it is sooner than
	// resilience.DefaultTimeout, otherwise capped at it, so one attempt can
	// neither hang forever nor be cut short by a hardcoded client timeout.
	ctx, cancel := resilience.CallTimeout(ctx, resilience.DefaultTimeout)
	defer cancel()

	endpoint := fmt.Sprintf("%s/locations/%s/classification", c.baseURL, url.PathEscape(binID.String()))

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return product.SlotAttributes{}, err
	}
	req.Header.Set("Accept", "application/json")

	resp, err := c.doer.Do(req)
	if err != nil {
		return product.SlotAttributes{}, err
	}
	defer func() { _ = resp.Body.Close() }()

	switch resp.StatusCode {
	case http.StatusOK:
		var body classificationResponse
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			return product.SlotAttributes{}, err
		}
		temperatureClass, err := product.ParseTemperatureClass(body.TemperatureClass)
		if err != nil {
			// An unparseable/empty temperature class is still a known
			// response — the zone may simply have no temperature
			// constraint. Fall back to the zero value rather than
			// failing the whole lookup for a field this SKU may not
			// even need.
			temperatureClass = ""
		}
		return product.SlotAttributes{Hazmat: body.Hazmat, TemperatureClass: temperatureClass, Known: true}, nil
	case http.StatusNotFound:
		return product.SlotAttributes{Known: false}, nil
	default:
		return product.SlotAttributes{}, fmt.Errorf("%w: %d", ErrUnexpectedStatus, resp.StatusCode)
	}
}
