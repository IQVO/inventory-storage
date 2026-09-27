// breaker.go wraps Client with a per-dependency circuit breaker
// (sony/gobreaker/v2, ADR-0020) AND jittered retry (cenkalti/backoff/v4)
// — mirroring order-management's productclassification.BreakerClient
// (ADR-0025, PR #107) exactly: GetSlotAttributes is a pure GET/read, safe
// to retry, and this is the ONLY synchronous cross-context HTTP client in
// this service (the LOCATION_LOOKUP_MODE=kafka path is a full-replay
// local-cache consumer, not a per-call breaker — see internal/resilience's
// package doc comment). While the breaker is OPEN, this falls back to
// PermissiveLookup's existing fail-OPEN behaviour (Known=false, nil
// error) — the SAME fallback GetSlotAttributes already had for a
// transport error or an unexpected status, just now also reachable via
// the breaker short-circuiting a call it never attempts.
package facilitylayout

import (
	"context"
	"errors"
	"time"

	"github.com/cenkalti/backoff/v4"
	gobreaker "github.com/sony/gobreaker/v2"

	"github.com/claudioed/inventory-storage/internal/domain/product"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/resilience"
)

// DependencyName labels this breaker's Prometheus gauge series
// (circuit_breaker_state{dependency="facility-layout"}).
const DependencyName = "facility-layout"

// maxRetryAttempts caps the jittered retry at 3 total attempts (1
// original + 2 retries) per the fleet resilience plan's "max 3 attempts"
// bound.
const maxRetryAttempts = 3

// retryInitialInterval/retryMaxInterval bound the exponential-backoff-
// with-jitter schedule between attempts — short, because this whole call
// is already bounded by DefaultTimeout end to end (see
// GetSlotAttributes' resilience.CallTimeout use).
const (
	retryInitialInterval = 50 * time.Millisecond
	retryMaxInterval     = 500 * time.Millisecond
)

// BreakerClient wraps Client with retry-then-circuit-breaker for
// GetSlotAttributes. This dependency has exactly one call, so this type
// stays single-purpose (mirroring order-management's
// productclassification.BreakerClient, not its two-method
// inventorystorage.BreakerClient).
type BreakerClient struct {
	breaker  *gobreaker.CircuitBreaker[product.SlotAttributes]
	inner    *Client
	fallback *PermissiveLookup
}

var _ interface {
	GetSlotAttributes(ctx context.Context, binID shared.BinId) (product.SlotAttributes, error)
} = (*BreakerClient)(nil)

// NewBreakerClient builds a BreakerClient wrapping inner. recorder is
// resilience.StateRecorder (typically
// telemetry.CircuitBreakerMetrics) — nil is a valid, documented no-op
// (see resilience.RecordStateChange), so a test that does not care about
// the metric never needs to construct one.
func NewBreakerClient(inner *Client, recorder resilience.StateRecorder) *BreakerClient {
	return newBreakerClient(inner, recorder, resilience.DefaultTimeout)
}

// NewBreakerClientWithTimeout is NewBreakerClient with an explicit
// breaker cooldown (gobreaker.Settings.Timeout) instead of
// resilience.DefaultTimeout, so a half-open-recovery test does not have
// to sleep for the full production cooldown in real time. Production
// code should always use NewBreakerClient; this exists for tests.
func NewBreakerClientWithTimeout(inner *Client, recorder resilience.StateRecorder, cooldown time.Duration) *BreakerClient {
	return newBreakerClient(inner, recorder, cooldown)
}

func newBreakerClient(inner *Client, recorder resilience.StateRecorder, cooldown time.Duration) *BreakerClient {
	return &BreakerClient{
		breaker: gobreaker.NewCircuitBreaker[product.SlotAttributes](gobreaker.Settings{
			Name:          DependencyName,
			MaxRequests:   resilience.DefaultMaxRequests,
			Interval:      resilience.DefaultInterval,
			Timeout:       cooldown,
			ReadyToTrip:   resilience.ReadyToTrip,
			IsExcluded:    func(err error) bool { return errors.Is(err, context.Canceled) },
			OnStateChange: resilience.RecordStateChange(DependencyName, recorder),
		}),
		inner:    inner,
		fallback: NewPermissiveLookup(),
	}
}

// GetSlotAttributes derives its timeout from the inbound request's
// remaining deadline (capped at DefaultTimeout), retries fetch up to
// maxRetryAttempts times with jittered backoff, and runs the whole
// retry loop through the breaker as ONE logical call — a retry storm
// against an already-degraded dependency still only ever counts as one
// success/failure toward the breaker's trip condition, not N. While the
// breaker is OPEN (or half-open and saturated), this falls back to
// PermissiveLookup — the SAME fail-open contract GetSlotAttributes
// already had for a transport error or an unexpected status, just
// reached via a different path.
func (c *BreakerClient) GetSlotAttributes(ctx context.Context, binID shared.BinId) (product.SlotAttributes, error) {
	callCtx, cancel := resilience.CallTimeout(ctx, resilience.DefaultTimeout)
	defer cancel()

	result, err := c.breaker.Execute(func() (product.SlotAttributes, error) {
		return c.retryingFetch(callCtx, binID)
	})
	if isBreakerRejection(err) {
		return c.fallback.GetSlotAttributes(ctx, binID)
	}
	if err != nil {
		// Client.GetSlotAttributes never returns a "fail open" nil
		// error for a real problem (a transport error or
		// ErrUnexpectedStatus) — any error reaching here is genuine.
		// StowStock's own checkPlacement decides fail-open vs
		// fail-closed based on whether the SKU is classified (see
		// its doc comment); this adapter's job is only to surface
		// the real error once retry/breaker are exhausted, exactly
		// as Client.GetSlotAttributes already did before this
		// breaker existed.
		return product.SlotAttributes{}, err
	}
	return result, nil
}

// retryingFetch retries inner.GetSlotAttributes with jittered
// exponential backoff, bounded to maxRetryAttempts total attempts and to
// callCtx's own deadline (whichever is tighter). Client.GetSlotAttributes
// already has the "raw, no fail-open conversion on error" shape this
// retry loop needs (see client.go: a 404 legitimately returns
// Known=false/nil, but a transport error or ErrUnexpectedStatus is
// returned as a real error) — unlike order-management's
// productclassification.Client, this repo's facilitylayout.Client never
// needed a separate fetch()/GetClassification() split to get that shape,
// so this calls the existing exported method directly.
func (c *BreakerClient) retryingFetch(callCtx context.Context, binID shared.BinId) (product.SlotAttributes, error) {
	policy := backoff.NewExponentialBackOff(
		backoff.WithInitialInterval(retryInitialInterval),
		backoff.WithMaxInterval(retryMaxInterval),
	)
	bounded := backoff.WithContext(backoff.WithMaxRetries(policy, maxRetryAttempts-1), callCtx)

	return backoff.RetryNotifyWithData(func() (product.SlotAttributes, error) {
		return c.inner.GetSlotAttributes(callCtx, binID)
	}, bounded, nil)
}

// isBreakerRejection reports whether err is gobreaker refusing to even
// attempt the call (open, or half-open and already at its probe limit)
// — the ONLY case that means "fall back to the permissive behaviour"; a
// real error FROM a call gobreaker did let through must propagate
// unchanged, exactly as it did before this breaker existed.
func isBreakerRejection(err error) bool {
	return errors.Is(err, gobreaker.ErrOpenState) || errors.Is(err, gobreaker.ErrTooManyRequests)
}
