package kafka

import (
	"context"
	"fmt"
	"sync"

	"github.com/claudioed/inventory-storage/internal/application/ports"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/container"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
)

// heldFake is an in-test ports.HeldStockRepo (an inbound adapter test must
// not import an outbound adapter): it keeps the saved units and the carton
// marks as "receipt/line/lpn".
type heldFake struct {
	mu    sync.Mutex
	units []*stock.HeldUnit
	marks []string
}

func (h *heldFake) Save(_ context.Context, u *stock.HeldUnit) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.units = append(h.units, u)
	return nil
}

func (h *heldFake) FindByID(context.Context, string) (*stock.HeldUnit, error) { return nil, nil }

func (h *heldFake) ListHeld(context.Context, ports.HeldStockQuery) ([]*stock.HeldUnit, error) {
	return nil, nil
}

func (h *heldFake) MarkCarton(_ context.Context, receiptID string, lineNo int, lpn, _ string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.marks = append(h.marks, fmt.Sprintf("%s/%d/%s", receiptID, lineNo, lpn))
	return nil
}

func (h *heldFake) NextID(context.Context) (string, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return fmt.Sprintf("hu-%d", len(h.units)+1), nil
}

type nopSettler struct{}

func (nopSettler) Settle(context.Context, string, int) (usecases.SettleResult, error) {
	return usecases.SettleResult{}, nil
}

type nopPending struct{}

func (nopPending) Add(context.Context, container.PendingLink) error { return nil }
func (nopPending) FindPendingByLine(context.Context, string, int) ([]container.PendingLink, error) {
	return nil, nil
}
func (nopPending) FindPendingBySKU(context.Context, shared.SKU) ([]container.PendingLink, error) {
	return nil, nil
}
func (nopPending) Remove(context.Context, string, int, string) error         { return nil }
func (nopPending) Reject(context.Context, string, int, string, string) error { return nil }
