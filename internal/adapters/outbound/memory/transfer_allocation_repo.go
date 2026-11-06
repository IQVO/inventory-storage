package memory

import (
	"context"
	"sync"

	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/transfer"
)

// lineLedger is the shared shape of the two in-memory transfer ledgers
// (allocation and release): one decision row per transfer_line_id, the
// Postgres ledger's UNIQUE semantics. The two repos stay distinct types
// over it — each carries its own already-decided error, because the use
// cases classify them separately — but the map-store mechanics live once.
type lineLedger[T any] struct {
	mu     sync.RWMutex
	byLine map[string]T
}

func newLineLedger[T any]() lineLedger[T] {
	return lineLedger[T]{byLine: make(map[string]T)}
}

// find returns the decided row, or nil when the line was never decided.
func (l *lineLedger[T]) find(transferLineID string) (T, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	row, ok := l.byLine[transferLineID]
	return row, ok
}

// saveUnique inserts the row; a line already decided fails with
// alreadyDecided (the Postgres UNIQUE constraint's memory twin).
func (l *lineLedger[T]) saveUnique(key string, row T, alreadyDecided error) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, exists := l.byLine[key]; exists {
		return alreadyDecided
	}
	l.byLine[key] = row
	return nil
}

// TransferAllocationRepo is an in-memory implementation of
// ports.TransferAllocationRepo, mirroring the Postgres ledger's
// unique-on-transfer_line_id semantics (a second Save for a decided line
// fails with usecases.ErrTransferLineAlreadyDecided).
type TransferAllocationRepo struct {
	ledger lineLedger[*transfer.Allocation]
}

func NewTransferAllocationRepo() *TransferAllocationRepo {
	return &TransferAllocationRepo{ledger: newLineLedger[*transfer.Allocation]()}
}

func (r *TransferAllocationRepo) FindByTransferLineID(_ context.Context, transferLineID string) (*transfer.Allocation, error) {
	if a, ok := r.ledger.find(transferLineID); ok {
		return a, nil
	}
	return nil, nil
}

func (r *TransferAllocationRepo) Save(_ context.Context, a *transfer.Allocation) error {
	return r.ledger.saveUnique(a.TransferLineID(), a, usecases.ErrTransferLineAlreadyDecided)
}
