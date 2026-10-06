package memory

import (
	"context"
	"sync"

	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/transfer"
)

// TransferAllocationRepo is an in-memory implementation of
// ports.TransferAllocationRepo, mirroring the Postgres ledger's
// unique-on-transfer_line_id semantics (a second Save for a decided line
// fails with usecases.ErrTransferLineAlreadyDecided).
type TransferAllocationRepo struct {
	mu     sync.RWMutex
	byLine map[string]*transfer.Allocation
}

func NewTransferAllocationRepo() *TransferAllocationRepo {
	return &TransferAllocationRepo{byLine: make(map[string]*transfer.Allocation)}
}

func (r *TransferAllocationRepo) FindByTransferLineID(_ context.Context, transferLineID string) (*transfer.Allocation, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if a, ok := r.byLine[transferLineID]; ok {
		return a, nil
	}
	return nil, nil
}

func (r *TransferAllocationRepo) Save(_ context.Context, a *transfer.Allocation) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.byLine[a.TransferLineID()]; exists {
		return usecases.ErrTransferLineAlreadyDecided
	}
	r.byLine[a.TransferLineID()] = a
	return nil
}
