package memory

import (
	"context"
	"strconv"
	"sync"

	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/transfer"
)

// TransferReceiptRepo is an in-memory implementation of
// ports.TransferReceiptRepo, mirroring the Postgres table's semantics:
// unique on transfer_line_id (a second Save fails with
// usecases.ErrTransferReceiptAlreadyStaged) and a STOWED-state guard on
// SaveStowed (usecases.ErrTransferReceiptAlreadyStowed).
//
// states tracks the repo's OWN view of each row's state (exactly like
// the database's row state): the caller mutates its aggregate pointer
// BEFORE calling SaveStowed, so guarding on the stored pointer's state
// would race the mutation itself.
type TransferReceiptRepo struct {
	mu     sync.RWMutex
	byLine map[string]*transfer.Receipt
	states map[string]transfer.ReceiptState
}

func NewTransferReceiptRepo() *TransferReceiptRepo {
	return &TransferReceiptRepo{
		byLine: make(map[string]*transfer.Receipt),
		states: make(map[string]transfer.ReceiptState),
	}
}

func (r *TransferReceiptRepo) FindByTransferLineID(_ context.Context, transferLineID string) (*transfer.Receipt, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if receipt, ok := r.byLine[transferLineID]; ok {
		return receipt, nil
	}
	return nil, nil
}

func (r *TransferReceiptRepo) Save(_ context.Context, receipt *transfer.Receipt) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.byLine[receipt.TransferLineID()]; exists {
		return usecases.ErrTransferReceiptAlreadyStaged
	}
	r.byLine[receipt.TransferLineID()] = receipt
	r.states[receipt.TransferLineID()] = transfer.ReceiptStaged
	return nil
}

func (r *TransferReceiptRepo) SaveStowed(_ context.Context, receipt *transfer.Receipt) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.byLine[receipt.TransferLineID()]; !ok {
		return usecases.ErrReceiptNotStaged
	}
	if r.states[receipt.TransferLineID()] == transfer.ReceiptStowed {
		return usecases.ErrTransferReceiptAlreadyStowed
	}
	r.byLine[receipt.TransferLineID()] = receipt
	r.states[receipt.TransferLineID()] = transfer.ReceiptStowed
	return nil
}

// InventoryExceptionRepo is an in-memory implementation of
// ports.InventoryExceptionRepo: one row per exact scan tuple, mirroring
// the Postgres unique constraint.
type InventoryExceptionRepo struct {
	mu     sync.RWMutex
	byScan map[string]*transfer.Exception
}

func NewInventoryExceptionRepo() *InventoryExceptionRepo {
	return &InventoryExceptionRepo{byScan: make(map[string]*transfer.Exception)}
}

// scanKey is the exact-scan tuple the Postgres unique constraint covers.
func scanKey(lineID string, site shared.SiteID, sku shared.SKU, qty shared.Quantity) string {
	return lineID + "|" + site.String() + "|" + sku.String() + "|" + strconv.Itoa(qty.Int())
}

func (r *InventoryExceptionRepo) FindByScan(_ context.Context, transferLineID string, destinationSiteID shared.SiteID, sku shared.SKU, receivedQty shared.Quantity) (*transfer.Exception, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if e, ok := r.byScan[scanKey(transferLineID, destinationSiteID, sku, receivedQty)]; ok {
		return e, nil
	}
	return nil, nil
}

func (r *InventoryExceptionRepo) Save(_ context.Context, e *transfer.Exception) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := scanKey(e.TransferLineID(), e.DestinationSiteID(), e.SKU(), e.ReceivedQuantity())
	if _, exists := r.byScan[key]; exists {
		return usecases.ErrTransferExceptionScanExists
	}
	r.byScan[key] = e
	return nil
}
