package memory

import (
	"context"

	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/transfer"
)

// TransferReleaseRepo is an in-memory implementation of
// ports.TransferReleaseRepo, the release ledger's twin of
// TransferAllocationRepo over the shared lineLedger (ADR 0051): one
// decision per transfer_line_id, a second Save for a decided line fails
// with usecases.ErrTransferReleaseAlreadyDecided.
type TransferReleaseRepo struct {
	ledger lineLedger[*transfer.Release]
}

func NewTransferReleaseRepo() *TransferReleaseRepo {
	return &TransferReleaseRepo{ledger: newLineLedger[*transfer.Release]()}
}

func (r *TransferReleaseRepo) FindByTransferLineID(_ context.Context, transferLineID string) (*transfer.Release, error) {
	if rel, ok := r.ledger.find(transferLineID); ok {
		return rel, nil
	}
	return nil, nil
}

func (r *TransferReleaseRepo) Save(_ context.Context, rel *transfer.Release) error {
	return r.ledger.saveUnique(rel.TransferLineID(), rel, usecases.ErrTransferReleaseAlreadyDecided)
}
