package memory

import (
	"context"
	"sync"

	"github.com/claudioed/inventory-storage/internal/application/ports"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
)

// binSku is the key of one level: a SKU in a bin.
type binSku struct {
	bin shared.BinId
	sku shared.SKU
}

// BinLevelLedger is the in-memory ports.BinLevelLedger: it keeps the
// per-(bin, sku) versions in a map and reads the levels from the in-memory
// stock repo. It has no transactions, so a rolled-back change cannot roll its
// bump back; that is acceptable for unit tests and local runs without a
// database (the Postgres adapter is the contract for production).
type BinLevelLedger struct {
	mu       sync.Mutex
	stock    *StockRepo
	versions map[binSku]int64
}

// NewBinLevelLedger builds a ledger reading levels from stock.
func NewBinLevelLedger(stock *StockRepo) *BinLevelLedger {
	return &BinLevelLedger{stock: stock, versions: make(map[binSku]int64)}
}

// Advance implements ports.BinLevelLedger.
func (l *BinLevelLedger) Advance(ctx context.Context, bin shared.BinId, sku shared.SKU) (shared.BinLevel, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	units, err := l.stock.FindByBin(ctx, bin)
	if err != nil {
		return shared.BinLevel{}, err
	}
	var level shared.BinLevel
	for _, unit := range units {
		if unit.SKU() != sku || unit.State() == stock.StateUnlocated || unit.State() == stock.StateRemoved {
			continue
		}
		level.OnHand = level.OnHand.Add(unit.Quantity())
		level.Reserved = level.Reserved.Add(unit.Reserved())
	}
	key := binSku{bin: bin, sku: sku}
	l.versions[key]++
	level.Version = l.versions[key]
	return level, nil
}

var _ ports.BinLevelLedger = (*BinLevelLedger)(nil)
