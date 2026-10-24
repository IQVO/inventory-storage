package usecases_test

import "github.com/claudioed/inventory-storage/internal/adapters/outbound/memory"

func newOwnerLedger(w *cw) *memory.BinLevelLedger { return memory.NewBinLevelLedger(w.Stock) }
