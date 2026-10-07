package memory

import (
	"context"
	"sync"
	"time"
)

// OrderPickProgressRepo is an in-memory ports.OrderPickProgressRepo.
type OrderPickProgressRepo struct {
	mu     sync.Mutex
	counts map[string]int
}

func NewOrderPickProgressRepo() *OrderPickProgressRepo {
	return &OrderPickProgressRepo{counts: make(map[string]int)}
}

// RecordPick adds one pick for demandRef and returns the new count.
func (r *OrderPickProgressRepo) RecordPick(_ context.Context, demandRef string, _ time.Time) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.counts[demandRef]++
	return r.counts[demandRef], nil
}

// Count returns the recorded picks for demandRef (test helper).
func (r *OrderPickProgressRepo) Count(demandRef string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.counts[demandRef]
}
