package memory

import (
	"context"
	"fmt"
	"sync"

	"github.com/claudioed/inventory-storage/internal/domain/relocation"
)

// RelocationRepo is an in-memory implementation of ports.RelocationRepo.
type RelocationRepo struct {
	mu          sync.RWMutex
	relocations map[string]*relocation.Relocation
	nextID      int
}

func NewRelocationRepo() *RelocationRepo {
	return &RelocationRepo{relocations: make(map[string]*relocation.Relocation)}
}

func (r *RelocationRepo) Save(_ context.Context, rel *relocation.Relocation) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.relocations[rel.ID()] = rel
	return nil
}

func (r *RelocationRepo) FindByID(_ context.Context, id string) (*relocation.Relocation, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	rel, ok := r.relocations[id]
	if !ok {
		return nil, nil
	}
	return rel, nil
}

func (r *RelocationRepo) NextID(_ context.Context) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nextID++
	return fmt.Sprintf("rel-%d", r.nextID), nil
}
