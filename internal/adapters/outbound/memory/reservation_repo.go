package memory

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/claudioed/inventory-storage/internal/domain/reservation"
)

// ReservationRepo is an in-memory implementation of ports.ReservationRepo.
type ReservationRepo struct {
	mu           sync.RWMutex
	reservations map[string]*reservation.Reservation
	nextID       int
}

func NewReservationRepo() *ReservationRepo {
	return &ReservationRepo{reservations: make(map[string]*reservation.Reservation)}
}

func (r *ReservationRepo) Save(_ context.Context, res *reservation.Reservation) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reservations[res.ID()] = res
	return nil
}

func (r *ReservationRepo) FindByID(_ context.Context, id string) (*reservation.Reservation, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	res, ok := r.reservations[id]
	if !ok {
		return nil, nil
	}
	return res, nil
}

// FindByDemandRef performs a simple linear scan over every reservation held,
// filtering by demand ref, in ascending (created_at, id) order like the
// Postgres adapter. Suitable for tests/local use; the Postgres adapter uses an
// indexed query instead.
func (r *ReservationRepo) FindByDemandRef(_ context.Context, demandRef string) ([]*reservation.Reservation, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.sortedByDemandRef(demandRef, nil), nil
}

// FindByDemandRefAfter is the keyset-paged FindByDemandRef: at most limit
// reservations strictly after the (created_at, id) position (nil = from the
// start), in ascending (created_at, id) order.
func (r *ReservationRepo) FindByDemandRefAfter(_ context.Context, demandRef string, after *reservation.PageAfter, limit int) ([]*reservation.Reservation, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	results := r.sortedByDemandRef(demandRef, after)
	if limit >= 0 && len(results) > limit {
		results = results[:limit]
	}
	return results, nil
}

// sortedByDemandRef returns the reservations of demandRef that sort after the
// position (all of them when after is nil) in (created_at, id) order. The
// caller holds r.mu.
func (r *ReservationRepo) sortedByDemandRef(demandRef string, after *reservation.PageAfter) []*reservation.Reservation {
	var results []*reservation.Reservation
	for _, res := range r.reservations {
		if res.DemandRef() != demandRef {
			continue
		}
		if after != nil && !after.Admits(res.CreatedAt(), res.ID()) {
			continue
		}
		results = append(results, res)
	}
	sort.Slice(results, func(i, j int) bool {
		if c := results[i].CreatedAt().Compare(results[j].CreatedAt()); c != 0 {
			return c < 0
		}
		return results[i].ID() < results[j].ID()
	})
	return results
}

func (r *ReservationRepo) NextID(_ context.Context) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nextID++
	return fmt.Sprintf("res-%d", r.nextID), nil
}
