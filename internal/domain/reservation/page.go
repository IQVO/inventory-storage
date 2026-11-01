package reservation

import "time"

// PageAfter is the keyset position a page of one demandRef's reservations
// starts after: the (created_at, id) of the last reservation the previous page
// returned. Reservations are listed in ascending (created_at, id) order, so
// two reservations created in the same instant are never skipped or repeated
// (REST API guideline section 3.4).
type PageAfter struct {
	CreatedAt time.Time
	ID        string
}

// Admits reports whether a reservation with the given createdAt and id sorts
// strictly after p, i.e. belongs to a page that follows it.
func (p PageAfter) Admits(createdAt time.Time, id string) bool {
	if c := createdAt.Compare(p.CreatedAt); c != 0 {
		return c > 0
	}
	return id > p.ID
}
