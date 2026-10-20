// Package system holds the production adapters that talk to the host rather
// than to a database or a broker. The in-memory adapters in
// internal/adapters/outbound/memory are test doubles and no cmd/** binary may
// import them; the wall clock the binaries need lives here.
package system

import "time"

// Clock implements ports.Clock with the real wall clock.
type Clock struct{}

// Now returns the current wall-clock time.
func (Clock) Now() time.Time { return time.Now() }
