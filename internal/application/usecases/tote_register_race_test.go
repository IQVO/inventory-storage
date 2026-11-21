package usecases_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/claudioed/inventory-storage/internal/application/ports"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/container"
)

// racingTotes lets the first `racers` FindByLPN calls return only after every
// racer has read, so concurrent registrations of one new LPN all see "no tote"
// and race on Add. Later reads (the loser's re-read) pass straight through.
type racingTotes struct {
	ports.ToteRepo
	racers int32
	reads  atomic.Int32
	all    sync.WaitGroup
}

func newRacingTotes(inner ports.ToteRepo, racers int) *racingTotes {
	r := &racingTotes{ToteRepo: inner, racers: int32(racers)}
	r.all.Add(racers)
	return r
}

func (r *racingTotes) FindByLPN(ctx context.Context, lpn string) (*container.Tote, error) {
	if r.reads.Add(1) > r.racers {
		return r.ToteRepo.FindByLPN(ctx, lpn)
	}
	got, err := r.ToteRepo.FindByLPN(ctx, lpn)
	r.all.Done()
	done := make(chan struct{})
	go func() { r.all.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		return nil, errors.New("racingTotes: the other racers never read")
	}
	return got, err
}

type registerResult struct {
	tote    *container.Tote
	created bool
	err     error
}

func registerConcurrently(uc *usecases.RegisterTote, lpn string, n int) []registerResult {
	out := make([]registerResult, n)
	var wg sync.WaitGroup
	for i := range out {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			t, c, err := uc.Execute(context.Background(), lpn)
			out[i] = registerResult{t, c, err}
		}(i)
	}
	wg.Wait()
	return out
}

// carry (Task 5/10 review, ADR 0054): two concurrent PUT /totes/{lpn} of the
// same new LPN both read "no tote"; the loser of the license-plate claim
// re-reads and answers the winner's tote with created=false (200), never 409
// license-plate-taken. Exactly one ToteRegistered is raised.
func TestConcurrentRegisterToteOfOneNewLPNAnswersTheExistingTote(t *testing.T) {
	w := newTW()
	w.Register.Totes = newRacingTotes(w.Totes, 2)
	results := registerConcurrently(w.Register, "TOTE-RACE", 2)
	created := 0
	for i, r := range results {
		if r.err != nil || r.tote == nil || r.tote.LPN() != "TOTE-RACE" {
			t.Fatalf("racer %d: tote %+v created %v err %v, want the tote and no error", i, r.tote, r.created, r.err)
		}
		if r.created {
			created++
		}
	}
	if created != 1 {
		t.Fatalf("created = %d of %d racers, want exactly 1", created, len(results))
	}
	assertFacts(t, map[string]any{"events": w.eventNames()}, map[string]any{"events": []string{"ToteRegistered"}})
}

// lostToACase is a ToteRepo whose claim loses to a sealed case registered
// between the use case's checks and its Add: the re-read finds no tote.
type lostToACase struct{ ports.ToteRepo }

func (lostToACase) FindByLPN(context.Context, string) (*container.Tote, error) { return nil, nil }
func (lostToACase) Add(context.Context, *container.Tote) (bool, error)         { return false, nil }

// carry (Task 5/10 review): when the plate was lost to a sealed case (the
// re-read finds no tote) the answer stays 409 license-plate-taken, FINAL.
func TestRegisterToteLosingThePlateToASealedCaseIsLicensePlateTaken(t *testing.T) {
	w := newTW()
	w.Register.Totes = lostToACase{w.Totes}
	if _, _, err := w.Register.Execute(context.Background(), "003400000000000031"); !errors.Is(err, usecases.ErrLicensePlateTaken) {
		t.Fatalf("err = %v, want ErrLicensePlateTaken", err)
	}
	if n := len(w.Events.Events()); n != 0 {
		t.Fatalf("events = %d, want none", n)
	}
}
