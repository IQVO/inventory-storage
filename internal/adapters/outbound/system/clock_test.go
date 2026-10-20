package system

import (
	"testing"
	"time"
)

func TestClockNowIsTheWallClock(t *testing.T) {
	before := time.Now()
	got := Clock{}.Now()
	after := time.Now()
	if got.Before(before) || got.After(after) {
		t.Fatalf("Now() = %v, want between %v and %v", got, before, after)
	}
}
