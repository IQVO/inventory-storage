package main

import (
	"strings"
	"testing"
)

func TestCombineConsumerHandles_DrainsEveryHandle(t *testing.T) {
	var calls []string
	mk := func(name string) transferConsumerHandle {
		return transferConsumerHandle{
			stop:  func() { calls = append(calls, name+".stop") },
			wait:  func() { calls = append(calls, name+".wait") },
			close: func() { calls = append(calls, name+".close") },
		}
	}
	h := combineConsumerHandles(mk("a"), mk("b"))
	h.stop()
	h.wait()
	h.close()
	want := "a.stop b.stop a.wait b.wait a.close b.close"
	if got := strings.Join(calls, " "); got != want {
		t.Fatalf("calls = %q, want %q", got, want)
	}
}
