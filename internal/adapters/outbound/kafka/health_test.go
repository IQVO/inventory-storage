package kafka

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

type fakeLister struct {
	brokers []kafkago.Broker
	err     error
}

func (f fakeLister) Brokers(context.Context) ([]kafkago.Broker, error) { return f.brokers, f.err }

func TestBrokerCheck(t *testing.T) {
	one := []kafkago.Broker{{Host: "k", Port: 9092}}
	tests := []struct {
		name    string
		lister  fakeLister
		wantErr bool
	}{
		{"a broker answers", fakeLister{brokers: one}, false},
		{"lister fails", fakeLister{err: errors.New("down")}, true},
		{"empty broker list", fakeLister{}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := NewBrokerCheckWithLister(tc.lister).Check(context.Background())
			if (err != nil) != tc.wantErr {
				t.Fatalf("Check() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

func TestDialListerNoAddress(t *testing.T) {
	if err := NewBrokerCheck(nil).Check(context.Background()); err == nil {
		t.Fatal("a check over no broker address must fail")
	}
}

func TestDialListerUnreachableBroker(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close() // nothing listens on addr any more
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := NewBrokerCheck([]string{addr}).Check(ctx); err == nil {
		t.Fatal("a check over a closed port must fail")
	}
}
