//go:build integration

package main

import (
	"context"
	"testing"

	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
)

// carry (Task 10 review, optional): the production repo set reads a tote's
// units through the SAME stock repo value the stock writes go to (ADR 0054;
// adapters.go: "containerStock is the same stock repo value as stock").
// The memory-backed wiring tests cannot see buildRepos, so this pins it.
func TestBuildRepos_ContainerStockIsTheStockRepo(t *testing.T) {
	ctx := context.Background()
	pg, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("inventory"),
		tcpostgres.WithUsername("inventory"),
		tcpostgres.WithPassword("inventory"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(pg) })
	url, err := pg.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}

	set, err := buildRepos(ctx, url, url, migrationsDirForTest(t), quietLogger())
	if err != nil {
		t.Fatalf("buildRepos: %v", err)
	}
	t.Cleanup(set.close)

	if set.stock == nil || set.containerStock == nil || set.totes == nil {
		t.Fatalf("stock=%v containerStock=%v totes=%v: every tote repo must be wired", set.stock, set.containerStock, set.totes)
	}
	if any(set.containerStock) != any(set.stock) {
		t.Fatalf("containerStock (%p) must be the stock repo value (%p)", set.containerStock, set.stock)
	}
}
