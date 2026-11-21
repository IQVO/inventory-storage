package http

import (
	"context"
	"net/http"
	"testing"

	"github.com/claudioed/inventory-storage/internal/adapters/outbound/memory"
	"github.com/claudioed/inventory-storage/internal/domain/container"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// carry (Task 10 review): PUT /totes/{lpn} for the license plate of a sealed
// case answers 409 license-plate-taken over the real router (D1: one license
// plate, one container), and registers no tote.
func TestPutToteWithTheLicensePlateOfASealedCaseIsLicensePlateTaken(t *testing.T) {
	const lpn = "003400000000000017"
	containers := memory.NewContainerRepo()
	sealed := container.Rehydrate(container.Snapshot{
		LPN: lpn, SKU: shared.SKU("SKU-1"), UnitsPerPack: 12, Quantity: shared.Quantity(12),
		SiteID: "WH1", BinID: "WH1-RCV-DEC-D01-01-01-A", State: container.StateSealed, Version: 1,
	})
	if added, err := containers.Add(context.Background(), sealed); err != nil || !added {
		t.Fatalf("seed sealed case: added=%v err=%v", added, err)
	}
	srv := newToteTestServerWith(t, containers)
	assertProblem(t, do(t, srv, http.MethodPut, "/totes/"+lpn, ""), http.StatusConflict, "license-plate-taken")
	assertProblem(t, do(t, srv, http.MethodGet, "/totes/"+lpn, ""), http.StatusNotFound, "tote-not-found")
}
