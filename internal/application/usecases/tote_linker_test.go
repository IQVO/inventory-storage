package usecases_test

import (
	"context"
	"testing"
)

func TestACartonNeverTakesATotesLicensePlate(t *testing.T) {
	w := newTW()
	if _, _, err := w.Register.Execute(context.Background(), "003400000000000099"); err != nil {
		t.Fatal(err)
	}
	w.declareCase(t, 12)
	w.bookLine(t, "ev-line", 12)
	w.link(t, carton("ev-carton", "003400000000000099"))
	if c, _ := w.Containers.FindByLPN(context.Background(), "003400000000000099"); c != nil {
		t.Fatal("a carton was registered under a tote's LPN")
	}
	if countEvents(w.env, "ContainerRegistered") != 0 {
		t.Fatal("ContainerRegistered raised for a tote's LPN")
	}
	if r := rejected(w.cw); len(r) != 1 || r[0].Reason != "LPN_REGISTERED_AT_ANOTHER_RECEIPT_LINE" {
		t.Fatalf("rejected = %+v, want one LPN_REGISTERED_AT_ANOTHER_RECEIPT_LINE", r)
	}
}
