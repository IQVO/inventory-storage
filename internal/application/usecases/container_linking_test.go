package usecases_test

import (
	"context"
	"errors"
	"testing"

	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/container"
)

func threeCartons() []usecases.ReceiptCarton {
	return []usecases.ReceiptCarton{carton("c1", "LPN-1"), carton("c2", "LPN-2"), carton("c3", "LPN-3")}
}

func TestReceiptOfThreeSealedCasesBooksThirtySixOnceCartonsAfterLine(t *testing.T) {
	w := newCW()
	w.declareCase(t, 12)
	w.bookLine(t, "line-1", 36)
	for _, c := range threeCartons() {
		w.link(t, c)
	}
	sum, _ := w.Containers.SumQuantity(context.Background(), "R-1", 1)
	if sum != 36 || countEvents(w.env, "ContainerRegistered") != 3 {
		t.Fatalf("containers hold %d eaches with %d registered events, want 36 and 3", sum, countEvents(w.env, "ContainerRegistered"))
	}
	if n := countEvents(w.env, "StockReceived"); n != 1 {
		t.Fatalf("StockReceived = %d, want exactly 1: cartons never book quantity", n)
	}
}

func TestReceiptOfThreeSealedCasesBooksThirtySixOnceCartonsBeforeLine(t *testing.T) {
	w := newCW()
	w.declareCase(t, 12)
	for _, c := range threeCartons() {
		w.link(t, c)
	}
	if sum, _ := w.Containers.SumQuantity(context.Background(), "R-1", 1); sum != 0 {
		t.Fatalf("containers before the line is booked hold %d, want 0", sum)
	}
	if n := countEvents(w.env, "StockReceived"); n != 0 {
		t.Fatalf("cartons alone booked stock: %d StockReceived", n)
	}
	w.bookLine(t, "line-1", 36)
	sum, _ := w.Containers.SumQuantity(context.Background(), "R-1", 1)
	if sum != 36 || countEvents(w.env, "ContainerRegistered") != 3 || countEvents(w.env, "StockReceived") != 1 {
		t.Fatalf("after the line: %d eaches, %d registered, %d received", sum, countEvents(w.env, "ContainerRegistered"), countEvents(w.env, "StockReceived"))
	}
}

func TestContainersNeverExceedTheBookedQuantity(t *testing.T) {
	w := newCW()
	w.declareCase(t, 12)
	w.bookLine(t, "line-1", 24)
	for _, c := range threeCartons() {
		w.link(t, c)
	}
	if sum, _ := w.Containers.SumQuantity(context.Background(), "R-1", 1); sum != 24 {
		t.Fatalf("containers hold %d, want 24 (booked)", sum)
	}
	if n := len(w.Pending.All()); n != 1 {
		t.Fatalf("pending links = %d, want the third carton waiting", n)
	}
	w.bookLine(t, "line-2", 12) // the line is received again for 12 more eaches
	if sum, _ := w.Containers.SumQuantity(context.Background(), "R-1", 1); sum != 36 {
		t.Fatalf("after the extra booking containers hold %d, want 36", sum)
	}
}

func TestDuplicateCartonDeliveryIsIdempotent(t *testing.T) {
	w := newCW()
	w.declareCase(t, 12)
	w.bookLine(t, "line-1", 12)
	if got := w.link(t, carton("c1", "LPN-1")); got != usecases.CartonRecorded {
		t.Fatalf("first delivery = %s", got)
	}
	if got := w.link(t, carton("c1", "LPN-1")); got != usecases.CartonDuplicate {
		t.Fatalf("same event id = %s, want DUPLICATE", got)
	}
	// Same carton under another event id: the key (receipt, line, lpn) makes it a no-op.
	w.link(t, carton("c1-again", "LPN-1"))
	if n := countEvents(w.env, "ContainerRegistered"); n != 1 {
		t.Fatalf("ContainerRegistered = %d, want 1", n)
	}
	if sum, _ := w.Containers.SumQuantity(context.Background(), "R-1", 1); sum != 12 {
		t.Fatalf("containers hold %d, want 12", sum)
	}
	if len(w.Pending.All()) != 0 {
		t.Fatalf("a redelivered carton left a pending link behind: %+v", w.Pending.All())
	}
}

func TestDuplicateLineDeliveryDoesNotDoubleTheBooking(t *testing.T) {
	w := newCW()
	w.declareCase(t, 12)
	w.bookLine(t, "line-1", 12)
	out, err := w.Book.Execute(context.Background(), usecases.InboundReceiptLine{
		EventID: "line-1", ReceiptID: "R-1", LineNo: 1, SKU: "SKU-1", Quantity: 12, Condition: "Good",
	})
	if err != nil || out != usecases.LineDuplicate {
		t.Fatalf("redelivery = %s %v, want DUPLICATE", out, err)
	}
	if booked, _ := w.Bookings.Booked(context.Background(), "R-1", 1); booked != 12 {
		t.Fatalf("booked = %d, want 12", booked)
	}
}

func TestCartonsThatGetNoContainer(t *testing.T) {
	w := newCW()
	w.declareCase(t, 12)
	w.bookLine(t, "line-1", 12)
	mixed := carton("mixed", "LPN-M")
	mixed.Contents = append(mixed.Contents, usecases.ReceiptCartonContent{LineNo: 2, SKU: "SKU-2", GTIN: caseGTIN, Quantity: 6, Condition: "Good"})
	unsealed := carton("opened", "LPN-O")
	unsealed.Sealed = false
	damaged := carton("damaged", "LPN-D")
	damaged.Contents[0].Condition = "Damaged"
	for _, c := range []usecases.ReceiptCarton{mixed, unsealed, damaged} {
		if got := w.link(t, c); got != usecases.CartonNoContainer {
			t.Errorf("%s = %s, want NO_CONTAINER", c.EventID, got)
		}
	}
	if len(w.Pending.All()) != 0 || countEvents(w.env, "ContainerRegistered") != 0 {
		t.Fatal("mixed, unsealed or damaged cartons must leave neither a link nor a container")
	}
}

func TestMalformedCartonsAreRejectedWithoutBeingClaimed(t *testing.T) {
	w := newCW()
	bad := map[string]func(*usecases.ReceiptCarton){
		"no event id":    func(c *usecases.ReceiptCarton) { c.EventID = "" },
		"no receipt id":  func(c *usecases.ReceiptCarton) { c.ReceiptID = "" },
		"no lpn":         func(c *usecases.ReceiptCarton) { c.LPN, c.SSCC = "", "" },
		"bad sku":        func(c *usecases.ReceiptCarton) { c.Contents[0].SKU = "" },
		"zero quantity":  func(c *usecases.ReceiptCarton) { c.Contents[0].Quantity = 0 },
		"bad gtin":       func(c *usecases.ReceiptCarton) { c.Contents[0].GTIN = "12" },
		"bad owner":      func(c *usecases.ReceiptCarton) { c.OwnerRef = "no good" },
		"line below one": func(c *usecases.ReceiptCarton) { c.Contents[0].LineNo = 0 },
		"unknown cond":   func(c *usecases.ReceiptCarton) { c.Contents[0].Condition = "Weird" },
	}
	for name, mutate := range bad {
		c := carton("ev", "LPN-1")
		mutate(&c)
		if _, _, err := w.Link.Execute(context.Background(), c); !errors.Is(err, usecases.ErrMalformedReceiptCarton) {
			t.Errorf("%s: err = %v, want ErrMalformedReceiptCarton", name, err)
		}
	}
}

func TestLPNFallsBackToTheSSCC(t *testing.T) {
	w := newCW()
	w.declareCase(t, 12)
	w.bookLine(t, "line-1", 12)
	c := carton("c1", "")
	c.SSCC = "003400000000000017"
	w.link(t, c)
	w.containerOf(t, "003400000000000017")
}

func TestCartonWaitsForItsPackLevelThenRegisters(t *testing.T) {
	w := newCW()
	w.bookLine(t, "line-1", 12)
	w.link(t, carton("c1", "LPN-1"))
	if c, _ := w.Containers.FindByLPN(context.Background(), "LPN-1"); c != nil {
		t.Fatal("a carton whose GTIN is unknown must stay pending, never be guessed")
	}
	w.declareCase(t, 12)
	c := w.containerOf(t, "LPN-1")
	if c.GTIN() != caseGTIN || c.UnitsPerPack() != 12 || c.OwnerRef() != "seller-a" || c.Lot() != "L1" || c.Expiry() != "2027-03-01" {
		t.Fatalf("registered container is wrong: %+v", c)
	}
}

func TestEachGTINOnTheCartonResolvesTheActiveCaseLevel(t *testing.T) {
	w := newCW()
	w.declareCase(t, 12)
	if _, err := w.Pack.Declared(context.Background(), usecases.PackLevelDeclared{
		EventID: "pm-each", SKU: "SKU-1", Level: "EACH", GTIN: eachGTIN, UnitsPerPack: 1, EffectiveFrom: w.Clock.Now().Add(-48 * 3600e9), Version: 1,
	}); err != nil {
		t.Fatal(err)
	}
	w.bookLine(t, "line-1", 12)
	c := carton("c1", "LPN-1")
	c.Contents[0].GTIN = eachGTIN
	w.link(t, c)
	if got := w.containerOf(t, "LPN-1"); got.GTIN() != caseGTIN || got.UnitsPerPack() != 12 {
		t.Fatalf("container gtin %s units %d, want the case level", got.GTIN(), got.UnitsPerPack())
	}
}

func rejected(w *cw) []container.PendingLink {
	var out []container.PendingLink
	for _, l := range w.Pending.All() {
		if l.Status == container.LinkRejected {
			out = append(out, l)
		}
	}
	return out
}

func TestCartonThatIsNotOneCaseOfTheDeclaredSizeIsRejected(t *testing.T) {
	w := newCW()
	w.declareCase(t, 12)
	w.bookLine(t, "line-1", 10)
	c := carton("c1", "LPN-1")
	c.Contents[0].Quantity = 10
	w.link(t, c)
	r := rejected(w)
	if len(r) != 1 || r[0].Reason != "QUANTITY_MISMATCH" {
		t.Fatalf("rejected = %+v, want one QUANTITY_MISMATCH", r)
	}
	if c, _ := w.Containers.FindByLPN(context.Background(), "LPN-1"); c != nil {
		t.Fatal("a mismatching carton became a container")
	}
}

func TestCartonWithAGTINOfAnotherSKUOrLevelIsRejected(t *testing.T) {
	w := newCW()
	w.declareCase(t, 12)
	w.bookLine(t, "line-1", 24)
	other := carton("c1", "LPN-1")
	other.Contents[0].SKU = "SKU-OTHER"
	w.link(t, other)
	if _, err := w.Pack.Declared(context.Background(), usecases.PackLevelDeclared{
		EventID: "pm-pallet", SKU: "SKU-1", Level: "PALLET", GTIN: "40012345000018", UnitsPerPack: 480, EffectiveFrom: w.Clock.Now().Add(-3600e9), Version: 5,
	}); err != nil {
		t.Fatal(err)
	}
	pallet := carton("c2", "LPN-2")
	pallet.Contents[0].GTIN = "40012345000018"
	w.link(t, pallet)
	reasons := map[string]bool{}
	for _, r := range rejected(w) {
		reasons[r.Reason] = true
	}
	if !reasons["GTIN_BELONGS_TO_OTHER_SKU"] || !reasons["GTIN_IS_NOT_A_CASE"] {
		t.Fatalf("rejection reasons = %v", reasons)
	}
}

func TestDuplicateSSCCAtAnotherReceiptIsRejectedNotMerged(t *testing.T) {
	w := newCW()
	w.registered(t, "LPN-1")
	again := carton("c-other", "LPN-1")
	again.ReceiptID = "R-2"
	w.bookLineAt(t, "R-2")
	w.link(t, again)
	r := rejected(w)
	if len(r) != 1 || r[0].ReceiptID != "R-2" || r[0].Reason != "LPN_REGISTERED_AT_ANOTHER_RECEIPT_LINE" {
		t.Fatalf("rejected = %+v", r)
	}
	if got := w.containerOf(t, "LPN-1"); got.ReceiptID() != "R-1" {
		t.Fatalf("the original container was merged into receipt %s", got.ReceiptID())
	}
}

func (w *cw) bookLineAt(t *testing.T, receipt string) {
	t.Helper()
	if _, err := w.Book.Execute(context.Background(), usecases.InboundReceiptLine{
		EventID: "line-" + receipt, ReceiptID: receipt, LineNo: 1, SKU: "SKU-1", Quantity: 12, Condition: "Good",
	}); err != nil {
		t.Fatal(err)
	}
}

func TestDamagedAndFlatLinesRecordNoBooking(t *testing.T) {
	w := newCW()
	if out, err := w.Book.Execute(context.Background(), usecases.InboundReceiptLine{
		EventID: "d", ReceiptID: "R-1", LineNo: 1, SKU: "SKU-1", Quantity: 5, Condition: "Damaged",
	}); err != nil || out != usecases.LineDamagedNotBooked {
		t.Fatalf("damaged = %s %v", out, err)
	}
	if out, err := w.Book.Execute(context.Background(), usecases.InboundReceiptLine{
		EventID: "flat", SKU: "SKU-1", Quantity: 5, Condition: "Good",
	}); err != nil || out != usecases.LineBooked {
		t.Fatalf("flat line without receipt id = %s %v", out, err)
	}
	if booked, _ := w.Bookings.Booked(context.Background(), "R-1", 1); booked != 0 {
		t.Fatalf("booked = %d, want 0", booked)
	}
}

func TestLineProvenanceFieldsNeverChangeTheBookedQuantity(t *testing.T) {
	w := newCW()
	out, err := w.Book.Execute(context.Background(), usecases.InboundReceiptLine{
		EventID: "l", ReceiptID: "R-1", LineNo: 1, SKU: "SKU-1", Quantity: 36, Condition: "Good",
		OwnerRef: "seller-a", PackGTIN: caseGTIN, Cases: 3, SSCC: "003400000000000017",
	})
	if err != nil || out != usecases.LineBooked {
		t.Fatalf("%s %v", out, err)
	}
	if booked, _ := w.Bookings.Booked(context.Background(), "R-1", 1); booked != 36 {
		t.Fatalf("booked = %d, want 36 eaches (cases are provenance only)", booked)
	}
	if _, err := w.Book.Execute(context.Background(), usecases.InboundReceiptLine{
		EventID: "l2", SKU: "SKU-1", Quantity: 1, Condition: "Good", OwnerRef: "no good",
	}); !errors.Is(err, usecases.ErrMalformedInboundReceiptLine) {
		t.Fatalf("invalid owner = %v, want malformed", err)
	}
}
