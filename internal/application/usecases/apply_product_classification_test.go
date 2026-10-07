package usecases_test

import (
	"context"
	"errors"
	"testing"

	"github.com/claudioed/inventory-storage/internal/adapters/outbound/memory"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/product"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// failingLocalCopy fails ApplyIfNewer, to prove a transient repo error
// surfaces unchanged (so the consumer retries).
type failingLocalCopy struct{}

func (failingLocalCopy) ApplyIfNewer(context.Context, *product.ProductClassification, int64, string) (bool, error) {
	return false, errFake
}

// failingClaims fails Claim.
type failingClaims struct{}

func (failingClaims) Claim(context.Context, string, string) (bool, error) { return false, errFake }

// rollbackUnitOfWork runs fn and, when it fails, restores the repos the
// test hands it — a stand-in for the Postgres transaction rollback, so the
// test can prove the claim does not survive a failed upsert.
type rollbackUnitOfWork struct{ onRollback func() }

func (u rollbackUnitOfWork) Execute(ctx context.Context, fn func(ctx context.Context) error) error {
	if err := fn(ctx); err != nil {
		u.onRollback()
		return err
	}
	return nil
}

func validUpdate(id string, version int64) usecases.ProductClassificationUpdate {
	return usecases.ProductClassificationUpdate{
		EventID:              id,
		SKU:                  "SKU-1",
		HandlingTags:         []string{"Hazmat", "TemperatureSensitive"},
		TemperatureClass:     "Frozen",
		DOTHazardClass:       3,
		ClassificationSource: "native",
		Version:              version,
	}
}

func newApply(e env) (*usecases.ApplyProductClassification, *memory.ProcessedEventRepo) {
	processed := memory.NewProcessedEventRepo()
	return &usecases.ApplyProductClassification{Classifications: e.Classifications, ProcessedEvents: processed}, processed
}

func TestApplyProductClassification_InsertsWhenAbsent(t *testing.T) {
	e := newEnv()
	uc, _ := newApply(e)

	outcome, err := uc.Execute(context.Background(), validUpdate("ce-1", 3))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if outcome != usecases.ClassificationApplied {
		t.Fatalf("outcome = %s, want APPLIED", outcome)
	}
	got, _ := e.Classifications.FindBySKU(context.Background(), mustSKU(t, "SKU-1"))
	if got == nil || !got.IsHazmat() || got.TemperatureClass() != product.Frozen || got.DOTHazardClass() != 3 {
		t.Fatalf("local copy = %+v", got)
	}
	version, source, ok := e.Classifications.VersionOf(mustSKU(t, "SKU-1"))
	if !ok || version != 3 || source != "native" {
		t.Fatalf("stored version/source = %d/%q/%t, want 3/native/true", version, source, ok)
	}
}

// A legacy row (version 0) is replaced by any product-master version.
func TestApplyProductClassification_ReplacesLegacyRow(t *testing.T) {
	e := newEnv()
	legacy, _ := product.New(mustSKU(t, "SKU-1"), []product.HandlingTag{product.Fragile}, "", 0)
	if err := e.Classifications.Save(context.Background(), legacy); err != nil {
		t.Fatal(err)
	}
	uc, _ := newApply(e)

	outcome, err := uc.Execute(context.Background(), validUpdate("ce-1", 1))
	if err != nil || outcome != usecases.ClassificationApplied {
		t.Fatalf("outcome=%s err=%v, want APPLIED", outcome, err)
	}
	got, _ := e.Classifications.FindBySKU(context.Background(), mustSKU(t, "SKU-1"))
	if got.HasTag(product.Fragile) || !got.IsHazmat() {
		t.Fatalf("legacy row not replaced: %v", got.HandlingTags())
	}
}

func TestApplyProductClassification_StaleAndEqualVersionsAreIgnored(t *testing.T) {
	e := newEnv()
	uc, _ := newApply(e)
	if _, err := uc.Execute(context.Background(), validUpdate("ce-1", 5)); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		id      string
		version int64
	}{{"ce-2", 4}, {"ce-3", 5}} {
		older := validUpdate(tc.id, tc.version)
		older.HandlingTags = []string{"Fragile"}
		older.TemperatureClass = ""
		older.DOTHazardClass = 0
		outcome, err := uc.Execute(context.Background(), older)
		if err != nil {
			t.Fatalf("version %d: unexpected error %v", tc.version, err)
		}
		if outcome != usecases.ClassificationStale {
			t.Fatalf("version %d: outcome = %s, want STALE", tc.version, outcome)
		}
	}
	got, _ := e.Classifications.FindBySKU(context.Background(), mustSKU(t, "SKU-1"))
	if !got.IsHazmat() || got.HasTag(product.Fragile) {
		t.Fatalf("stale update overwrote the copy: %v", got.HandlingTags())
	}
	if version, _, _ := e.Classifications.VersionOf(mustSKU(t, "SKU-1")); version != 5 {
		t.Fatalf("version = %d, want 5", version)
	}
}

func TestApplyProductClassification_NewerVersionReplaces(t *testing.T) {
	e := newEnv()
	uc, _ := newApply(e)
	if _, err := uc.Execute(context.Background(), validUpdate("ce-1", 1)); err != nil {
		t.Fatal(err)
	}
	newer := validUpdate("ce-2", 2)
	newer.HandlingTags = []string{"Fragile"}
	newer.TemperatureClass = ""
	newer.DOTHazardClass = 0
	newer.ClassificationSource = "legacy-import"
	outcome, err := uc.Execute(context.Background(), newer)
	if err != nil || outcome != usecases.ClassificationApplied {
		t.Fatalf("outcome=%s err=%v, want APPLIED", outcome, err)
	}
	got, _ := e.Classifications.FindBySKU(context.Background(), mustSKU(t, "SKU-1"))
	if got.IsHazmat() || !got.HasTag(product.Fragile) || got.DOTHazardClass() != product.DOTHazardClassUnspecified {
		t.Fatalf("copy = %v dot=%d", got.HandlingTags(), got.DOTHazardClass())
	}
	if _, source, _ := e.Classifications.VersionOf(mustSKU(t, "SKU-1")); source != "legacy-import" {
		t.Fatalf("source = %q, want legacy-import", source)
	}
}

// A redelivery of the same CloudEvents id is a no-op, even if its payload
// would otherwise be newer.
func TestApplyProductClassification_DuplicateEventIDIsANoOp(t *testing.T) {
	e := newEnv()
	uc, _ := newApply(e)
	if _, err := uc.Execute(context.Background(), validUpdate("ce-1", 1)); err != nil {
		t.Fatal(err)
	}
	replay := validUpdate("ce-1", 9)
	replay.HandlingTags = []string{"Fragile"}
	replay.TemperatureClass = ""
	replay.DOTHazardClass = 0
	outcome, err := uc.Execute(context.Background(), replay)
	if err != nil || outcome != usecases.ClassificationDuplicate {
		t.Fatalf("outcome=%s err=%v, want DUPLICATE", outcome, err)
	}
	if version, _, _ := e.Classifications.VersionOf(mustSKU(t, "SKU-1")); version != 1 {
		t.Fatalf("version = %d, want 1 (the duplicate must not apply)", version)
	}
}

// The use case raises no domain event: product-master -> inventory-storage
// is one-way, nothing must ever reach the outbox (ADR 0034).
func TestApplyProductClassification_RaisesNoDomainEvent(t *testing.T) {
	e := newEnv()
	uc, _ := newApply(e)
	if _, err := uc.Execute(context.Background(), validUpdate("ce-1", 1)); err != nil {
		t.Fatal(err)
	}
	if n := len(e.Events.Events()); n != 0 {
		t.Fatalf("published %d events, want 0", n)
	}
}

func TestApplyProductClassification_MalformedUpdatesAreDeterministic(t *testing.T) {
	cases := map[string]func(u *usecases.ProductClassificationUpdate){
		"empty event id":   func(u *usecases.ProductClassificationUpdate) { u.EventID = "" },
		"zero version":     func(u *usecases.ProductClassificationUpdate) { u.Version = 0 },
		"negative version": func(u *usecases.ProductClassificationUpdate) { u.Version = -1 },
		"empty sku":        func(u *usecases.ProductClassificationUpdate) { u.SKU = "" },
		"unknown tag":      func(u *usecases.ProductClassificationUpdate) { u.HandlingTags = []string{"Radioactive"} },
		"no tags": func(u *usecases.ProductClassificationUpdate) {
			u.HandlingTags = nil
			u.TemperatureClass = ""
			u.DOTHazardClass = 0
		},
		"unknown temperature": func(u *usecases.ProductClassificationUpdate) { u.TemperatureClass = "Lukewarm" },
		"missing temperature": func(u *usecases.ProductClassificationUpdate) { u.TemperatureClass = "" },
		"dot class too high":  func(u *usecases.ProductClassificationUpdate) { u.DOTHazardClass = 10 },
		"dot class without hz": func(u *usecases.ProductClassificationUpdate) {
			u.HandlingTags = []string{"Fragile"}
			u.TemperatureClass = ""
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			e := newEnv()
			uc, processed := newApply(e)
			u := validUpdate("ce-1", 1)
			mutate(&u)
			_, err := uc.Execute(context.Background(), u)
			if !errors.Is(err, usecases.ErrMalformedProductClassification) {
				t.Fatalf("err = %v, want ErrMalformedProductClassification", err)
			}
			// Nothing claimed: a valid redelivery of the same id would
			// still apply.
			if claimed, _ := processed.Claim(context.Background(), usecases.ProductMasterClassificationConsumer, "ce-1"); !claimed {
				t.Fatal("a malformed update must not claim its event id")
			}
			if got, _ := e.Classifications.FindBySKU(context.Background(), shared.SKU("SKU-1")); got != nil {
				t.Fatalf("malformed update wrote the copy: %+v", got)
			}
		})
	}
}

// resettableClaims is a ProcessedEventRepo whose claims a fake rollback can
// discard.
type resettableClaims struct{ seen map[string]bool }

func (c *resettableClaims) Claim(_ context.Context, consumer, id string) (bool, error) {
	if c.seen == nil {
		c.seen = map[string]bool{}
	}
	key := consumer + "/" + id
	if c.seen[key] {
		return false, nil
	}
	c.seen[key] = true
	return true, nil
}

func (c *resettableClaims) reset() { c.seen = nil }

func TestApplyProductClassification_TransientUpsertFailureRollsBackTheClaim(t *testing.T) {
	processed := &resettableClaims{}
	uc := &usecases.ApplyProductClassification{
		Classifications: failingLocalCopy{},
		ProcessedEvents: processed,
		UnitOfWork:      rollbackUnitOfWork{onRollback: processed.reset},
	}
	if _, err := uc.Execute(context.Background(), validUpdate("ce-1", 1)); !errors.Is(err, errFake) {
		t.Fatalf("err = %v, want the repo's transient error", err)
	}
	if claimed, _ := processed.Claim(context.Background(), usecases.ProductMasterClassificationConsumer, "ce-1"); !claimed {
		t.Fatal("the claim survived a failed upsert: a retry would be treated as a duplicate")
	}
}

func TestApplyProductClassification_ClaimFailurePropagates(t *testing.T) {
	e := newEnv()
	uc := &usecases.ApplyProductClassification{Classifications: e.Classifications, ProcessedEvents: failingClaims{}}
	if _, err := uc.Execute(context.Background(), validUpdate("ce-1", 1)); !errors.Is(err, errFake) {
		t.Fatalf("err = %v, want errFake", err)
	}
	if got, _ := e.Classifications.FindBySKU(context.Background(), mustSKU(t, "SKU-1")); got != nil {
		t.Fatal("upsert ran although the claim failed")
	}
}
