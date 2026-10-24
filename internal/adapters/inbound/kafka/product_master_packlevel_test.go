package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/inventory-storage/internal/application/usecases"
)

type packRecorder struct {
	declared []usecases.PackLevelDeclared
	retired  []usecases.PackLevelRetired
	err      error
}

func (r *packRecorder) Declared(_ context.Context, d usecases.PackLevelDeclared) (usecases.PackLevelOutcome, error) {
	r.declared = append(r.declared, d)
	return usecases.PackLevelApplied, r.err
}

func (r *packRecorder) Retired(_ context.Context, v usecases.PackLevelRetired) (usecases.PackLevelOutcome, error) {
	r.retired = append(r.retired, v)
	return usecases.PackLevelApplied, r.err
}

const (
	typeDeclaredForTest = "com.warehouse.wms.product-master.product.ProductPackLevelDeclared"
	typeRetiredForTest  = "com.warehouse.wms.product-master.product.ProductPackLevelRetired"
)

func packConsumer(rec *packRecorder) *ProductMasterConsumer {
	return &ProductMasterConsumer{Packaging: rec, Logger: quietLogger()}
}

func TestProductMasterConsumer_PackLevelDeclaredIsProjected(t *testing.T) {
	rec := &packRecorder{}
	payload := map[string]any{
		"sku": "SKU-1", "level": "CASE", "gtin": "24006381333935", "child_gtin": "04006381333931", "qty_of_child": 12, "units_per_pack": 12,
		"dims":         map[string]any{"length_mm": 400, "width_mm": 300, "height_mm": 200, "weight_g": 6500},
		"is_orderable": true, "is_despatch_unit": true, "effective_from": "2026-10-06T20:00:00Z", "version": 3,
	}
	msg := kafkaMsg(productMasterCE(t, "pm-1", typeDeclaredForTest, payload))
	if err := packConsumer(rec).Handle(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if len(rec.declared) != 1 {
		t.Fatalf("declared = %d", len(rec.declared))
	}
	d := rec.declared[0]
	if d.EventID != "pm-1" || d.SKU != "SKU-1" || d.Level != "CASE" || d.GTIN != "24006381333935" || d.ChildGTIN != "04006381333931" ||
		d.UnitsPerPack != 12 || d.Version != 3 || !d.EffectiveFrom.Equal(time.Date(2026, 10, 6, 20, 0, 0, 0, time.UTC)) {
		t.Fatalf("declared = %+v", d)
	}
}

func TestProductMasterConsumer_PackLevelRetiredIsProjected(t *testing.T) {
	rec := &packRecorder{}
	payload := map[string]any{"sku": "SKU-1", "level": "CASE", "gtin": "24006381333935", "effective_to": "2026-10-06T21:01:00Z", "reason": "superseded", "version": 4}
	if err := packConsumer(rec).Handle(context.Background(), kafkaMsg(productMasterCE(t, "pm-2", typeRetiredForTest, payload))); err != nil {
		t.Fatal(err)
	}
	if len(rec.retired) != 1 || rec.retired[0].GTIN != "24006381333935" || rec.retired[0].Version != 4 ||
		!rec.retired[0].EffectiveTo.Equal(time.Date(2026, 10, 6, 21, 1, 0, 0, time.UTC)) {
		t.Fatalf("retired = %+v", rec.retired)
	}
}

func TestProductMasterConsumer_PackLevelErrors(t *testing.T) {
	declared := kafkaMsg(productMasterCE(t, "pm-1", typeDeclaredForTest, map[string]any{"sku": "SKU-1", "version": 3}))
	retired := kafkaMsg(productMasterCE(t, "pm-2", typeRetiredForTest, map[string]any{"sku": "SKU-1", "version": 3}))
	for _, msg := range []kafkaMessage{declared, retired} {
		if err := packConsumer(&packRecorder{err: errors.New("db is down")}).Handle(context.Background(), msg); err == nil {
			t.Error("a transient failure must be returned for retry")
		}
		if err := packConsumer(&packRecorder{err: usecases.ErrMalformedPackLevel}).Handle(context.Background(), msg); err != nil {
			t.Errorf("a malformed pack level must be skipped, got %v", err)
		}
	}
	bad := kafkaMsg(productMasterCE(t, "pm-3", typeDeclaredForTest, map[string]any{"sku": "SKU-1", "units_per_pack": "twelve"}))
	badRetired := kafkaMsg(productMasterCE(t, "pm-4", typeRetiredForTest, map[string]any{"version": "x"}))
	rec := &packRecorder{}
	for _, msg := range []kafkaMessage{bad, badRetired} {
		if err := packConsumer(rec).Handle(context.Background(), msg); err != nil {
			t.Errorf("an undecodable payload must be skipped, got %v", err)
		}
	}
	if len(rec.declared)+len(rec.retired) != 0 {
		t.Fatal("an undecodable payload reached the use case")
	}
}

func TestProductMasterConsumer_PackLevelEventsAreIgnoredWithoutAProjection(t *testing.T) {
	msg := kafkaMsg(productMasterCE(t, "pm-1", typeDeclaredForTest, map[string]any{"sku": "SKU-1"}))
	c := &ProductMasterConsumer{Logger: quietLogger()}
	if err := c.Handle(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	msg = kafkaMsg(productMasterCE(t, "pm-2", typeRetiredForTest, map[string]any{"sku": "SKU-1"}))
	if err := c.Handle(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
}
