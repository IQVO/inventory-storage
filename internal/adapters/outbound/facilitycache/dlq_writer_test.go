package facilitycache

import "testing"

// TestNewDLQWriter_AutoCreatesTopic pins the dead-letter writer config: a
// missing "<topic>.dlq" must be auto-created on first publish (fleet
// convention), not fail and stop the facility cache.
func TestNewDLQWriter_AutoCreatesTopic(t *testing.T) {
	w := newDLQWriter([]string{"localhost:9092"}, "warehouse.facility.events")
	t.Cleanup(func() { _ = w.Close() })
	if w.Topic != "warehouse.facility.events.dlq" {
		t.Fatalf("DLQ topic = %q", w.Topic)
	}
	if !w.AllowAutoTopicCreation {
		t.Fatal("DLQ writer must set AllowAutoTopicCreation")
	}
}
