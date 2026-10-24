package http

import (
	"encoding/json"
	"testing"
)

func TestCasesFieldRejectsNullAndNonInteger(t *testing.T) {
	var req reserveStockRequest
	if err := json.Unmarshal([]byte(`{"sku":"S","cases":null}`), &req); err == nil {
		t.Fatal("cases:null must be a malformed body")
	}
	if err := json.Unmarshal([]byte(`{"sku":"S","cases":"2"}`), &req); err == nil {
		t.Fatal("cases:\"2\" must be a malformed body")
	}
	var ok reserveStockRequest
	if err := json.Unmarshal([]byte(`{"sku":"S","cases":3}`), &ok); err != nil || !ok.Cases.set || ok.Cases.n != 3 {
		t.Fatalf("cases:3 = %+v, %v", ok.Cases, err)
	}
	var absent reserveStockRequest
	if err := json.Unmarshal([]byte(`{"sku":"S","quantity":2}`), &absent); err != nil || absent.Cases.set {
		t.Fatalf("absent cases must stay unset: %+v, %v", absent.Cases, err)
	}
}
