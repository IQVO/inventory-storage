package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// TestPublisherBrokers pins the single KAFKA_BROKERS default (gap fix D22):
// the publishers and the outbox relay fall back to localhost:9092 when the
// variable is unset, while the consumers keep the raw (nil) list so their
// "requires KAFKA_BROKERS" boot checks still fire.
func TestPublisherBrokers(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", "")
	unset := loadConfig()
	if unset.kafkaBrokers != nil {
		t.Fatalf("consumer broker list = %v, want nil when KAFKA_BROKERS is unset", unset.kafkaBrokers)
	}
	if got := unset.publisherBrokers(); !slices.Equal(got, []string{"localhost:9092"}) {
		t.Fatalf("publisherBrokers() unset = %v, want [localhost:9092]", got)
	}

	t.Setenv("KAFKA_BROKERS", "a:9092,b:9092")
	set := loadConfig()
	want := []string{"a:9092", "b:9092"}
	if !slices.Equal(set.kafkaBrokers, want) || !slices.Equal(set.publisherBrokers(), want) {
		t.Fatalf("kafkaBrokers = %v, publisherBrokers() = %v, want both %v", set.kafkaBrokers, set.publisherBrokers(), want)
	}
}

// kafkaBrokersReads returns file:line of every getenv/os.Getenv call whose
// key is the literal "KAFKA_BROKERS" in one Go file.
func kafkaBrokersReads(t *testing.T, path string) []string {
	t.Helper()
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	var out []string
	ast.Inspect(f, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok || len(call.Args) == 0 {
			return true
		}
		lit, ok := call.Args[0].(*ast.BasicLit)
		if !ok || lit.Kind != token.STRING {
			return true
		}
		if key, _ := strconv.Unquote(lit.Value); key == "KAFKA_BROKERS" {
			out = append(out, fset.Position(call.Pos()).String())
		}
		return true
	})
	return out
}

// TestKafkaBrokersReadOnce guards against the drift D22 fixed: config.go and
// adapters.go each read KAFKA_BROKERS with different defaults. cmd/inventory
// must read it exactly once, in config.go.
func TestKafkaBrokersReadOnce(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	var reads []string
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		reads = append(reads, kafkaBrokersReads(t, filepath.Join(".", name))...)
	}
	if len(reads) != 1 || !strings.HasPrefix(reads[0], "config.go:") {
		t.Fatalf("KAFKA_BROKERS reads = %v, want exactly one, in config.go", reads)
	}
}
