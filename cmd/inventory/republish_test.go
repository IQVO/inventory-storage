package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestRunCommand_UnknownCommandExits2(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := runCommand(context.Background(), []string{"frobnicate"}, &stdout, &stderr); code != 2 {
		t.Fatalf("exit code = %d, want 2", code)
	}
	if !strings.Contains(stderr.String(), republishCommand) {
		t.Fatalf("stderr must list the supported command, got %q", stderr.String())
	}
}

func TestRunCommand_RepublishWithoutDatabaseURLFails(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	var stdout, stderr bytes.Buffer
	if code := runCommand(context.Background(), []string{republishCommand}, &stdout, &stderr); code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "DATABASE_URL is required") {
		t.Fatalf("stderr = %q", stderr.String())
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout must stay empty on failure, got %q", stdout.String())
	}
}
