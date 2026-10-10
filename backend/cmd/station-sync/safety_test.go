package main

import (
	"context"
	"os"
	"strings"
	"testing"
)

func TestSafetyReserveBoundaries(t *testing.T) {
	if err := budget(12*1024*1024, 5.2, 8, 50*1024*1024*1024); err != nil {
		t.Fatal(err)
	}
	for _, v := range []struct {
		mem  uint64
		load float64
		cpu  int
		disk uint64
	}{{1, 0, 8, 1 << 40}, {1 << 30, 5.21, 8, 1 << 40}, {1 << 30, 0, 0, 1 << 40}, {1 << 30, 0, 8, 1}} {
		if budget(v.mem, v.load, v.cpu, v.disk) == nil {
			t.Fatal("unsafe budget accepted")
		}
	}
}
func TestChildFailureRedactsSecretAndSourceText(t *testing.T) {
	t.Setenv("ANPFUEL_DATABASE_URL", "synthetic-secret-never-log")
	err := child(context.Background(), "synthetic_failure", "/nonexistent-owned-test-binary", nil, true)
	if err == nil || strings.Contains(err.Error(), os.Getenv("ANPFUEL_DATABASE_URL")) {
		t.Fatal("child failed without safe error")
	}
}

func TestChildCountersAreBoundedAndAllowlisted(t *testing.T) {
	var output countsBuffer
	_, _ = output.Write([]byte("accepted=7 preserved_curated=2 secret=synthetic-private-value source_key=04218406000104 accepted=invalid\n"))
	result := output.counts()
	if len(result) != 2 || result["accepted"] != 7 || result["preserved_curated"] != 2 {
		t.Fatalf("unsafe counters %+v", result)
	}
	_, _ = output.Write([]byte(strings.Repeat("X", 10000)))
	if output.Len() != 4096 {
		t.Fatal("child output exceeded cap")
	}
}
