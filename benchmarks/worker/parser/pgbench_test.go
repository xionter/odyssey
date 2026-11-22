package parser

import (
	"strings"
	"testing"
	"time"
)

func TestParsePgbenchOutputKeepsFailedTransactions(t *testing.T) {
	started := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	parsed, err := ParsePgbenchOutput(strings.NewReader("pgbench (16)\nprogress: 1.0 s, 10.0 tps, lat 2.0 ms stddev 0.5, 2 failed\nnumber of transactions actually processed: 10\n"), started)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if parsed.Version != "16" || len(parsed.Samples) != 1 {
		t.Fatalf("unexpected parsed result: %+v", parsed)
	}
	if parsed.Samples[0].FailedTransactions != 2 {
		t.Fatalf("failed transactions = %d, want 2", parsed.Samples[0].FailedTransactions)
	}
}
