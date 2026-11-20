package models

import (
	"time"

	"github.com/google/uuid"
)

type BenchmarkRun struct {
	ID             uuid.UUID
	CommitHash     *string
	Scenario       string
	Status         string
	PgbenchVersion *string
	PgbenchArgs    map[string]any
	StartedAt      *time.Time
	FinishedAt     *time.Time
	CreatedAt      time.Time
	ErrorMessage   *string
}

type BenchmarkSample struct {
	IntervalStart time.Time

	IntervalSeconds float64
	NumTransactions int64

	LatencySumUS   int64
	LatencySumSqUS int64
	LatencyMinUS   *int64
	LatencyMaxUS   *int64

	TPS             float64
	LatencyAvgMS    *float64
	LatencyStddevMS *float64
}
