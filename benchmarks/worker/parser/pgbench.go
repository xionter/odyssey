package parser

import (
	"bufio"
	"fmt"
	"io"
	"math"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/yandex/odyssey/benchmarks/worker/models"
)

type PgbenchOutput struct {
	Version string
	Samples []models.BenchmarkSample
}

const maxScannerTokenSize = 1024 * 1024

var progressLine = regexp.MustCompile(`^progress:\s+([0-9]+(?:\.[0-9]+)?)\s+s,\s+([0-9]+(?:\.[0-9]+)?)\s+tps,\s+lat\s+([0-9]+(?:\.[0-9]+)?)\s+ms\s+stddev\s+([0-9]+(?:\.[0-9]+)?)\s*,\s+([0-9]+)\s+failed$`)
var totalTransactionsLine = regexp.MustCompile(`^number of transactions actually processed:\s+([0-9]+)$`)

func ParsePgbenchOutput(r io.Reader, runStart time.Time) (PgbenchOutput, error) {
	if r == nil {
		return PgbenchOutput{}, fmt.Errorf("parse pgbench output: nil reader")
	}
	if runStart.IsZero() {
		runStart = time.Now().UTC()
	}

	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 64*1024), maxScannerTokenSize)
	result := PgbenchOutput{Samples: make([]models.BenchmarkSample, 0)}
	lineNumber := 0
	var previousElapsed float64
	var totalTransactions *int64
	for scanner.Scan() {
		lineNumber++
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "pgbench (") && strings.HasSuffix(line, ")") {
			result.Version = strings.TrimSuffix(strings.TrimPrefix(line, "pgbench ("), ")")
			continue
		}
		if matches := totalTransactionsLine.FindStringSubmatch(line); matches != nil {
			value, err := strconv.ParseInt(matches[1], 10, 64)
			if err != nil || value < 0 {
				return PgbenchOutput{}, fmt.Errorf("parse pgbench output: line %d: invalid total transaction count %q", lineNumber, matches[1])
			}
			totalTransactions = &value
			continue
		}
		matches := progressLine.FindStringSubmatch(line)
		if matches == nil {
			if strings.HasPrefix(line, "progress:") {
				return PgbenchOutput{}, fmt.Errorf("parse pgbench output: line %d: malformed progress line", lineNumber)
			}
			continue
		}
		elapsed, err := strconv.ParseFloat(matches[1], 64)
		if err != nil || elapsed <= 0 {
			return PgbenchOutput{}, fmt.Errorf("parse pgbench output: line %d: invalid elapsed time %q", lineNumber, matches[1])
		}
		tps, err := strconv.ParseFloat(matches[2], 64)
		if err != nil || tps < 0 {
			return PgbenchOutput{}, fmt.Errorf("parse pgbench output: line %d: invalid tps %q", lineNumber, matches[2])
		}
		averageMS, err := strconv.ParseFloat(matches[3], 64)
		if err != nil || averageMS < 0 {
			return PgbenchOutput{}, fmt.Errorf("parse pgbench output: line %d: invalid latency %q", lineNumber, matches[3])
		}
		stddevMS, err := strconv.ParseFloat(matches[4], 64)
		if err != nil || stddevMS < 0 {
			return PgbenchOutput{}, fmt.Errorf("parse pgbench output: line %d: invalid latency stddev %q", lineNumber, matches[4])
		}
		failed, err := strconv.ParseInt(matches[5], 10, 64)
		if err != nil || failed < 0 {
			return PgbenchOutput{}, fmt.Errorf("parse pgbench output: line %d: invalid failed transaction count %q", lineNumber, matches[5])
		}
		if len(result.Samples) > 0 && elapsed <= previousElapsed {
			return PgbenchOutput{}, fmt.Errorf("parse pgbench output: line %d: elapsed time is not increasing", lineNumber)
		}

		actualInterval := elapsed
		if len(result.Samples) > 0 {
			actualInterval = elapsed - previousElapsed
			if actualInterval <= 0 {
				return PgbenchOutput{}, fmt.Errorf("parse pgbench output: line %d: interval must be positive", lineNumber)
			}
		}
		transactionsFloat := tps * actualInterval
		if transactionsFloat > math.MaxInt64 {
			return PgbenchOutput{}, fmt.Errorf("parse pgbench output: line %d: transaction count overflows int64", lineNumber)
		}
		transactions := int64(math.Round(transactionsFloat))
		meanUS := averageMS * 1000
		stddevUS := stddevMS * 1000
		latencySumFloat := meanUS * float64(transactions)
		latencySumSqFloat := (meanUS*meanUS + stddevUS*stddevUS) * float64(transactions)
		if latencySumFloat > math.MaxInt64 || latencySumSqFloat > math.MaxInt64 {
			return PgbenchOutput{}, fmt.Errorf("parse pgbench output: line %d: latency aggregate overflows int64", lineNumber)
		}
		latencySum := int64(math.Round(latencySumFloat))
		latencySumSq := int64(math.Round(latencySumSqFloat))
		intervalStart := runStart.Add(time.Duration((elapsed - actualInterval) * float64(time.Second)))
		result.Samples = append(result.Samples, models.BenchmarkSample{
			IntervalStart:      intervalStart,
			IntervalSeconds:    actualInterval,
			NumTransactions:    transactions,
			FailedTransactions: failed,
			LatencySumUS:       latencySum,
			LatencySumSqUS:     latencySumSq,
			LatencyMinUS:       nil,
			LatencyMaxUS:       nil,
			TPS:                tps,
			LatencyAvgMS:       float64Pointer(averageMS),
			LatencyStddevMS:    float64Pointer(stddevMS),
		})
		previousElapsed = elapsed
	}
	if err := scanner.Err(); err != nil {
		return PgbenchOutput{}, fmt.Errorf("parse pgbench output: read line %d: %w", lineNumber+1, err)
	}
	if len(result.Samples) == 0 {
		return PgbenchOutput{}, fmt.Errorf("parse pgbench output: no progress lines found")
	}
	if totalTransactions != nil {
		var parsedTransactions int64
		for _, sample := range result.Samples {
			parsedTransactions += sample.NumTransactions
		}
		delta := *totalTransactions - parsedTransactions
		last := &result.Samples[len(result.Samples)-1]
		if last.NumTransactions+delta < 0 {
			return PgbenchOutput{}, fmt.Errorf("parse pgbench output: final transaction count is lower than progress totals")
		}
		last.NumTransactions += delta
		last.TPS = float64(last.NumTransactions) / last.IntervalSeconds
		if last.LatencyAvgMS != nil && last.LatencyStddevMS != nil {
			meanUS := *last.LatencyAvgMS * 1000
			stddevUS := *last.LatencyStddevMS * 1000
			last.LatencySumUS = int64(math.Round(meanUS * float64(last.NumTransactions)))
			last.LatencySumSqUS = int64(math.Round((meanUS*meanUS + stddevUS*stddevUS) * float64(last.NumTransactions)))
		}
	}
	return result, nil
}

func ParsePgbenchOutputFile(path string, runStart time.Time) (PgbenchOutput, error) {
	file, err := os.Open(path)
	if err != nil {
		return PgbenchOutput{}, fmt.Errorf("open pgbench output %q: %w", path, err)
	}
	defer file.Close()
	return ParsePgbenchOutput(file, runStart)
}

func float64Pointer(value float64) *float64 { return &value }
