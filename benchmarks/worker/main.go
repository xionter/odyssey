package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	workerapi "github.com/yandex/odyssey/benchmarks/worker/api"
	"github.com/yandex/odyssey/benchmarks/worker/collector"
	"github.com/yandex/odyssey/benchmarks/worker/internal/client"
	"github.com/yandex/odyssey/benchmarks/worker/models"
	"github.com/yandex/odyssey/benchmarks/worker/storage"
)

func main() {
	inputPath := flag.String("input", "", "pgbench output file for local file mode")
	databaseURL := flag.String("database-url", os.Getenv("RESULTS_DATABASE_URL"), "results database URL")
	spoolDir := flag.String("spool-dir", "", "directory for result artifacts kept until upload succeeds")
	scenario := flag.String("scenario", "pgbench", "benchmark scenario name")
	commitHash := flag.String("commit-hash", "", "tested commit hash")
	agentAddr := flag.String("agent", "", "load generator gRPC address")
	host := flag.String("host", "127.0.0.1", "Odyssey host")
	port := flag.Uint("port", 6432, "Odyssey port")
	db := flag.String("db", "", "target database")
	user := flag.String("user", "", "target PostgreSQL user")
	clients := flag.Uint("clients", 1, "pgbench clients")
	duration := flag.Uint("duration", 1, "pgbench duration in seconds")
	progress := flag.Uint("progress", 1, "pgbench progress interval in seconds")
	poll := flag.Duration("poll", 200*time.Millisecond, "agent status polling interval")
	flag.Parse()

	if *spoolDir == "" {
		*spoolDir = os.Getenv("RESULTS_SPOOL_DIR")
	}
	if *agentAddr == "" && *inputPath == "" {
		fatal(fmt.Errorf("either -agent-address or -input is required"))
	}
	if *agentAddr != "" && (*db == "" || *user == "") {
		fatal(fmt.Errorf("-db and -user are required in gRPC mode"))
	}

	ctx := context.Background()
	repository, err := storage.New(ctx, *databaseURL)
	if err != nil {
		fatal(err)
	}
	defer repository.Close()
	collector := &collector.Collector{Repository: repository, SpoolDir: *spoolDir}
	if err := collector.Replay(ctx); err != nil {
		fatal(fmt.Errorf("replay stored results: %w", err))
	}

	runID := uuid.New()
	startedAt := time.Now().UTC()
	run := models.BenchmarkRun{
		ID:         runID,
		Scenario:   *scenario,
		Status:     "running",
		StartedAt:  &startedAt,
		CommitHash: stringPointer(*commitHash),
		PgbenchArgs: map[string]any{
			"clients":  *clients,
			"duration": *duration,
			"progress": *progress,
			"host":     *host,
			"port":     *port,
			"db":       *db,
		},
		Config: map[string]any{
			"clients":  *clients,
			"duration": *duration,
			"progress": *progress,
		},
		Environment: map[string]any{
			"odyssey_host":   *host,
			"odyssey_port":   *port,
			"load_generator": *agentAddr,
		},
	}
	if err := repository.CreateRun(ctx, run); err != nil {
		fatal(err)
	}

	var rawOutput []byte
	if *agentAddr != "" {
		result, remoteErr := runRemote(ctx, *agentAddr, client.RunRequest{
			RunID:        runID.String(),
			Scenario:     *scenario,
			Host:         *host,
			Port:         uint32(*port),
			DB:           *db,
			User:         *user,
			Clients:      uint32(*clients),
			Duration:     uint32(*duration),
			Progress:     uint32(*progress),
			PollInterval: *poll,
		})
		err = remoteErr
		if err == nil {
			rawOutput = combinePgbenchOutput(result)
		}
	} else {
		rawOutput, err = os.ReadFile(*inputPath)
		if err != nil {
			err = fmt.Errorf("collect result file %q: %w", *inputPath, err)
		}
	}
	if err != nil {
		failRun(repository, runID, err)
		fatal(err)
	}
	if err := collector.Collect(ctx, run, rawOutput, time.Now().UTC()); err != nil {
		fatal(err)
	}
	fmt.Printf("saved benchmark run %s\n", runID)
}

// pgbench writes progress reports to stderr on some versions and the final
// report to stdout. The parser accepts both kinds of lines, so preserve both
// streams before handing the result to the collector.
func combinePgbenchOutput(result client.RunResult) []byte {
	output := []byte(result.Stdout)
	if result.Stderr != "" {
		if len(output) > 0 && output[len(output)-1] != '\n' {
			output = append(output, '\n')
		}
		output = append(output, result.Stderr...)
	}
	return output
}

func runRemote(ctx context.Context, address string, request client.RunRequest) (client.RunResult, error) {
	connection, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return client.RunResult{}, fmt.Errorf("connect to load generator %q: %w", address, err)
	}
	defer connection.Close()

	result, err := client.Run(ctx, workerapi.NewLoadGeneratorClient(connection), request)
	if err != nil {
		return client.RunResult{}, fmt.Errorf("collect remote result: %w", err)
	}
	if result.State != "finished" {
		return client.RunResult{}, fmt.Errorf("execution: load generator finished with state %q: %s", result.State, result.Error)
	}
	if result.ExitCode != 0 {
		return client.RunResult{}, fmt.Errorf("execution: pgbench exited with code %d: %s", result.ExitCode, executionDetails(result))
	}
	if result.Error != "" {
		return client.RunResult{}, fmt.Errorf("execution: pgbench failed: %s", executionDetails(result))
	}
	return result, nil
}

func executionDetails(result client.RunResult) string {
	if result.Error != "" && result.Stderr != "" {
		return result.Error + ": " + result.Stderr
	}
	if result.Error != "" {
		return result.Error
	}
	if result.Stderr != "" {
		return result.Stderr
	}
	return "no diagnostic output"
}

func failRun(repository *storage.PostgresRepository, runID uuid.UUID, cause error) {
	if err := repository.FailRun(context.Background(), runID, time.Now().UTC(), cause.Error()); err != nil {
		fmt.Fprintf(os.Stderr, "also failed to mark run %s as failed: %v\n", runID, err)
	}
}

func stringPointer(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
