package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	workerapi "github.com/yandex/odyssey/benchmarks/worker/api"
	"github.com/yandex/odyssey/benchmarks/worker/internal/client"
	"github.com/yandex/odyssey/benchmarks/worker/models"
	"github.com/yandex/odyssey/benchmarks/worker/parser"
	"github.com/yandex/odyssey/benchmarks/worker/storage"
)

const defaultDatabaseURL = "postgres://benchmarks:benchmarks@127.0.0.1:5433/benchmarks?sslmode=disable"

func main() {
	inputPath := flag.String("input", "", "pgbench output file for local file mode")
	databaseURL := flag.String("database-url", os.Getenv("RESULTS_DATABASE_URL"), "results database URL")
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

	if *databaseURL == "" {
		*databaseURL = defaultDatabaseURL
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
	}
	if err := repository.CreateRun(ctx, run); err != nil {
		fatal(err)
	}

	var parsed parser.PgbenchOutput
	if *agentAddr != "" {
		parsed, err = runRemote(ctx, *agentAddr, client.RunRequest{
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
		}, startedAt)
	} else {
		parsed, err = parser.ParsePgbenchOutputFile(*inputPath, startedAt)
	}
	if err != nil {
		failRun(repository, runID, err)
		fatal(err)
	}

	run.PgbenchVersion = stringPointer(parsed.Version)
	if err := repository.SaveSamples(ctx, runID, parsed.Samples); err != nil {
		failRun(repository, runID, err)
		fatal(err)
	}
	if err := repository.CompleteRun(ctx, runID, time.Now().UTC()); err != nil {
		fatal(err)
	}
	fmt.Printf("saved benchmark run %s: %d samples\n", runID, len(parsed.Samples))
}

func runRemote(ctx context.Context, address string, request client.RunRequest, startedAt time.Time) (parser.PgbenchOutput, error) {
	connection, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return parser.PgbenchOutput{}, fmt.Errorf("connect to load generator %q: %w", address, err)
	}
	defer connection.Close()

	result, err := client.Run(ctx, workerapi.NewLoadGeneratorClient(connection), request)
	if err != nil {
		return parser.PgbenchOutput{}, err
	}
	if result.State != "finished" {
		return parser.PgbenchOutput{}, fmt.Errorf("load generator finished with state %q: %s", result.State, result.Error)
	}
	if result.ExitCode != 0 {
		return parser.PgbenchOutput{}, fmt.Errorf("pgbench exited with code %d: %s", result.ExitCode, result.Error)
	}
	if result.Error != "" {
		return parser.PgbenchOutput{}, fmt.Errorf("pgbench failed: %s", result.Error)
	}
	return parser.ParsePgbenchOutput(bytes.NewReader([]byte(result.Stdout)), startedAt)
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
