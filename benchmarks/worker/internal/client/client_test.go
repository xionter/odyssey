package client

import (
	"context"
	"net"
	"testing"
	"time"

	workerapi "github.com/yandex/odyssey/benchmarks/worker/api"
	"github.com/yandex/odyssey/benchmarks/worker/internal/generator"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestRunThroughGRPCServer(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	server := grpc.NewServer()
	workerapi.RegisterLoadGeneratorServer(server, generator.New(func(context.Context, generator.Workload) generator.ExecutionResult {
		return generator.ExecutionResult{ExitCode: 0, Stdout: "progress: 1.0 s, 10.0 tps, lat 1.0 ms stddev 0.1, 0 failed"}
	}))
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()

	connection, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer connection.Close()

	result, err := Run(context.Background(), workerapi.NewLoadGeneratorClient(connection), RunRequest{
		RunID:        "integration-run",
		Scenario:     "pgbench",
		Host:         "127.0.0.1",
		Port:         6432,
		DB:           "benchmarks",
		User:         "benchmarks",
		Clients:      1,
		Duration:     1,
		Progress:     1,
		PollInterval: time.Millisecond,
	})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if result.State != "finished" || result.ExitCode != 0 || result.Stdout == "" {
		t.Fatalf("unexpected result: %+v", result)
	}
}
