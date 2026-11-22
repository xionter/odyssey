package generator

import (
	"context"
	"errors"
	"testing"
	"time"

	workerapi "github.com/yandex/odyssey/benchmarks/worker/api"
)

func TestGeneratorRunsWorkloadAndReturnsResult(t *testing.T) {
	generator := New(func(context.Context, Workload) ExecutionResult {
		return ExecutionResult{ExitCode: 0, Stdout: "progress", Stderr: "warning"}
	})
	request := validPrepareRequest()

	if _, err := generator.Prepare(context.Background(), request); err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if _, err := generator.Start(context.Background(), &workerapi.StartRequest{RunId: request.RunId}); err != nil {
		t.Fatalf("start: %v", err)
	}

	waitForState(t, generator, StateFinished)
	response, err := generator.Status(context.Background(), &workerapi.StatusRequest{RunId: request.RunId})
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if response.ExitCode != 0 || response.Stdout != "progress" || response.Stderr != "warning" {
		t.Fatalf("unexpected result: %+v", response)
	}
}

func TestGeneratorReturnsFailedProcess(t *testing.T) {
	generator := New(func(context.Context, Workload) ExecutionResult {
		return ExecutionResult{ExitCode: 7, Err: errors.New("pgbench failed")}
	})
	request := validPrepareRequest()

	if _, err := generator.Prepare(context.Background(), request); err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if _, err := generator.Start(context.Background(), &workerapi.StartRequest{RunId: request.RunId}); err != nil {
		t.Fatalf("start: %v", err)
	}

	waitForState(t, generator, StateFinished)
	response, err := generator.Status(context.Background(), &workerapi.StatusRequest{RunId: request.RunId})
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if response.ExitCode != 7 || response.Error != "pgbench failed" {
		t.Fatalf("unexpected result: %+v", response)
	}
}

func TestGeneratorStopsRunningWorkload(t *testing.T) {
	started := make(chan struct{})
	generator := New(func(ctx context.Context, _ Workload) ExecutionResult {
		close(started)
		<-ctx.Done()
		return ExecutionResult{ExitCode: -1, Err: ctx.Err()}
	})
	request := validPrepareRequest()

	if _, err := generator.Prepare(context.Background(), request); err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if _, err := generator.Start(context.Background(), &workerapi.StartRequest{RunId: request.RunId}); err != nil {
		t.Fatalf("start: %v", err)
	}
	<-started

	response, err := generator.Stop(context.Background(), &workerapi.StopRequest{RunId: request.RunId})
	if err != nil {
		t.Fatalf("stop: %v", err)
	}
	if response.State != StateCanceled {
		t.Fatalf("unexpected state: %s", response.State)
	}
}

func TestValidateRequest(t *testing.T) {
	request := validPrepareRequest()
	request.Port = 0
	if err := validateRequest(request); err == nil {
		t.Fatal("expected invalid port error")
	}
}

func validPrepareRequest() *workerapi.PrepareRequest {
	return &workerapi.PrepareRequest{
		RunId:    "run-1",
		Scenario: "pgbench",
		Host:     "127.0.0.1",
		Port:     6432,
		Db:       "benchmarks",
		User:     "benchmarks",
		Clients:  1,
		Duration: 1,
		Progress: 1,
	}
}

func waitForState(t *testing.T, generator *Generator, state string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		generator.mu.Lock()
		current := generator.state
		generator.mu.Unlock()
		if current == state {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("generator did not reach state %q", state)
}
