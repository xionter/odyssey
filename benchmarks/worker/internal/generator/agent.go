package generator

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strconv"
	"sync"
	"time"

	workerapi "github.com/yandex/odyssey/benchmarks/worker/api"
)

const (
	StateIdle     = "idle"
	StatePrepared = "prepared"
	StateRunning  = "running"
	StateFinished = "finished"
	StateCanceled = "cancelled"
)

type Workload struct {
	Scenario                string
	TargetHost              string
	TargetPort              uint32
	Database                string
	User                    string
	Clients                 uint32
	DurationSeconds         uint32
	ProgressIntervalSeconds uint32
}

type ExecutionResult struct {
	ExitCode int32
	Stdout   string
	Stderr   string
	Err      error
}

type Runner func(context.Context, Workload) ExecutionResult

type Generator struct {
	workerapi.UnimplementedLoadGeneratorServer
	mu       sync.Mutex
	runner   Runner
	workload Workload
	runID    string
	state    string
	result   ExecutionResult
	started  time.Time
	finished time.Time
	cancel   context.CancelFunc
	done     chan struct{}
}

func New(runner Runner) *Generator {
	if runner == nil {
		runner = runPgbench
	}
	return &Generator{state: StateIdle, runner: runner}
}

func (generator *Generator) Prepare(_ context.Context, request *workerapi.PrepareRequest) (*workerapi.PrepareResponse, error) {
	if request == nil {
		return nil, fmt.Errorf("prepare request is required")
	}
	if err := validateRequest(request); err != nil {
		return nil, err
	}

	generator.mu.Lock()
	defer generator.mu.Unlock()

	if generator.state == StateRunning || generator.state == StatePrepared {
		return nil, fmt.Errorf("cannot prepare while generator is %s", generator.state)
	}

	generator.runID = request.RunId
	generator.workload = Workload{
		Scenario:                request.ScenarioId,
		TargetHost:              request.TargetHost,
		TargetPort:              request.TargetPort,
		Database:                request.Database,
		User:                    request.User,
		Clients:                 request.Clients,
		DurationSeconds:         request.DurationSeconds,
		ProgressIntervalSeconds: request.ProgressIntervalSeconds,
	}
	generator.result = ExecutionResult{ExitCode: -1}
	generator.started = time.Time{}
	generator.finished = time.Time{}
	generator.done = nil
	generator.state = StatePrepared

	return &workerapi.PrepareResponse{RunId: generator.runID, State: generator.state}, nil
}

func (generator *Generator) Start(_ context.Context, request *workerapi.StartRequest) (*workerapi.StartResponse, error) {
	if request == nil {
		return nil, fmt.Errorf("start request is required")
	}
	if err := validateRunID(request.RunId); err != nil {
		return nil, err
	}

	generator.mu.Lock()
	defer generator.mu.Unlock()

	if generator.runID != request.RunId {
		return nil, fmt.Errorf("unknown run_id %q", request.RunId)
	}
	if generator.state != StatePrepared {
		return nil, fmt.Errorf("cannot start while generator is %s", generator.state)
	}

	runContext, cancel := context.WithCancel(context.Background())
	generator.cancel = cancel
	generator.done = make(chan struct{})
	generator.started = time.Now().UTC()
	generator.state = StateRunning
	workload := generator.workload
	done := generator.done

	go generator.execute(runContext, workload, done)

	return &workerapi.StartResponse{
		RunId:           generator.runID,
		State:           generator.state,
		StartedUnixNano: generator.started.UnixNano(),
	}, nil
}

func (generator *Generator) execute(ctx context.Context, workload Workload, done chan struct{}) {
	result := generator.runner(ctx, workload)

	generator.mu.Lock()
	generator.result = result
	if errors.Is(ctx.Err(), context.Canceled) {
		generator.state = StateCanceled
	} else {
		generator.state = StateFinished
	}
	generator.finished = time.Now().UTC()
	generator.mu.Unlock()
	close(done)
}

func (generator *Generator) Stop(ctx context.Context, request *workerapi.StopRequest) (*workerapi.StopResponse, error) {
	if request == nil {
		return nil, fmt.Errorf("stop request is required")
	}
	if err := validateRunID(request.RunId); err != nil {
		return nil, err
	}

	generator.mu.Lock()
	if generator.runID != request.RunId {
		generator.mu.Unlock()
		return nil, fmt.Errorf("unknown run_id %q", request.RunId)
	}
	if generator.state != StateRunning {
		response := generator.stopResponseLocked()
		generator.mu.Unlock()
		return response, nil
	}
	cancel := generator.cancel
	done := generator.done
	generator.mu.Unlock()

	cancel()
	select {
	case <-done:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	generator.mu.Lock()
	defer generator.mu.Unlock()
	return generator.stopResponseLocked(), nil
}

func (generator *Generator) Status(_ context.Context, request *workerapi.StatusRequest) (*workerapi.StatusResponse, error) {
	if request == nil {
		return nil, fmt.Errorf("status request is required")
	}
	if err := validateRunID(request.RunId); err != nil {
		return nil, err
	}

	generator.mu.Lock()
	defer generator.mu.Unlock()

	if generator.runID != request.RunId {
		return nil, fmt.Errorf("unknown run_id %q", request.RunId)
	}

	response := &workerapi.StatusResponse{
		RunId:            generator.runID,
		State:            generator.state,
		ElapsedNanos:     generator.elapsedLocked().Nanoseconds(),
		ExitCode:         generator.result.ExitCode,
		Stdout:           generator.result.Stdout,
		Stderr:           generator.result.Stderr,
		FinishedUnixNano: unixNano(generator.finished),
	}
	if generator.result.Err != nil {
		response.Error = generator.result.Err.Error()
	}
	return response, nil
}

func (generator *Generator) stopResponseLocked() *workerapi.StopResponse {
	response := &workerapi.StopResponse{
		RunId:            generator.runID,
		State:            generator.state,
		ElapsedNanos:     generator.elapsedLocked().Nanoseconds(),
		ExitCode:         generator.result.ExitCode,
		Stdout:           generator.result.Stdout,
		Stderr:           generator.result.Stderr,
		FinishedUnixNano: unixNano(generator.finished),
	}
	if generator.result.Err != nil {
		response.Error = generator.result.Err.Error()
	}
	return response
}

func (generator *Generator) elapsedLocked() time.Duration {
	if generator.started.IsZero() {
		return 0
	}
	end := generator.finished
	if end.IsZero() {
		end = time.Now().UTC()
	}
	return end.Sub(generator.started)
}

func validateRequest(request *workerapi.PrepareRequest) error {
	if err := validateRunID(request.RunId); err != nil {
		return err
	}
	if request.ScenarioId != "pgbench" {
		return fmt.Errorf("unsupported scenario %q", request.ScenarioId)
	}
	if request.TargetHost == "" {
		return fmt.Errorf("target_host is required")
	}
	if request.TargetPort == 0 || request.TargetPort > 65535 {
		return fmt.Errorf("target_port must be between 1 and 65535")
	}
	if request.Database == "" {
		return fmt.Errorf("database is required")
	}
	if request.User == "" {
		return fmt.Errorf("user is required")
	}
	if request.Clients == 0 || request.Clients > 10000 {
		return fmt.Errorf("clients must be between 1 and 10000")
	}
	if request.DurationSeconds == 0 || request.DurationSeconds > 86400 {
		return fmt.Errorf("duration_seconds must be between 1 and 86400")
	}
	if request.ProgressIntervalSeconds == 0 || request.ProgressIntervalSeconds > request.DurationSeconds {
		return fmt.Errorf("progress_interval_seconds must be between 1 and duration_seconds")
	}
	return nil
}

func validateRunID(runID string) error {
	if runID == "" {
		return fmt.Errorf("run_id is required")
	}
	return nil
}

func runPgbench(ctx context.Context, workload Workload) ExecutionResult {
	args := []string{
		"--progress=" + strconv.FormatUint(uint64(workload.ProgressIntervalSeconds), 10),
		"--client=" + strconv.FormatUint(uint64(workload.Clients), 10),
		"--time=" + strconv.FormatUint(uint64(workload.DurationSeconds), 10),
		"--host=" + workload.TargetHost,
		"--port=" + strconv.FormatUint(uint64(workload.TargetPort), 10),
		"--username=" + workload.User,
		workload.Database,
	}

	command := exec.CommandContext(ctx, "pgbench", args...)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	err := command.Run()
	result := ExecutionResult{
		ExitCode: -1,
		Stdout:   stdout.String(),
		Stderr:   stderr.String(),
		Err:      err,
	}
	if err == nil {
		result.ExitCode = 0
		return result
	}
	var exitError *exec.ExitError
	if errors.As(err, &exitError) {
		result.ExitCode = int32(exitError.ExitCode())
	}
	return result
}

func unixNano(value time.Time) int64 {
	if value.IsZero() {
		return 0
	}
	return value.UnixNano()
}
