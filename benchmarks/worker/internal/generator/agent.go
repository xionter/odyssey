package generator

import (
	"context"
	"fmt"
	"sync"
	"time"

	workerapi "github.com/yandex/odyssey/benchmarks/worker/api"
)

const (
	StateIdle     = "idle"
	StatePrepared = "prepared"
	StateRunning  = "running"
	StateFinished = "finished"
)

type Operation func(context.Context) error

type Generator struct {
	workerapi.UnimplementedLoadGeneratorServer
	mu         sync.Mutex
	operation  Operation
	runID      string
	scenario   string
	target     string
	operations uint32
	state      string
	err        error
	started    time.Time
	finished   time.Time
	cancel     context.CancelFunc
	done       chan struct{}
}

func New(operation Operation) *Generator {
	if operation == nil {
		operation = func(context.Context) error { return nil }
	}
	return &Generator{state: StateIdle, operation: operation}
}

// сохраняет конфигурацию запуска и переводит генератор в prepared
func (generator *Generator) Prepare(_ context.Context, request *workerapi.PrepareRequest) (*workerapi.PrepareResponse, error) {
	if request == nil {
		return nil, fmt.Errorf("prepare request is required")
	}
	if err := validateRunID(request.RunId); err != nil {
		return nil, err
	}

	generator.mu.Lock()
	defer generator.mu.Unlock()

	if generator.state == StateRunning || generator.state == StatePrepared {
		return nil, fmt.Errorf("cannot prepare while generator is %s", generator.state)
	}

	generator.runID = request.RunId
	generator.scenario = request.ScenarioId
	generator.target = request.Target
	generator.operations = request.Operations
	generator.err = nil
	generator.started = time.Time{}
	generator.finished = time.Time{}
	generator.done = nil
	generator.state = StatePrepared

	return &workerapi.PrepareResponse{RunId: generator.runID, State: generator.state}, nil
}

// запускает операцию в отдельной горутине и фиксирует время старта
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
	generator.started = time.Now()
	generator.state = StateRunning

	go generator.execute(runContext, generator.done)

	return &workerapi.StartResponse{
		RunId:           generator.runID,
		State:           generator.state,
		StartedUnixNano: generator.started.UnixNano(),
	}, nil
}

// выполняет операцию и переводит генератор в конечное состояние
func (generator *Generator) execute(ctx context.Context, done chan struct{}) {
	err := generator.operation(ctx)

	generator.mu.Lock()
	generator.err = err
	if generator.state == StateRunning {
		generator.state = StateFinished
	}
	generator.finished = time.Now()
	generator.mu.Unlock()
	close(done)
}

// отменяет активную операцию и ждёт её завершения
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

// возвращает состояние, ошибку и длительность текущего запуска
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
		RunId:        generator.runID,
		State:        generator.state,
		ElapsedNanos: generator.elapsedLocked().Nanoseconds(),
	}
	if generator.err != nil {
		response.Error = generator.err.Error()
	}
	return response, nil
}

// формирует ответ при остановке
func (generator *Generator) stopResponseLocked() *workerapi.StopResponse {
	return &workerapi.StopResponse{
		RunId:        generator.runID,
		State:        generator.state,
		ElapsedNanos: generator.elapsedLocked().Nanoseconds(),
	}
}

// вычисляет длительность запуска
func (generator *Generator) elapsedLocked() time.Duration {
	if generator.started.IsZero() {
		return 0
	}
	end := generator.finished
	if end.IsZero() {
		end = time.Now()
	}
	return end.Sub(generator.started)
}

// checks that a run has an identifier.
func validateRunID(runID string) error {
	if runID == "" {
		return fmt.Errorf("run_id is required")
	}
	return nil
}
