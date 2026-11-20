package client

import (
	"context"
	"fmt"
	"time"

	workerapi "github.com/yandex/odyssey/benchmarks/worker/api"
	"google.golang.org/grpc"
)

type RunRequest struct {
	RunID        string
	Scenario     string
	Host         string
	Port         uint32
	DB           string
	User         string
	Clients      uint32
	Duration     uint32
	Progress     uint32
	PollInterval time.Duration
}

type RunResult struct {
	State            string
	ExitCode         int32
	Stdout           string
	Stderr           string
	Error            string
	StartedUnixNano  int64
	FinishedUnixNano int64
	ElapsedNanos     int64
}

type LoadGenerator interface {
	Prepare(context.Context, *workerapi.PrepareRequest, ...grpc.CallOption) (*workerapi.PrepareResponse, error)
	Start(context.Context, *workerapi.StartRequest, ...grpc.CallOption) (*workerapi.StartResponse, error)
	Stop(context.Context, *workerapi.StopRequest, ...grpc.CallOption) (*workerapi.StopResponse, error)
	Status(context.Context, *workerapi.StatusRequest, ...grpc.CallOption) (*workerapi.StatusResponse, error)
}

func Run(ctx context.Context, generator LoadGenerator, request RunRequest) (RunResult, error) {
	if generator == nil {
		return RunResult{}, fmt.Errorf("load generator client is required")
	}
	if request.RunID == "" {
		return RunResult{}, fmt.Errorf("run id is required")
	}
	if request.PollInterval <= 0 {
		request.PollInterval = 100 * time.Millisecond
	}

	prepareResponse, err := generator.Prepare(ctx, &workerapi.PrepareRequest{
		RunId:    request.RunID,
		Scenario: request.Scenario,
		Host:     request.Host,
		Port:     request.Port,
		Db:       request.DB,
		User:     request.User,
		Clients:  request.Clients,
		Duration: request.Duration,
		Progress: request.Progress,
	})
	if err != nil {
		return RunResult{}, fmt.Errorf("prepare workload: %w", err)
	}
	if prepareResponse.GetState() != "prepared" {
		return RunResult{}, fmt.Errorf("prepare workload: unexpected state %q", prepareResponse.GetState())
	}

	startResponse, err := generator.Start(ctx, &workerapi.StartRequest{RunId: request.RunID})
	if err != nil {
		return RunResult{}, fmt.Errorf("start workload: %w", err)
	}

	statusRequest := &workerapi.StatusRequest{RunId: request.RunID}
	ticker := time.NewTicker(request.PollInterval)
	defer ticker.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return stopAfterCancellation(ctx, generator, request.RunID, err)
		}
		statusResponse, err := generator.Status(ctx, statusRequest)
		if err != nil {
			return RunResult{}, fmt.Errorf("get workload status: %w", err)
		}
		if statusResponse.GetState() == "finished" || statusResponse.GetState() == "cancelled" {
			return statusResult(statusResponse, startResponse.GetStartedUnixNano()), nil
		}

		select {
		case <-ctx.Done():
			return stopAfterCancellation(ctx, generator, request.RunID, ctx.Err())
		case <-ticker.C:
		}
	}
}

func stopAfterCancellation(ctx context.Context, generator LoadGenerator, runID string, cause error) (RunResult, error) {
	stopContext, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, stopErr := generator.Stop(stopContext, &workerapi.StopRequest{RunId: runID})
	if stopErr != nil {
		return RunResult{}, fmt.Errorf("stop workload after cancellation: %w: %v", cause, stopErr)
	}
	return RunResult{}, cause
}

func statusResult(response *workerapi.StatusResponse, startedUnixNano int64) RunResult {
	return RunResult{
		State:            response.GetState(),
		ExitCode:         response.GetExitCode(),
		Stdout:           response.GetStdout(),
		Stderr:           response.GetStderr(),
		Error:            response.GetError(),
		StartedUnixNano:  startedUnixNano,
		FinishedUnixNano: response.GetFinishedUnixNano(),
		ElapsedNanos:     response.GetElapsedNanos(),
	}
}
