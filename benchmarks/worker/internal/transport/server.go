package transport

import (
	"context"
	"net"

	workerapi "github.com/yandex/odyssey/benchmarks/worker/api"
	"google.golang.org/grpc"
)

// запускает grpc сервер и корректно завершает его после отмены
func ListenAndServe(ctx context.Context, address string, implementation workerapi.LoadGeneratorServer) error {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	server := grpc.NewServer()
	workerapi.RegisterLoadGeneratorServer(server, implementation)
	go func() {
		<-ctx.Done()
		server.GracefulStop()
	}()
	return server.Serve(listener)
}
