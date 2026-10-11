package main

import (
	"context"
	"flag"
	"log"
	"time"

	"github.com/yandex/odyssey/benchmarks/worker/internal/agent"
	"github.com/yandex/odyssey/benchmarks/worker/internal/transport"
)

// запускает воркер
func main() {
	address := flag.String("address", ":9000", "gRPC listen address")
	flag.Parse()

	op := func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
			return nil
		}
	}

	log.Printf("worker agent listening on %s", *address)
	if err := transport.ListenAndServe(context.Background(), *address, agent.New(op)); err != nil {
		log.Fatal(err)
	}
}
