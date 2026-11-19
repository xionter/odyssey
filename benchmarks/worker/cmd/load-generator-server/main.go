package main

import (
	"context"
	"flag"
	"log"
	"time"

	"github.com/yandex/odyssey/benchmarks/worker/internal/generator"
	"github.com/yandex/odyssey/benchmarks/worker/internal/transport"
)

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

	log.Printf("load-generator-server listening on %s", *address)
	if err := transport.ListenAndServe(context.Background(), *address, generator.New(op)); err != nil {
		log.Fatal(err)
	}
}
