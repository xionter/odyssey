package main

import (
	"context"
	"flag"
	"log"

	"github.com/yandex/odyssey/benchmarks/worker/internal/generator"
	"github.com/yandex/odyssey/benchmarks/worker/internal/transport"
)

func main() {
	address := flag.String("address", ":9000", "gRPC listen address")
	flag.Parse()

	log.Printf("load-generator-server listening on %s", *address)
	if err := transport.ListenAndServe(context.Background(), *address, generator.New(nil)); err != nil {
		log.Fatal(err)
	}
}
