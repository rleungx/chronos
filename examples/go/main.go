package main

import (
	"context"
	"log"

	chronos "github.com/rleungx/chronos"
)

func main() {
	ctx := context.Background()
	client, err := chronos.NewWithOptions(
		ctx,
		"127.0.0.1:50051",
		"orders.primary",
		chronos.WithInsecureTransport(),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	ranges, err := client.AllocateTimestamps(ctx, 1)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("tso=%d", ranges[0].StartTso)
}
