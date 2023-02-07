package main

import "github.com/vikrant-segment/segment-bulk-objects-client/internal/segment"

func main() {
	client := segment.New()

	client.Start()
	client.Upload()
	client.Finish()
}
