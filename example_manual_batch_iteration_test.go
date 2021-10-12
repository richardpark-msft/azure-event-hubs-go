package eventhub_test

//	MIT License
//
//	Copyright (c) Microsoft Corporation. All rights reserved.
//
//	Permission is hereby granted, free of charge, to any person obtaining a copy
//	of this software and associated documentation files (the "Software"), to deal
//	in the Software without restriction, including without limitation the rights
//	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
//	copies of the Software, and to permit persons to whom the Software is
//	furnished to do so, subject to the following conditions:
//
//	The above copyright notice and this permission notice shall be included in all
//	copies or substantial portions of the Software.
//
//	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
//	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
//	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
//	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
//	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
//	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
//	SOFTWARE

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/Azure/azure-amqp-common-go/v3/uuid"
	eventhub "github.com/Azure/azure-event-hubs-go/v3"
	"github.com/joho/godotenv"
)

func init() {
	err := godotenv.Load()

	if err != nil && os.Getenv("EVENTHUB_CONNECTION_STRING") == "" {
		log.Fatalf("No .env file and EVENTHUB_CONNECTION_STRING was not in the environment")
	}
}

// singleBatchIterator is an iterator that has only a single pre-created
// EventBatch.
type singleBatchIterator struct {
	*eventhub.EventBatch
}

func (eb *singleBatchIterator) Next(eventID string, opts *eventhub.BatchOptions) (*eventhub.EventBatch, error) {
	batch := eb.EventBatch
	batch.ID = eventID
	eb.EventBatch = nil
	return batch, nil
}

func (eb *singleBatchIterator) Done() bool {
	return eb.EventBatch == nil
}

func ExampleHub_SendBatch_manually() {
	cs := os.Getenv("EVENTHUB_CONNECTION_STRING")

	hub, err := eventhub.NewHubFromConnectionString(cs)

	if err != nil {
		log.Fatalf("failed to create Hub client: %s", err.Error())
	}

	var events []*eventhub.Event

	for i := 0; i < 5000; i++ {
		var stubData = [1024]byte{1}
		events = append(events, &eventhub.Event{
			Data: stubData[:],
			Properties: map[string]interface{}{
				"idx": i,
			},
		})
	}

	iterator := eventhub.NewEventBatchIterator(events...)

	numBatchesSent := 0

	// send the messages ourselves
	for !iterator.Done() {
		batch, err := iterator.Next(getID(), nil)

		if err != nil {
			log.Fatalf("Iteration of the batch failed: %s", err.Error())
		}

		err = hub.SendBatch(context.TODO(), &singleBatchIterator{EventBatch: batch})

		if err != nil {
			log.Fatalf("Failed to send batch of events: %s", err.Error())
		}

		numBatchesSent++
	}

	fmt.Printf("Sent %d batches", numBatchesSent)

	// Output: Sent 6 batches
}

func getID() string {
	id, err := uuid.NewV4()

	if err != nil {
		log.Fatalf("Failed to create UUID: %s", err)
	}

	return id.String()
}
