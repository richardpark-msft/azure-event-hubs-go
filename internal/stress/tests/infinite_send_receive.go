// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package tests

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/Azure/azure-amqp-common-go/v3/uuid"
	eventhub "github.com/Azure/azure-event-hubs-go/v3"
	"github.com/joho/godotenv"
)

func InfiniteSendAndReceive(args []string) int {
	fs := flag.NewFlagSet("sendreceive", flag.ExitOnError)

	durationStr := fs.String("duration", "10m", "Duration to run the test")

	if err := fs.Parse(args); err != nil {
		fs.PrintDefaults()
		return 1
	}

	duration, err := time.ParseDuration(*durationStr)

	if err != nil {
		log.Printf("ERROR: invalid duration: %s", err.Error())
		return 1
	}

	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	envFile := os.Getenv("ENV_FILE")

	if envFile == "" {
		envFile = ".env"
	}

	if err = godotenv.Load(envFile); err != nil {
		log.Printf("Failed to load .env file from '%s': %s", envFile, err.Error())
		return 1
	}

	cs := os.Getenv("EVENTHUB_CONNECTION_STRING")

	if cs == "" {
		log.Fatalf("No connection string in environment variable EVENTHUB_CONNECTION_STRING")
	}

	// we're just going to use a single partition for this test
	partitionID := "0"

	counters := &counters{}

	log.Printf("Starting test, will run for %d seconds", duration/time.Second)

	// grab the partition end now so we can get a corresponding count for sending
	// (otherwise any existing messages in the hub will make our receiving stats seem
	// mismatched)

	startingOffset := getCurrentPartitionOffset(ctx, cs, partitionID)

	go printCounterStats(counters, ctx)
	go sendMessages(ctx, cs, partitionID, counters)

	receiveMessages(ctx, cs, partitionID, startingOffset, counters)
	return 0
}

func printCounterStats(counters *counters, ctx context.Context) {
	t := time.NewTicker(5 * time.Second)

CounterLoop:
	for {
		counters.PrintAndClear()
		select {
		case <-ctx.Done():
			log.Printf("Printing final stats")
			counters.PrintAndClear()
			break CounterLoop
		case <-t.C:
		}
	}
}

func sendMessages(ctx context.Context, cs string, partitionID string, ctrs *counters) {
	log.Printf("Starting to send messages")

	hub, err := eventhub.NewHubFromConnectionString(cs)

	if err != nil {
		log.Fatalf("Failed to create eventhub from connection string: %s", err.Error())
	}

	defer hub.Close(ctx)

	messageBodyCtr := -1

	for {
		// create the batches
		batches := createBatchesToSend(&messageBodyCtr)

		// send the batches
		for _, batch := range batches {
		BatchRetryLoop:
			for {
				err := hub.SendBatch(ctx, &singleBatchIterator{
					EventBatch: batch.EventBatch,
				})

				if err != nil {
					ctrs.Inc(0, 1, 0, 0)
					log.Printf("ERROR: send failed with error %#v", err)
				} else {
					ctrs.Inc(int32(batch.Count), 0, 0, 0)
					break BatchRetryLoop
				}
			}
		}
	}
}

func createBatchesToSend(messageCtr *int) []*BatchWithCount {
	var batches []*BatchWithCount

	currentBatch := &BatchWithCount{
		EventBatch: eventhub.NewEventBatch(getID(), nil),
	}

	for i := 0; i < 5000; i++ {
		evt := &eventhub.Event{
			Data: []byte(fmt.Sprintf("Message %d", *messageCtr+i)),
		}

		added, err := currentBatch.Add(evt)

		if err != nil {
			panic(err)
		}

		if !added {
			*messageCtr++
			batches = append(batches, currentBatch)

			currentBatch = &BatchWithCount{
				EventBatch: eventhub.NewEventBatch(getID(), nil),
			}
		}
	}

	if currentBatch.Count > 0 {
		batches = append(batches, currentBatch)
	}

	return batches
}

func receiveMessages(ctx context.Context, cs string, partitionID string, startingOffset int64, ctrs *counters) {
	log.Printf("Starting to receive messages for partition ID %s, starting offset %d", partitionID, startingOffset)
	hub, err := eventhub.NewHubFromConnectionString(cs)

	if err != nil {
		log.Fatalf("Failed to create eventhub from connection string: %s", err.Error())
	}

	defer hub.Close(ctx)

	lastReceivedOffset := startingOffset

	for {
		handle, err := hub.Receive(ctx, partitionID, func(ctx context.Context, event *eventhub.Event) error {
			ctrs.Inc(0, 0, 1, 0)
			lastReceivedOffset = *event.SystemProperties.Offset
			return nil
		}, eventhub.ReceiveWithStartingOffset(fmt.Sprintf("%d", lastReceivedOffset)))

		<-handle.Done()

		if err != nil {
			log.Printf("ERROR: receive failed with error %#v", err)
			ctrs.Inc(0, 0, 0, 1)
		}

		if err := handle.Close(ctx); err != nil {
			log.Printf("ERROR: receive(close) failed with error %#v", err)
			ctrs.Inc(0, 0, 0, 1)
		}
	}
}

func getCurrentPartitionOffset(ctx context.Context, cs string, partitionID string) int64 {
	hub, err := eventhub.NewHubFromConnectionString(cs)

	if err != nil {
		log.Fatalf("Failed to create hub from connection string: %s", err.Error())
	}

	defer hub.Close(ctx)

	props, err := hub.GetPartitionInformation(ctx, partitionID)

	if err != nil {
		log.Fatalf("Failed to get partition information for partition ID '%s': %s", partitionID, err.Error())
	}

	lastOffset, err := strconv.ParseInt(props.LastEnqueuedOffset, 10, 64)

	if err != nil {
		log.Fatalf("Failed to convert string offset '%s' to an int64 offset: %s", props.LastEnqueuedOffset, err.Error())
	}

	return lastOffset
}

func getID() string {
	id, err := uuid.NewV4()

	if err != nil {
		log.Fatalf("Failed to create UUID: %s", err)
	}

	return id.String()
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

type BatchWithCount struct {
	Count int
	*eventhub.EventBatch
}

func (bwc *BatchWithCount) Add(evt *eventhub.Event) (bool, error) {
	added, err := bwc.EventBatch.Add(evt)

	if added {
		bwc.Count++
	}

	return added, err
}

type counters struct {
	mu sync.Mutex

	sent       int32
	sentTotal  int32
	sendErrors int32

	received      int32
	receivedTotal int32
	receiveErrors int32
}

func (c *counters) Inc(addSent int32, addSendErrors int32, addReceived int32, addReceiveErrors int32) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.sent += addSent
	c.sendErrors += addSendErrors

	c.received += addReceived
	c.receiveErrors += addReceiveErrors
}

// PrintAndClear prints the current statistics and clears them.
func (c *counters) PrintAndClear() {
	c.mu.Lock()
	sent, sendErrors := c.sent, c.sendErrors
	received, receiveErrors := c.received, c.receiveErrors

	c.sentTotal += c.sent
	c.receivedTotal += c.received

	sentTotal, receivedTotal := c.sentTotal, c.receivedTotal

	c.sent, c.sendErrors = 0, 0
	c.received, c.receiveErrors = 0, 0
	c.mu.Unlock()

	log.Printf("\n  sent : (%d errors:%d)"+
		"\n  recv : (%d errors:%d)"+
		"\n  total: (s:%d, r:%d, diff:%d)", sent, sendErrors, received, receiveErrors, sentTotal, receivedTotal, sentTotal-receivedTotal)
}
