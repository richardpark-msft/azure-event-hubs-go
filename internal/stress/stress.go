// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package main

import (
	"log"
	"os"

	"github.com/Azure/azure-event-hubs-go/v3/internal/stress/tests"
)

func main() {
	if len(os.Args) < 2 {
		log.Printf("Usage: stress (infinite)")
		os.Exit(1)
	}

	remainingArgs := os.Args[2:]

	var ec int

	switch os.Args[1] {
	case "infinite":
		ec = tests.InfiniteSendAndReceive(remainingArgs)
	default:
		log.Printf("Usage: stress (infinite)")
		ec = 1
	}

	os.Exit(ec)
}
