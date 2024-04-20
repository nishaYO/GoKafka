package main

import (
	"context"
	"log"
	"os"
	"os/signal"

	"github.com/segmentio/kafka-go"
)

const (
	kafkaBrokers = "localhost:9092"
	topic        = "data-stream"
	groupID      = "my-group"
)

func main() {
	// Create Kafka reader
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{kafkaBrokers},
		GroupID:  groupID,
		Topic:    topic,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})

	defer r.Close()

	// Capture OS signals to gracefully shut down the consumer
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, os.Interrupt)

	// Start consuming messages
	ctx := context.Background()
	for {
		select {
		case <-sigchan:
			log.Println("Interrupt signal received. Shutting down...")
			return
		default:
			// Read a message
			m, err := r.ReadMessage(ctx)
			if err != nil {
				log.Printf("Failed to read message: %v\n", err)
				continue
			}

			// Process the message
			log.Printf("Received message: key=%s, value=%s\n", string(m.Key), string(m.Value))
		}
	}
}
