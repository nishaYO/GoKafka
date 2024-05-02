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
	// Create Kafka reader for partition 0
	r1 := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{kafkaBrokers},
		GroupID:   "my-group",
		Topic:     "data-stream",
		Partition: 0,    // Specify partition 0
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
	})

	// Create Kafka reader for partition 1
	r2 := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{kafkaBrokers},
		GroupID:   "my-group",
		Topic:     "data-stream",
		Partition: 1,    // Specify partition 1
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
	})

	defer r1.Close()
	defer r2.Close()

	// Capture OS signals to gracefully shut down the consumers
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, os.Interrupt)

	// Start consuming messages from partition 0
	go consumeMessages(r1, 0)
	// Start consuming messages from partition 1
	go consumeMessages(r2, 1)

	// Wait for interrupt signal to gracefully shut down the consumers
	<-sigchan
	log.Println("Main: Interrupt signal received. Shutting down...")
}

func consumeMessages(r *kafka.Reader, partition int) {
	ctx := context.Background()
	for {
		select {
		case <-sigchan:
			log.Printf("Consumer for partition %d: Interrupt signal received. Shutting down...", partition)
			return
		default:
			// Read a message
			m, err := r.ReadMessage(ctx)
			if err != nil {
				log.Printf("Consumer for partition %d: Failed to read message: %v\n", partition, err)
				continue
			}
			// Process the message
			log.Printf("Consumer for partition %d: Received message: key=%s, value=%s\n", partition, string(m.Key), string(m.Value))
		}
	}
}
