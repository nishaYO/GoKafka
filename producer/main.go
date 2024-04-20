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
)

func main() {
	// Create Kafka writer
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{kafkaBrokers},
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	})

	defer w.Close()

	// Capture OS signals to gracefully shut down the producer
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, os.Interrupt)

	// Start producing messages
	for {
		select {
		case <-sigchan:
			log.Println("Interrupt signal received. Shutting down...")
			return
		default:
			// Produce a message
			err := w.WriteMessages(context.Background(), kafka.Message{
				Key:   []byte("key"),
				Value: []byte("hello, kafka!"),
			})
			if err != nil {
				log.Printf("Failed to write message: %v\n", err)
			} else {
				log.Println("Message sent successfully")
			}
		}
	}
}
