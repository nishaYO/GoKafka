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
	// Create Kafka writer for partition 0
	w1 := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{kafkaBrokers},
		Topic:   topic,
	})

	// Create Kafka writer for partition 1
	w2 := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{kafkaBrokers},
		Topic:   topic,
	})

	defer w1.Close()
	defer w2.Close()

	// Capture OS signals to gracefully shut down the producers
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, os.Interrupt)

	// Start the first producer goroutine
	go produceTypeA(w1)

	// Start the second producer goroutine
	go produceTypeB(w2)

	// Wait for an interrupt signal to gracefully shut down the producers
	<-sigchan
	log.Println("Main: Interrupt signal received. Shutting down...")
}

// produceTypeA sends messages of type A to partition 0
func produceTypeA(w *kafka.Writer) {
	ctx := context.Background()
	for {
		select {
		case <-ctx.Done():
			log.Println("Producer A: Context done. Shutting down...")
			return
		default:
			// Produce a message of type A to partition 0
			err := w.WriteMessages(ctx, kafka.Message{
				Key:     []byte("keyA"),
				Value:   []byte("message of type A"),
				Headers: []kafka.Header{{Key: "header-key", Value: []byte("A")}},
			})
			if err != nil {
				log.Printf("Producer A: Failed to write message: %v\n", err)
			} else {
				log.Println("Producer A: Message of type A sent to partition 0 successfully")
			}
		}
	}
}

// produceTypeB sends messages of type B to partition 1
func produceTypeB(w *kafka.Writer) {
	ctx := context.Background()
	for {
		select {
		case <-ctx.Done():
			log.Println("Producer B: Context done. Shutting down...")
			return
		default:
			// Produce a message of type B to partition 1
			err := w.WriteMessages(ctx, kafka.Message{
				Key:     []byte("keyB"),
				Value:   []byte("message of type B"),
				Headers: []kafka.Header{{Key: "header-key", Value: []byte("B")}},
			})
			if err != nil {
				log.Printf("Producer B: Failed to write message: %v\n", err)
			} else {
				log.Println("Producer B: Message of type B sent to partition 1 successfully")
			}
		}
	}
}
