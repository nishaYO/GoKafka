# Simple Kafka Producer-Consumer Pipeline in Go

## Set Up Instructions

1. Startup Kafka Server
2. Create a Kafka topic named "data-stream":
   ```
   bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic data-stream
   ```
3. Run the Producer and the Consumer Executables


