package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	brokers := []string{"localhost:9092"}
	topic := "plaintext-event-topic"

	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_8_0_0
	cfg.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(brokers, cfg)
	if err != nil {
		log.Fatalf("NewSyncProducer: %v", err)
	}
	defer producer.Close()

	fmt.Println("Type a message and press Enter (Ctrl+C to quit):")
	in := bufio.NewScanner(os.Stdin)

	for in.Scan() {
		text := in.Text()
		msg := &sarama.ProducerMessage{
			Topic:     topic,
			Key:       sarama.StringEncoder("plaintext"),
			Value:     sarama.StringEncoder(text),
			Timestamp: time.Now(),
		}

		partition, offset, err := producer.SendMessage(msg)
		if err != nil {
			log.Printf("SendMessage error: %v", err)
			continue
		}
		fmt.Printf("sent -> partition=%d offset=%d\n", partition, offset)
	}

	if err := in.Err(); err != nil {
		log.Printf("stdin error: %v", err)
	}
}
