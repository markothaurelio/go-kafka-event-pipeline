package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"
)

func main() {
	brokers := []string{"localhost:9092"}
	topic := "plaintext-event-topic"
	groupID := "plaintext-group"

	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_8_0_0
	cfg.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategyRoundRobin()
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest

	consumerGroup, err := sarama.NewConsumerGroup(brokers, groupID, cfg)
	if err != nil {
		log.Fatalf("NewConsumerGroup: %v", err)
	}
	defer consumerGroup.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle Ctrl+C cleanly
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Println("\nStopping consumer...")
		cancel()
	}()

	handler := &groupHandler{}

	fmt.Printf("Consuming topic=%s as group=%s...\n", topic, groupID)
	for ctx.Err() == nil {
		if err := consumerGroup.Consume(ctx, []string{topic}, handler); err != nil {
			log.Printf("Consume error: %v", err)
		}
	}
}

type groupHandler struct{}

func (groupHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (groupHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }
func (groupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		fmt.Printf("partition=%d offset=%d key=%s value=%s\n",
			msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))
		sess.MarkMessage(msg, "")
	}
	return nil
}
