package main

import (
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {

	deliveryChannel := make(chan kafka.Event)
	fmt.Println("Type your message")
	var input string
	fmt.Scanln(&input)
	producer := NewKafkaProducer()
	Publish(input, "teste", producer, []byte("queijo"), deliveryChannel)
	go DeliveryReport(deliveryChannel)
	producer.Flush(3000)
}

func NewKafkaProducer() *kafka.Producer {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers":   "go-kafka-kafka-1:9092",
		"delivery.timeout.ms": "3000",
		"acks":                "all",
		"enable.idempotence":  "true",
	}
	producer, err := kafka.NewProducer(configMap)
	if err != nil {
		log.Println(err.Error())
	}
	return producer
}

func Publish(msg string, topic string, producer *kafka.Producer, key []byte, deliveryChan chan kafka.Event) error {
	message := &kafka.Message{
		Value:          []byte(msg),
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            key,
	}
	err := producer.Produce(message, deliveryChan)
	if err != nil {
		return err
	}
	return nil
}

func DeliveryReport(deliveryChan chan kafka.Event) {
	for event := range deliveryChan {
		switch ev := event.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				fmt.Println("Error sending message")
			} else {
				fmt.Println("Message sent:", ev.TopicPartition)
			}
		}
	}
}
