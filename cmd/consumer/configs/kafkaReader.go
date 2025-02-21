package configs

import (
	"crypto/tls"
	"log"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
)

func NewKafkaReader(brokerUrl string, topic string, groupId string, username string, password string) *kafka.Reader {

	mechanism, err := scram.Mechanism(scram.SHA512, username, password)

	if err != nil {
		log.Fatalln(err)
	}

	dialer := &kafka.Dialer{
		Timeout:       10 * time.Second,
		DualStack:     true,
		SASLMechanism: mechanism,
		TLS:           &tls.Config{},
	}

	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:        strings.Split(brokerUrl, ","),
		GroupID:        groupId,
		Topic:          topic,
		MaxBytes:       20971520,
		StartOffset:    kafka.LastOffset,
		CommitInterval: time.Second,
		Dialer:         dialer,
		// Partition:      1,
	})
}
