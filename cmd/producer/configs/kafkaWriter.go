package configs

import (
	"crypto/tls"
	"fmt"
	"strings"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
)

func NewKafkaWriter(brokerUrl string, username string, password string) *kafka.Writer {
	mechanism, err := scram.Mechanism(scram.SHA512, username, password)
	if err != nil {
		fmt.Println(err)
	}

	transport := &kafka.Transport{
		SASL: mechanism,
		TLS:  &tls.Config{},
	}

	brokerUrlArray := strings.Split(brokerUrl, ",")

	return &kafka.Writer{
		Addr:                   kafka.TCP(brokerUrlArray[0], brokerUrlArray[1], brokerUrlArray[2]),
		Balancer:               &kafka.LeastBytes{},
		Async:                  true,
		RequiredAcks:           kafka.RequireOne,
		AllowAutoTopicCreation: false,
		BatchBytes:             20971520,
		Transport:              transport,
	}
}
