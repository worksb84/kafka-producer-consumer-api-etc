package main

import (
	"bot"
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"d.core.base/consumer/configs"
)

var (
	buffer_size      int
	kafka_broker_url string
	kafka_topic      string
	kafka_group_id   string
	kafka_username   string
	kafka_password   string
	mongodb_username string
	mongodb_password string
	mongodb_cluster  string
	mongodb_database string
	conversation_id  string
	api_key          string
)

func main() {

	flag.IntVar(&buffer_size, "buffer-size", 1024, "buffer size")

	flag.StringVar(&kafka_broker_url, "kafka-broker-url", "b-1-public.core.m87r8d.c3.kafka.ap-northeast-2.amazonaws.com:9196,b-2-public.core.m87r8d.c3.kafka.ap-northeast-2.amazonaws.com:9196,b-3-public.core.m87r8d.c3.kafka.ap-northeast-2.amazonaws.com:9196", "kafka brokers url")
	flag.StringVar(&kafka_topic, "kafka-topic", "default", "kafka topic")
	flag.StringVar(&kafka_group_id, "kafka-group-id", "", "kafka consumer groupId")
	flag.StringVar(&kafka_username, "kafka-username", "", "kafka sasl/scram username")
	flag.StringVar(&kafka_password, "kafka-password", "", "kafka sasl/scram password")

	flag.StringVar(&mongodb_username, "mongodb-username", "", "atlas mongoDb username")
	flag.StringVar(&mongodb_password, "mongodb-password", "", "atlas mongoDb password")
	flag.StringVar(&mongodb_cluster, "mongodb-cluster", "", "atlas mongoDb cluster")
	flag.StringVar(&mongodb_database, "mongodb-database", "", "atlas mongoDb database")

	flag.StringVar(&conversation_id, "conversation-id", "", "kakao work conversation id")
	flag.StringVar(&api_key, "api-key", "", "kakao work api key")

	flag.Parse()

	kafkaReader := configs.NewKafkaReader(kafka_broker_url, kafka_topic, kafka_group_id, kafka_username, kafka_password)
	defer kafkaReader.Close()

	mdb, err := configs.NewMongoDb(mongodb_username, mongodb_password, mongodb_cluster, mongodb_database)
	if err != nil {
		log.Fatalln(err)
	}

	bot := bot.NewBot(api_key, conversation_id)

	receive := NewReceive(mdb, bot)
	receive.init()

	go bot.Send("코어엔진 Consumer 엔진 실행")

	go func() {
		if err := receive.run(kafkaReader, buffer_size); err != nil {
			log.Fatalln(err)
		}
	}()

	quit := make(chan os.Signal)

	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Shutdown Server ...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	select {
	case <-ctx.Done():
		log.Println("timeout of 5 seconds.")
	}
	log.Println("Server exiting")
}
