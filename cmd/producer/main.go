package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"bot"

	"d.core.base/producer/configs"
	"github.com/gin-gonic/gin"
)

var (
	listen           string
	kafka_broker_url string
	kafka_username   string
	kafka_password   string
	conversation_id  string
	api_key          string
)

func main() {

	flag.StringVar(&listen, "listen", ":8080", "server port")

	flag.StringVar(&kafka_broker_url, "kafka-broker-url", "", "kafka brokers url")
	flag.StringVar(&kafka_username, "kafka-username", "", "kafka sasl/scram username")
	flag.StringVar(&kafka_password, "kafka-password", "", "kafka sasl/scram password")
	flag.StringVar(&conversation_id, "conversation-id", "", "kakao work conversation id")
	flag.StringVar(&api_key, "api-key", "", "kakao work api key")
	flag.Parse()

	kafkaWriter := configs.NewKafkaWriter(kafka_broker_url, kafka_username, kafka_password)
	defer kafkaWriter.Close()

	bot := bot.NewBot(api_key, conversation_id)
	go bot.Send("코어엔진 Producer 엔진 실행")

	handler := NewHandler(kafkaWriter, bot)

	router := gin.Default()
	router.POST("/v1/:id", handler.Base)

	srv := &http.Server{
		Addr:    listen,
		Handler: router,
	}

	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %s\n", err)
		}
	}()

	quit := make(chan os.Signal)

	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Shutdown Server ...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Fatal("Server Shutdown:", err)
	}

	select {
	case <-ctx.Done():
		log.Println("timeout of 5 seconds.")
	}
	log.Println("Server exiting")

}
