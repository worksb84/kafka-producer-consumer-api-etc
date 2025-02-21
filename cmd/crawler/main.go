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

	"aws"

	"d.core.base/crawler/cron"
	"github.com/go-co-op/gocron/v2"
)

var (
	producer_host         string
	aws_access_key_id     string
	aws_secret_access_key string
	conversation_id       string
	api_key               string
)

func main() {
	flag.StringVar(&producer_host, "producer-host", "http://localhost:8080/v1", "kafka producer host")
	flag.StringVar(&aws_access_key_id, "aws-access-key-id", "", "aws access key id")
	flag.StringVar(&aws_secret_access_key, "aws-secret-access-key", "", "aws secret access key")
	flag.StringVar(&conversation_id, "conversation-id", "", "kakao work conversation id")
	flag.StringVar(&api_key, "api-key", "", "kakao work api key")

	flag.Parse()

	bot := bot.NewBot(api_key, conversation_id)
	go bot.Send("코어엔진 Crawler 엔진 실행")

	os.Setenv("AWS_ACCESS_KEY_ID", aws_access_key_id)
	os.Setenv("AWS_SECRET_ACCESS_KEY", aws_secret_access_key)

	sch, _ := gocron.NewScheduler()
	defer func() { _ = sch.Shutdown() }()

	s := cron.NewScheduler(sch)
	sd := cron.NewSender(producer_host)
	ac := aws.NewAWSClient(aws_access_key_id, aws_secret_access_key)
	// cron.NewDartCrawler(s, sd, ac, bot).Init()
	cron.NewKindCrawler(s, sd, ac, bot).Init()
	// cron.NewKrxCrawler(s, sd, ac, bot).Init()

	go sch.Start()

	quit := make(chan os.Signal)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Shutdown server...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	select {
	case <-ctx.Done():
		log.Println("Timeout of 5 seconds.")
	}

	log.Println("Server exiting..")

}
