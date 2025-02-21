package cron

import (
	"bytes"
	"fmt"
	"log"
	"net/http"
)

type Sender struct {
	uri         string
	contentType string
}

func NewSender(uri string) *Sender {
	return &Sender{
		uri:         uri,
		contentType: "application/json",
	}
}

func (sd *Sender) Send(id string, msg []byte) {
	log.Println(id)
	log.Println(string(msg))
	uri := fmt.Sprintf("%s/%s", sd.uri, id)
	client := &http.Client{}
	req, err := http.NewRequest("POST", uri, bytes.NewBuffer(msg))
	if err != nil {
		fmt.Println(err)
		return
	}
	req.Header.Add("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer resp.Body.Close()

	log.Println(resp.StatusCode)
}
