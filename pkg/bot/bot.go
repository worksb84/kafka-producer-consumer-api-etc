package bot

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"
)

type Bot struct {
	Apikey         string
	ConversationId string
}

type Block struct {
	Type    string   `json:"type"`
	Text    string   `json:"text"`
	Inlines []Inline `json:"inlines"`
}

type Inline struct {
	Text  string `json:"text"`
	Type  string `json:"type"`
	Color string `json:"color,omitempty"`
	Bold  bool   `json:"bold"`
}

type Message struct {
	ConversationId string  `json:"conversation_id"`
	Text           string  `json:"text"`
	Blocks         []Block `json:"blocks"`
}

func (b *Bot) generate(message string) []byte {
	t := time.Now()
	messages := strings.Split(message, " ")
	now := t.Format("[2006/01/02 15:04:05]")

	inlines := make([]Inline, 0)

	inlines = append(inlines, Inline{Text: fmt.Sprintf("%s - ", now), Type: "styled", Bold: true})
	for idx, msg := range messages {
		text := msg
		if idx != len(messages)-1 {
			text = fmt.Sprintf("%s ", msg)
		}
		inline := Inline{Text: text, Type: "styled", Bold: false}
		inlines = append(inlines, inline)
	}

	blocks := make([]Block, 0)
	block := Block{Type: "text", Text: strings.Join(messages[:], " "), Inlines: inlines}
	blocks = append(blocks, block)

	m := Message{ConversationId: b.ConversationId, Text: fmt.Sprintf("%s - %s", now, strings.Join(messages[:], " ")), Blocks: blocks}

	bytes, _ := json.Marshal(&m)
	// log.Println(string(bytes))
	return bytes
}

func (b *Bot) Send(message string) {
	client := &http.Client{}
	jsonStr := b.generate(message)

	url := "https://api.kakaowork.com/v1/messages.send"
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonStr))
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", b.Apikey))
	req.Header.Set("Content-Type", "application/json")

	if err != nil {
		log.Println(err)
	}

	res, err := client.Do(req)
	if err != nil {
		log.Println(err)
	}
	defer res.Body.Close()

	_, err = io.ReadAll(res.Body)
	if err != nil {
		log.Println(err)
	}

	// data := string(body)

	// log.Println(data)
}

func NewBot(apikey string, conversationId string) *Bot {
	return &Bot{
		Apikey:         apikey,
		ConversationId: conversationId,
	}
}
