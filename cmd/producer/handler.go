package main

import (
	"bot"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net/http"
	"protobuf"

	"github.com/gin-gonic/gin"
	"github.com/segmentio/kafka-go"
)

type Handler struct {
	kafka *kafka.Writer
	bot   *bot.Bot
}

func NewHandler(k *kafka.Writer, b *bot.Bot) *Handler {
	return &Handler{kafka: k, bot: b}
}

func (h *Handler) Base(c *gin.Context) {
	id := c.Param("id")
	bodyAsByteArray, _ := io.ReadAll(c.Request.Body)
	// body := string(bodyAsByteArray)
	h.generateMessage(id, bodyAsByteArray)
	c.Status(http.StatusOK)
}

func (h *Handler) write(t string, v []byte) {
	err := h.kafka.WriteMessages(context.Background(),
		kafka.Message{
			Topic: t,
			Key:   nil,
			Value: v,
		},
	)

	if err != nil {
		go h.bot.Send(err.Error())
	}
}

func (h *Handler) generateMessage(id string, body []byte) {
	topicValue := protobuf.TopicId_value[fmt.Sprintf("T_%s", id)]

	topicId := uint16(topicValue)
	pkt := make([]byte, 0, len(body)+2)

	tmp := make([]byte, 2)
	binary.LittleEndian.PutUint16(tmp, topicId)
	pkt = append(pkt, tmp...)

	pkt = append(pkt, body...)
	// log.Println("pkt : ", pkt)
	h.write("default", pkt)
}
