FROM golang:1.21.5-alpine

RUN mkdir /producer
WORKDIR /producer
COPY . .
RUN go mod download

RUN go build -o producer ./cmd/producer

EXPOSE 8080