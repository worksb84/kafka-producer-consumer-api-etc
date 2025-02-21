FROM golang:1.21.5-alpine

RUN mkdir /crawler
WORKDIR /crawler
COPY . .
RUN go mod download

RUN go build -o crawler ./cmd/crawler

EXPOSE 8080