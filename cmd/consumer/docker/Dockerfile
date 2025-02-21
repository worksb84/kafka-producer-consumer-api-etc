FROM golang:1.21.5-alpine

RUN mkdir /consumer
WORKDIR /consumer
COPY . .
RUN go mod download

RUN go build -o consumer ./cmd/consumer

EXPOSE 8080