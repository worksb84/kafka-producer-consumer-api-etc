# Crawler
- go get github.com/go-co-op/gocron/v2
- go get github.com/go-gota/gota
- go get github.com/PuerkitoBio/goquery
- go get github.com/aws/aws-sdk-go-v2/aws
- go get github.com/aws/aws-sdk-go-v2/config
- go get github.com/aws/aws-sdk-go-v2/service/s3

## How to run
```shell
go build ./cmd/crawler
./crawler
```

# Producer
- go get github.com/segmentio/kafka-go
- go get github.com/gin-gonic/gin

## How to run
```shell
go build ./cmd/producer
./producer
```

# Consumer
- go get go.mongodb.org/mongo-driver/mongo
- go get github.com/segmentio/kafka-go

## How to run
```shell
go build ./cmd/consumer
./consumer
```

# Common
- go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
- go install google.golang.org/protobuf/cmd/protoc-gen-go@latest

## Generate protubuf
```shell
protoc -I ./proto --go_out ./pkg/protobuf --go_opt paths=source_relative --go-grpc_out=./pkg/protobuf --go-grpc_opt=paths=source_relative ./proto/*.proto
protoc -I ./proto --go_out ./pkg/protobuf --go_opt paths=source_relative ./proto/protocol.proto
```

# Refer to crontab
- https://crontab-generator.com/

# How to run Kafka(docker)
```shell
# run
docker-compose up -d
docker exec -it kafka /bin/bash

# create topic
### https://aws.amazon.com/ko/blogs/tech/amazon-msk-topic-iam-access-control/
### https://docs.aws.amazon.com/ko_kr/msk/latest/developerguide/msk-acls.html
./kafka-topics.sh --create --bootstrap-server b-3-public.x.c3.kafka.ap-northeast-2.amazonaws.com:9196,b-1-public.x.c3.kafka.ap-northeast-2.amazonaws.com:9196,b-2-public.x.c3.kafka.ap-northeast-2.amazonaws.com:9196 --command-config client.properties --replication-factor 3 --partitions 1 --topic default

```

# https://docs.aws.amazon.com/ko_kr/msk/latest/developerguide/public-access.html
# https://docs.aws.amazon.com/ko_kr/msk/latest/developerguide/msk-acls.html
```shell
./kafka-acls.sh --authorizer-properties zookeeper.connect=z-3.x.c3.kafka.ap-northeast-2.amazonaws.com:2181,z-1.x.c3.kafka.ap-northeast-2.amazonaws.com:2181,z-2.x.c3.kafka.ap-northeast-2.amazonaws.com:2181 --add --allow-principal "User:dofiang" --operation All --group=* --topic=*

```
# AWS SASL/SCRAM config
```shell
# client.properties
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="dofiang" password="dofiang1003!";
```


# AWS Create EKS
```shell
eksctl create cluster --name [name] --region ap-northeast-2 --vpc-public-subnets subnet-06406c23e0600ebb4,subnet-02013dddabbfcbcee,subnet-05351774f3749d6fc --version 1.28 --nodegroup-name app --node-type t3.small --nodes 1 --nodes-min 1 --nodes-max 5
```

# AWS EKS Command
```shell
aws eks update-kubeconfig --name [name]
kubectl config get-contexts

kubectl apply -f [filename]
kubectl delete -f [filename]

kubectl get nodes
kubectl get pod -w
kubectl describe pod [pod-name]

kubectl port-forward [pod-name] 8080:8080
```

# Container Build & ECR Push
```shell
# 01. docker build
docker build -t [container-name]:1.0 -f ./cmd/[program]/docker/Dockerfile .
# 02. run & test
docker run -it -d -p 8080:8080 [container-name]:1.0
# 03. ECR
aws ecr get-login-password --region ap-northeast-2 | docker login --username AWS --password-stdin x.dkr.ecr.ap-northeast-2.amazonaws.com
docker tag [container-name]:1.0 x.dkr.ecr.ap-northeast-2.amazonaws.com/[container-name]:1.0
docker push x.dkr.ecr.ap-northeast-2.amazonaws.com/[container-name]:1.0
```

# Kakaowork Crawler Bot
```shell
# KEY 53f99358.e4280de3b9c349dda321a4de576ee44a
```