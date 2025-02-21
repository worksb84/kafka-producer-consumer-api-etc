package aws

import (
	"bytes"
	"context"
	"io"
	"log"
	"sort"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type AWSClient struct {
	client *s3.Client
}

func NewAWSClient(ak, sK string) *AWSClient {

	config, err := config.LoadDefaultConfig(
		context.TODO(),
		config.WithRegion("ap-northeast-2"),
	)
	if err != nil {
		log.Fatalf("failed to load SDK configuration, %v", err)
	}

	client := s3.NewFromConfig(config)

	return &AWSClient{client: client}
}

func (a *AWSClient) RecentS3File(prefix string) string {

	result, err := a.client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket: aws.String("domestic-original-raw"),
		Prefix: aws.String(prefix),
	})

	if err != nil {
		log.Fatalln(err)
	}

	contents := result.Contents

	keys := []string{}
	for _, v := range contents {
		keys = append(keys, *v.Key)
	}

	sort.Slice(keys, func(i, j int) bool {
		return keys[i] > keys[j]
	})

	return keys[0]
}

func (a *AWSClient) PutS3File(buf []byte, key string) error {
	_, err := a.client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String("domestic-original-raw"),
		Key:    aws.String(key),
		Body:   bytes.NewBuffer(buf),
	})

	if err != nil {
		log.Fatalln(err)
		return err
	}

	return err
}

func (a *AWSClient) GetS3File(f string) []byte {
	result, err := a.client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String("domestic-original-raw"),
		Key:    aws.String(f),
	})

	if err != nil {
		log.Fatalln(err)
	}

	defer result.Body.Close()

	body, err := io.ReadAll(result.Body)
	if err != nil {
		log.Fatalln(err)
	}

	return body

}

func (a *AWSClient) IsExistS3File(f string) bool {
	result, err := a.client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String("domestic-original-raw"),
		Key:    aws.String(f),
	})

	if err != nil {
		return false
	}

	defer result.Body.Close()

	body, err := io.ReadAll(result.Body)

	if err != nil {
		log.Fatalln(err)
		return false
	}

	if len(body) == 0 {
		return false
	}

	return true
}
