package configs

import (
	"context"
	"net/url"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func NewMongoDb(username string, password string, cluster string, database string) (*mongo.Database, error) {
	uri := "mongodb+srv://" + url.QueryEscape(username) + ":" + url.QueryEscape(password) + "@" + cluster

	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(uri))
	if err != nil {
		return nil, err
	}

	db := client.Database(database)

	return db, nil
}
