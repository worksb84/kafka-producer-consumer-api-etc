package main

import (
	"bot"
	"context"
	"protobuf"

	"github.com/segmentio/kafka-go"
	"go.mongodb.org/mongo-driver/mongo"
)

type Receive struct {
	*mongo.Database
	fn  map[int32]func(c string, b []byte)
	bot *bot.Bot
}

func NewReceive(mongoDb *mongo.Database, bot *bot.Bot) *Receive {
	fn := make(map[int32]func(c string, b []byte))

	return &Receive{
		mongoDb,
		fn,
		bot,
	}
}

func (r *Receive) init() {
	r.fn[protobuf.TopicId_value["T_Report"]] = r.report
	r.fn[protobuf.TopicId_value["T_Footnote"]] = r.footnote
	r.fn[protobuf.TopicId_value["T_Bond"]] = r.bond
	r.fn[protobuf.TopicId_value["T_CommercialPaperSecuritiesUnpaid"]] = r.commercialPaperSecuritiesUnpaid
	r.fn[protobuf.TopicId_value["T_CorporateBondUnpaid"]] = r.corporateBondUnpaid
	r.fn[protobuf.TopicId_value["T_ShortTermBondUnpaid"]] = r.shortTermBondUnpaid
	r.fn[protobuf.TopicId_value["T_Disclosure"]] = r.disclosure
	r.fn[protobuf.TopicId_value["T_Indices"]] = r.indices
	r.fn[protobuf.TopicId_value["T_IndexComposition"]] = r.indexComposition
	r.fn[protobuf.TopicId_value["T_IndexPrice"]] = r.indexPrice
	r.fn[protobuf.TopicId_value["T_Price"]] = r.price
}

func (r *Receive) run(kafkaReader *kafka.Reader, bufferSize int) error {

	msgChan := make(chan kafka.Message, bufferSize)

	for {
		message, err := kafkaReader.ReadMessage(context.Background())

		if err != nil {
			return err
		}

		msgChan <- message

		go r.process(msgChan)
	}
}

func (r *Receive) InterfaceToObject(object interface{}) []interface{} {
	reflectValue := reflect.ValueOf(object)
	reflectObject := make([]interface{}, reflectValue.Len())
	for i := 0; i < reflectValue.Len(); i++ {
		reflectObject[i] = reflectValue.Index(i).Interface()
	}

	return reflectObject
}

func (r *Receive) save(collection string, msg interface{}) {
	obj := func(object interface{}) []interface{} {
		reflectValue := reflect.ValueOf(object)
		reflectObject := make([]interface{}, reflectValue.Len())

		for i := 0; i < reflectValue.Len(); i++ {
			reflectObject[i] = reflectValue.Index(i).Interface()
		}

		return reflectObject
	}(msg)

	col := r.Collection(collection)
	result, err := col.InsertMany(context.Background(), obj)
	if err != nil {
		log.Println(err)
	}

	log.Panicln(result.InsertedIDs...)
}
