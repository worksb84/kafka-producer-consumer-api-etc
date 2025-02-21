package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"protobuf"
	"strings"

	"github.com/segmentio/kafka-go"
	"go.mongodb.org/mongo-driver/bson"
)

func (r *Receive) process(msg <-chan kafka.Message) {
	m := <-msg
	id := int32(binary.LittleEndian.Uint16(m.Value[0:2]))
	c := strings.ReplaceAll(protobuf.TopicId(id).String(), "T_", "")
	b := m.Value[2:len(m.Value)]

	fn := r.fn[id]
	go fn(c, b)
}

func (r *Receive) ConvertInterface(object interface{}) interface{} {
	var intf map[string]interface{}
	rec, _ := json.Marshal(object)
	json.Unmarshal(rec, &intf)

	return intf
}

func (r *Receive) report(c string, b []byte) {
	s := []protobuf.Report{}
	err := json.Unmarshal(b, &s)
	if err != nil {
		log.Fatalln(err)
	}

	col := r.Collection(c)

	report := &s[0]

	stockName := report.StockName
	stockCode := report.StockCode
	rceptNo := report.RceptNo
	div := report.FsDiv

	filter := bson.D{
		{Key: "rceptNo", Value: bson.D{{Key: "$eq", Value: rceptNo}}},
		{Key: "stockCode", Value: bson.D{{Key: "$eq", Value: stockCode}}},
	}

	col.DeleteMany(context.TODO(), filter)

	var result []interface{}

	for _, v := range s {
		result = append(result, r.ConvertInterface(v))
	}

	_, err = col.InsertMany(context.TODO(), result)
	if err != nil {
		r.bot.Send(string(err.Error()))
	}

	msg := fmt.Sprintf("다트 [%s]%s %s 재무제표 MongoDB 업로드 완료", stockCode, stockName, div)
	r.bot.Send(msg)
}

func (r *Receive) footnote(c string, b []byte) {
	s := []protobuf.Footnote{}
	err := json.Unmarshal(b, &s)
	if err != nil {
		log.Fatalln(err)
	}

	col := r.Collection(c)

	footnote := &s[0]

	rcpNo := footnote.RcpNo
	stockCode := footnote.StockCode

	filter := bson.D{
		{Key: "rcpNo", Value: bson.D{{Key: "$eq", Value: rcpNo}}},
		{Key: "stockCode", Value: bson.D{{Key: "$eq", Value: stockCode}}},
	}

	col.DeleteMany(context.TODO(), filter)

	var result []interface{}

	for _, v := range s {
		result = append(result, r.ConvertInterface(v))
	}

	_, err = col.InsertMany(context.TODO(), result)
	if err != nil {
		r.bot.Send(string(err.Error()))
	}

	msg := fmt.Sprintf("다트 [%s] 재무제표 주석 MongoDB 업로드 완료", stockCode)
	r.bot.Send(msg)
}

func (r *Receive) bond(c string, b []byte) {
	s := []protobuf.Bond{}
	err := json.Unmarshal(b, &s)
	if err != nil {
		log.Fatalln(err)
	}

	col := r.Collection(c)

	bond := &s[0]

	corpName := bond.CorpName
	stockCode := bond.StockCode
	rceptNo := bond.RceptNo

	filter := bson.D{
		{Key: "rceptNo", Value: bson.D{{Key: "$eq", Value: rceptNo}}},
		{Key: "stockCode", Value: bson.D{{Key: "$eq", Value: stockCode}}},
	}

	col.DeleteMany(context.TODO(), filter)

	var result []interface{}

	for _, v := range s {
		result = append(result, r.ConvertInterface(v))
	}

	_, err = col.InsertMany(context.TODO(), result)
	if err != nil {
		r.bot.Send(string(err.Error()))
	}

	msg := fmt.Sprintf("다트 [%s]%s 채무증권 발행실적 MongoDB 업로드 완료", stockCode, corpName)
	r.bot.Send(msg)
}

func (r *Receive) commercialPaperSecuritiesUnpaid(c string, b []byte) {
	s := []protobuf.CommercialPaperSecuritiesUnpaid{}
	err := json.Unmarshal(b, &s)
	if err != nil {
		log.Fatalln(err)
	}

	col := r.Collection(c)

	cpsu := &s[0]

	corpName := cpsu.CorpName
	stockCode := cpsu.StockCode
	rceptNo := cpsu.RceptNo

	filter := bson.D{
		{Key: "rceptNo", Value: bson.D{{Key: "$eq", Value: rceptNo}}},
		{Key: "stockCode", Value: bson.D{{Key: "$eq", Value: stockCode}}},
	}

	col.DeleteMany(context.TODO(), filter)

	var result []interface{}

	for _, v := range s {
		result = append(result, r.ConvertInterface(v))
	}

	_, err = col.InsertMany(context.TODO(), result)
	if err != nil {
		r.bot.Send(string(err.Error()))
	}

	msg := fmt.Sprintf("다트 [%s]%s 기업어음증권 MongoDB 업로드 완료", stockCode, corpName)
	r.bot.Send(msg)
}

func (r *Receive) corporateBondUnpaid(c string, b []byte) {
	s := []protobuf.CorporateBondUnpaid{}
	err := json.Unmarshal(b, &s)
	if err != nil {
		log.Fatalln(err)
	}

	col := r.Collection(c)

	cbu := &s[0]

	corpName := cbu.CorpName
	stockCode := cbu.StockCode
	rceptNo := cbu.RceptNo

	filter := bson.D{
		{Key: "rceptNo", Value: bson.D{{Key: "$eq", Value: rceptNo}}},
		{Key: "stockCode", Value: bson.D{{Key: "$eq", Value: stockCode}}},
	}

	col.DeleteMany(context.TODO(), filter)

	var result []interface{}

	for _, v := range s {
		result = append(result, r.ConvertInterface(v))
	}

	_, err = col.InsertMany(context.TODO(), result)
	if err != nil {
		r.bot.Send(string(err.Error()))
	}

	msg := fmt.Sprintf("다트 [%s]%s 회사채 미상환 잔액 MongoDB 업로드 완료", stockCode, corpName)
	r.bot.Send(msg)
}

func (r *Receive) shortTermBondUnpaid(c string, b []byte) {
	s := []protobuf.ShortTermBondUnpaid{}
	err := json.Unmarshal(b, &s)
	if err != nil {
		log.Fatalln(err)
	}

	col := r.Collection(c)

	stbu := &s[0]

	corpName := stbu.CorpName
	stockCode := stbu.StockCode
	rceptNo := stbu.RceptNo

	filter := bson.D{
		{Key: "rceptNo", Value: bson.D{{Key: "$eq", Value: rceptNo}}},
		{Key: "stockCode", Value: bson.D{{Key: "$eq", Value: stockCode}}},
	}

	col.DeleteMany(context.TODO(), filter)

	var result []interface{}

	for _, v := range s {
		result = append(result, r.ConvertInterface(v))
	}

	_, err = col.InsertMany(context.TODO(), result)
	if err != nil {
		r.bot.Send(string(err.Error()))
	}

	msg := fmt.Sprintf("다트 [%s]%s 단기사채 미상환 잔액 MongoDB 업로드 완료", stockCode, corpName)
	r.bot.Send(msg)
}

func (r *Receive) disclosure(c string, b []byte) {
	s := []protobuf.Disclosure{}
	err := json.Unmarshal(b, &s)
	if err != nil {
		log.Fatalln(err)
	}
	col := r.Collection(c)

	for _, v := range s {
		dis := &v

		disclosureTime := dis.DisclosureTime
		rcptNo := dis.RcptNo
		stockCode := dis.StockCode

		filter := bson.D{
			{Key: "rcptNo", Value: bson.D{{Key: "$eq", Value: rcptNo}}},
			{Key: "stockCode", Value: bson.D{{Key: "$eq", Value: stockCode}}},
			{Key: "disclosureTime", Value: bson.D{{Key: "$eq", Value: disclosureTime}}},
		}

		col.DeleteMany(context.TODO(), filter)
	}

	var result []interface{}

	for _, v := range s {
		result = append(result, r.ConvertInterface(v))
	}

	_, err = col.InsertMany(context.TODO(), result)

	for _, v := range s {
		dis := &v

		stockCode := dis.StockCode
		stockNm := dis.StockNm

		msg := fmt.Sprintf("다트 [%s]%s 불성실공시 MongoDB 업로드 완료", stockCode, stockNm)
		r.bot.Send(msg)
	}

	if err != nil {
		r.bot.Send(string(err.Error()))
	}

}

func (r *Receive) indices(c string, b []byte) {
	s := []protobuf.Indices{}
	err := json.Unmarshal(b, &s)
	if err != nil {
		log.Fatalln(err)
	}

	col := r.Collection(c)

	idcs := &s[0]

	updateDate := idcs.UpdateDate

	filter := bson.D{
		{Key: "updateDate", Value: bson.D{{Key: "$eq", Value: updateDate}}},
	}

	col.DeleteMany(context.TODO(), filter)

	var result []interface{}

	for _, v := range s {
		result = append(result, r.ConvertInterface(v))
	}

	_, err = col.InsertMany(context.TODO(), result)
	if err != nil {
		r.bot.Send(string(err.Error()))
	}

	msg := fmt.Sprintf("다트 [%s] KRX 지수 MongoDB 업로드 완료", updateDate)
	r.bot.Send(msg)
}

func (r *Receive) indexComposition(c string, b []byte) {
	s := []protobuf.IndexComposition{}
	err := json.Unmarshal(b, &s)
	if err != nil {
		log.Fatalln(err)
	}

	col := r.Collection(c)

	ic := &s[0]

	updateDate := ic.UpdateDate

	filter := bson.D{
		{Key: "updateDate", Value: bson.D{{Key: "$eq", Value: updateDate}}},
	}

	col.DeleteMany(context.TODO(), filter)

	var result []interface{}

	for _, v := range s {
		result = append(result, r.ConvertInterface(v))
	}

	_, err = col.InsertMany(context.TODO(), result)
	if err != nil {
		r.bot.Send(string(err.Error()))
	}

	msg := fmt.Sprintf("다트 [%s] KRX 인덱스 지수 MongoDB 업로드 완료", updateDate)
	r.bot.Send(msg)
}

func (r *Receive) indexPrice(c string, b []byte) {
	s := []protobuf.IndexPrice{}
	err := json.Unmarshal(b, &s)
	if err != nil {
		log.Fatalln(err)
	}

	col := r.Collection(c)

	ip := &s[0]

	updateDate := ip.UpdateDate

	filter := bson.D{
		{Key: "updateDate", Value: bson.D{{Key: "$eq", Value: updateDate}}},
	}

	col.DeleteMany(context.TODO(), filter)

	var result []interface{}

	for _, v := range s {
		result = append(result, r.ConvertInterface(v))
	}

	_, err = col.InsertMany(context.TODO(), result)
	if err != nil {
		r.bot.Send(string(err.Error()))
	}

	msg := fmt.Sprintf("다트 [%s] KRX 인덱스 주가가격 MongoDB 업로드 완료", updateDate)
	r.bot.Send(msg)
}

func (r *Receive) price(c string, b []byte) {
	s := []protobuf.Price{}
	err := json.Unmarshal(b, &s)
	if err != nil {
		log.Fatalln(err)
	}

	col := r.Collection(c)

	p := &s[0]

	updateDate := p.UpdateDate

	filter := bson.D{
		{Key: "updateDate", Value: bson.D{{Key: "$eq", Value: updateDate}}},
	}

	col.DeleteMany(context.TODO(), filter)

	var result []interface{}

	for _, v := range s {
		result = append(result, r.ConvertInterface(v))
	}

	_, err = col.InsertMany(context.TODO(), result)
	if err != nil {
		r.bot.Send(string(err.Error()))
	}

	msg := fmt.Sprintf("다트 [%s] KRX 주가가격 MongoDB 업로드 완료", updateDate)
	r.bot.Send(msg)
}
