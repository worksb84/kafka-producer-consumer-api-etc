package cron

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"

	"d.core.base/crawler/utils"
	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
)

// 단기사채 미상환 잔액
func (d *DartCrawler) shortTermBondUnpaid(stockParameter StockParameter) {

	type DFElement struct {
		RceptNo              string `json:"rcept_no" dataframe:"rcept_no"`
		CorpCls              string `json:"corp_cls" dataframe:"corp_cls"`
		CorpCode             string `json:"corp_code" dataframe:"corp_code"`
		CorpName             string `json:"corp_name" dataframe:"corp_name"`
		RemndrExprtn1        string `json:"remndr_exprtn1" dataframe:"remndr_exprtn1"`
		RemndrExprtn2        string `json:"remndr_exprtn2" dataframe:"remndr_exprtn2"`
		De10Below            string `json:"de10_below" dataframe:"de10_below"`
		De10ExcessDe30Below  string `json:"de10_excess_de30_below" dataframe:"de10_excess_de30_below"`
		De30ExcessDe90Below  string `json:"de30_excess_de90_below" dataframe:"de30_excess_de90_below"`
		De90ExcessDe180Below string `json:"de90_excess_de180_below" dataframe:"de90_excess_de180_below"`
		De180ExcessYy1Below  string `json:"de180_excess_yy1_below" dataframe:"de180_excess_yy1_below"`
		Sm                   string `json:"sm" dataframe:"sm"`
		IsuLmt               string `json:"isu_lmt" dataframe:"isu_lmt"`
		RemndrLmt            string `json:"remndr_lmt" dataframe:"remndr_lmt"`
	}

	type DFStruct struct {
		Status  string      `json:"status"`
		Message string      `json:"message"`
		List    []DFElement `json:"list"`
	}

	CorpCode := stockParameter.CorpCode
	BsnsYear := stockParameter.BsnsYear
	StockCode := stockParameter.StockCode
	ReprtCode := stockParameter.ReprtCode

	params := url.Values{}
	availableApiKey, _ := d.availableApiKey()
	params.Add("crtfc_key", availableApiKey)
	params.Add("corp_code", CorpCode)
	params.Add("bsns_year", BsnsYear)
	params.Add("reprt_code", ReprtCode)

	uri := fmt.Sprintf("https://opendart.fss.or.kr/api/srtpdPsndbtNrdmpBlce.json?%s", params.Encode())
	req, _ := http.NewRequest("GET", uri, nil)
	req.Header.Add("User-Agent", "Mozilla/5.0")

	client := &http.Client{}
	res, err := client.Do(req)
	if err != nil {
		log.Fatalln(err)
	}
	d.increaseUseCount(availableApiKey)
	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		log.Fatal(err)
	}

	var dfStruct DFStruct

	json.Unmarshal(body, &dfStruct)
	if dfStruct.Status == "000" {
		df := dataframe.LoadStructs(dfStruct.List)

		df = utils.AddColumn(df, "stock_code", series.String, StockCode)
		df = utils.AddColumn(df, "bsns_year", series.String, BsnsYear)

		// s3 upload
		var buf bytes.Buffer
		buf.Reset()
		bytes := io.Writer(&buf)
		df.WriteCSV(bytes)

		prefix := fmt.Sprintf("/dart/short_term_bond/%s/%s/%s.csv", stockParameter.Year, stockParameter.Month, stockParameter.CorpCode)
		err := d.awsClient.PutS3File(buf.Bytes(), prefix)
		if err == nil {
			msg := fmt.Sprintf("다트 [%s]%s 단기사채 미상환 잔액 S3 업로드 완료", stockParameter.StockCode, stockParameter.StockName)
			go d.bot.Send(msg)
		}

		columns := []string{
			"rceptNo", "corpCls", "corpCode", "corpName", "sm",
			"remndrExprtn1", "remndrExprtn2", "de10Below", "de10ExcessDe30Below",
			"de30ExcessDe90Below", "de90ExcessDe180Below", "de180ExcessYy1Below",
			"isuLmt", "remndrLmt", "stockCode", "bsnsYear",
		}

		df, err = utils.RenameColumnTo(df, columns)
		if err != nil {
			log.Fatalln(err)
		}

		buf.Reset()
		bytes = io.Writer(&buf)
		df.WriteJSON(bytes)

		d.Sender.Send("ShortTermBondUnpaid", buf.Bytes())
		// 카프카 전송
	}
}

// 회사채 미상환 잔액
func (d *DartCrawler) corporateBondUnpaid(stockParameter StockParameter) {

	type DFElement struct {
		RceptNo            string `json:"rcept_no" dataframe:"rcept_no"`
		CorpCls            string `json:"corp_cls" dataframe:"corp_cls"`
		CorpCode           string `json:"corp_code" dataframe:"corp_code"`
		CorpName           string `json:"corp_name" dataframe:"corp_name"`
		RemndrExprtn1      string `json:"remndr_exprtn1" dataframe:"remndr_exprtn1"`
		RemndrExprtn2      string `json:"remndr_exprtn2" dataframe:"remndr_exprtn2"`
		Yy1Below           string `json:"yy1_below" dataframe:"yy1_below"`
		Yy1ExcessYy2Below  string `json:"yy1_excess_yy2_below" dataframe:"yy1_excess_yy2_below"`
		Yy2ExcessYy3Below  string `json:"yy2_excess_yy3_below" dataframe:"yy2_excess_yy3_below"`
		Yy3ExcessYy4Below  string `json:"yy3_excess_yy4_below" dataframe:"yy3_excess_yy4_below"`
		Yy4ExcessYy5Below  string `json:"yy4_excess_yy5_below" dataframe:"yy4_excess_yy5_below"`
		Yy5ExcessYy10Below string `json:"yy5_excess_yy10_below" dataframe:"yy5_excess_yy10_below"`
		Yy10Excess         string `json:"yy10_excess" dataframe:"yy10_excess"`
		Sm                 string `json:"sm" dataframe:"sm"`
	}

	type DFStruct struct {
		Status  string      `json:"status"`
		Message string      `json:"message"`
		List    []DFElement `json:"list"`
	}

	CorpCode := stockParameter.CorpCode
	BsnsYear := stockParameter.BsnsYear
	StockCode := stockParameter.StockCode
	ReprtCode := stockParameter.ReprtCode

	params := url.Values{}
	availableApiKey, _ := d.availableApiKey()
	params.Add("crtfc_key", availableApiKey)
	params.Add("corp_code", CorpCode)
	params.Add("bsns_year", BsnsYear)
	params.Add("reprt_code", ReprtCode)

	uri := fmt.Sprintf("https://opendart.fss.or.kr/api/cprndNrdmpBlce.json?%s", params.Encode())
	req, _ := http.NewRequest("GET", uri, nil)
	req.Header.Add("User-Agent", "Mozilla/5.0")

	client := &http.Client{}
	res, err := client.Do(req)
	if err != nil {
		log.Fatalln(err)
	}
	d.increaseUseCount(availableApiKey)
	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		log.Fatal(err)
	}

	var dfStruct DFStruct

	json.Unmarshal(body, &dfStruct)
	if dfStruct.Status == "000" {
		df := dataframe.LoadStructs(dfStruct.List)

		df = utils.AddColumn(df, "stock_code", series.String, StockCode)
		df = utils.AddColumn(df, "bsns_year", series.String, BsnsYear)

		// s3 upload
		var buf bytes.Buffer
		buf.Reset()
		bytes := io.Writer(&buf)
		df.WriteCSV(bytes)

		prefix := fmt.Sprintf("/dart/corporate_bond/%s/%s/%s.csv", stockParameter.Year, stockParameter.Month, stockParameter.CorpCode)
		err := d.awsClient.PutS3File(buf.Bytes(), prefix)
		if err == nil {
			msg := fmt.Sprintf("다트 [%s]%s 회사채 미상환 잔액 S3 업로드 완료", stockParameter.StockCode, stockParameter.StockName)
			go d.bot.Send(msg)
		}

		columns := []string{
			"rceptNo", "corpCls", "corpCode", "corpName", "sm",
			"remndrExprtn1", "remndrExprtn2", "yy1ExcessYy2Below",
			"yy2ExcessYy3Below", "yy1Below", "yy3ExcessYy4Below",
			"yy4ExcessYy5Below", "yy5ExcessYy10Below", "yy10Excess",
			"stockCode", "bsnsYear"}

		df, err = utils.RenameColumnTo(df, columns)
		if err != nil {
			log.Fatalln(err)
		}

		buf.Reset()
		bytes = io.Writer(&buf)
		df.WriteJSON(bytes)

		d.Sender.Send("corporateBondUnpaid", buf.Bytes())
	}
}

// 기업어음증권
func (d *DartCrawler) commercialPaperSecuritiesUnpaid(stockParameter StockParameter) {

	type DFElement struct {
		RceptNo              string `json:"rcept_no" dataframe:"rcept_no"`
		CorpCls              string `json:"corp_cls" dataframe:"corp_cls"`
		CorpCode             string `json:"corp_code" dataframe:"corp_code"`
		CorpName             string `json:"corp_name" dataframe:"corp_name"`
		RemndrExprtn1        string `json:"remndr_exprtn1" dataframe:"remndr_exprtn1"`
		RemndrExprtn2        string `json:"remndr_exprtn2" dataframe:"remndr_exprtn2"`
		De10Below            string `json:"de10_below" dataframe:"de10_below"`
		De10ExcessDe30Below  string `json:"de10_excess_de30_below" dataframe:"de10_excess_de30_below"`
		De30ExcessDe90Below  string `json:"de30_excess_de90_below" dataframe:"de30_excess_de90_below"`
		De90ExcessDe180Below string `json:"de90_excess_de180_below" dataframe:"de90_excess_de180_below"`
		De180ExcessYy1Below  string `json:"de180_excess_yy1_below" dataframe:"de180_excess_yy1_below"`
		Yy1ExcessYy2Below    string `json:"yy1_excess_yy2_below" dataframe:"yy1_excess_yy2_below"`
		Yy2ExcessYy3Below    string `json:"yy2_excess_yy3_below" dataframe:"yy2_excess_yy3_below"`
		Yy3Excess            string `json:"yy3_excess" dataframe:"yy3_excess"`
		Sm                   string `json:"sm" dataframe:"sm"`
	}

	type DFStruct struct {
		Status  string      `json:"status"`
		Message string      `json:"message"`
		List    []DFElement `json:"list"`
	}

	CorpCode := stockParameter.CorpCode
	BsnsYear := stockParameter.BsnsYear
	StockCode := stockParameter.StockCode
	ReprtCode := stockParameter.ReprtCode

	params := url.Values{}
	availableApiKey, _ := d.availableApiKey()
	params.Add("crtfc_key", availableApiKey)
	params.Add("corp_code", CorpCode)
	params.Add("bsns_year", BsnsYear)
	params.Add("reprt_code", ReprtCode)

	uri := fmt.Sprintf("https://opendart.fss.or.kr/api/entrprsBilScritsNrdmpBlce.json?%s", params.Encode())
	req, _ := http.NewRequest("GET", uri, nil)
	req.Header.Add("User-Agent", "Mozilla/5.0")

	client := &http.Client{}
	res, err := client.Do(req)
	if err != nil {
		log.Fatalln(err)
	}
	d.increaseUseCount(availableApiKey)
	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		log.Fatal(err)
	}

	var dfStruct DFStruct

	json.Unmarshal(body, &dfStruct)
	if dfStruct.Status == "000" {
		df := dataframe.LoadStructs(dfStruct.List)

		df = utils.AddColumn(df, "stock_code", series.String, StockCode)
		df = utils.AddColumn(df, "bsns_year", series.String, BsnsYear)

		// s3 upload
		var buf bytes.Buffer
		buf.Reset()
		bytes := io.Writer(&buf)
		df.WriteCSV(bytes)

		prefix := fmt.Sprintf("/dart/commercial_paper_securities/%s/%s/%s.csv", stockParameter.Year, stockParameter.Month, stockParameter.CorpCode)
		err := d.awsClient.PutS3File(buf.Bytes(), prefix)
		if err == nil {
			msg := fmt.Sprintf("다트 [%s]%s 기업어음증권 S3 업로드 완료", stockParameter.StockCode, stockParameter.StockName)
			go d.bot.Send(msg)
		}

		columns := []string{
			"rceptNo", "corpCls", "corpCode", "corpName", "sm",
			"remndrExprtn1", "remndrExprtn2", "de10Below", "de10ExcessDe30Below",
			"de30ExcessDe90Below", "de90ExcessDe180Below", "de180ExcessYy1Below",
			"yy1ExcessYy2Below", "yy2ExcessYy3Below", "yy3Excess",
			"stockCode", "bsnsYear"}

		df, err = utils.RenameColumnTo(df, columns)
		if err != nil {
			log.Fatalln(err)
		}

		buf.Reset()
		bytes = io.Writer(&buf)
		df.WriteJSON(bytes)

		d.Sender.Send("CommercialPaperSecuritiesUnpaid", buf.Bytes())
		// 카프카 전송
	}
}

// 채무증권 발행실적
func (d *DartCrawler) bond(stockParameter StockParameter) {

	type DFElement struct {
		RceptNo       string `json:"rcept_no" dataframe:"rcept_no"`
		CorpCls       string `json:"corp_cls" dataframe:"corp_cls"`
		CorpCode      string `json:"corp_code" dataframe:"corp_code"`
		CorpName      string `json:"corp_name" dataframe:"corp_name"`
		IsuCmpny      string `json:"isu_cmpny" dataframe:"isu_cmpny"`
		ScritsKndNm   string `json:"scrits_knd_nm" dataframe:"scrits_knd_nm"`
		IsuMthNm      string `json:"isu_mth_nm" dataframe:"isu_mth_nm"`
		IsuDe         string `json:"isu_de" dataframe:"isu_de"`
		FacvaluTotamt string `json:"facvalu_totamt" dataframe:"facvalu_totamt"`
		Intrt         string `json:"intrt" dataframe:"intrt"`
		EvlGradInstt  string `json:"evl_grad_instt" dataframe:"evl_grad_instt"`
		Mtd           string `json:"mtd" dataframe:"mtd"`
		RepyAt        string `json:"repy_at" dataframe:"repy_at"`
		MngtCmpny     string `json:"mngt_cmpny" dataframe:"mngt_cmpny"`
	}

	type DFStruct struct {
		Status  string      `json:"status"`
		Message string      `json:"message"`
		List    []DFElement `json:"list"`
	}

	CorpCode := stockParameter.CorpCode
	BsnsYear := stockParameter.BsnsYear
	StockCode := stockParameter.StockCode
	ReprtCode := stockParameter.ReprtCode

	params := url.Values{}
	availableApiKey, _ := d.availableApiKey()
	params.Add("crtfc_key", availableApiKey)
	params.Add("corp_code", CorpCode)
	params.Add("bsns_year", BsnsYear)
	params.Add("reprt_code", ReprtCode)

	uri := fmt.Sprintf("https://opendart.fss.or.kr/api/detScritsIsuAcmslt.json?%s", params.Encode())
	req, _ := http.NewRequest("GET", uri, nil)
	req.Header.Add("User-Agent", "Mozilla/5.0")

	client := &http.Client{}
	res, err := client.Do(req)
	if err != nil {
		log.Fatalln(err)
	}
	d.increaseUseCount(availableApiKey)
	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		log.Fatal(err)
	}

	var dfStruct DFStruct

	json.Unmarshal(body, &dfStruct)
	if dfStruct.Status == "000" {
		df := dataframe.LoadStructs(dfStruct.List)

		df = utils.AddColumn(df, "stock_code", series.String, StockCode)
		df = utils.AddColumn(df, "bsns_year", series.String, BsnsYear)
		df = utils.AddColumn(df, "reprt_code", series.String, ReprtCode)

		// s3 upload
		var buf bytes.Buffer
		buf.Reset()
		bytes := io.Writer(&buf)
		df.WriteCSV(bytes)

		prefix := fmt.Sprintf("/dart/bond/%s/%s/%s.csv", stockParameter.Year, stockParameter.Month, stockParameter.CorpCode)
		err := d.awsClient.PutS3File(buf.Bytes(), prefix)

		if err == nil {
			msg := fmt.Sprintf("다트 [%s]%s 채무증권 발행실적 S3 업로드 완료", stockParameter.StockCode, stockParameter.StockName)
			go d.bot.Send(msg)
		}

		columns := []string{
			"rceptNo", "corpCls", "corpCode", "corpName", "isuCmpny",
			"scritsKndNm", "isuMthNm", "isuDe", "facvaluTotamt", "intrt",
			"evlGradInstt", "mtd", "repyAt", "mngtCmpny", "stockCode",
			"bsnsYear", "reprtCode"}

		df, err = utils.RenameColumnTo(df, columns)
		if err != nil {
			log.Fatalln(err)
		}

		buf.Reset()
		bytes = io.Writer(&buf)
		df.WriteJSON(bytes)
		d.Sender.Send("Bond", buf.Bytes())
	}

}
