package cron

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"d.core.base/crawler/utils"
	"github.com/go-co-op/gocron/v2"
	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
)

func (d *DartCrawler) report() {

	j, _ := d.Scheduler.s.NewJob(
		gocron.CronJob("CRON_TZ=Asia/Seoul */10 * * * *", false),
		gocron.NewTask(func() {

			f := d.awsClient.RecentS3File("dart/company/")
			b := d.awsClient.GetS3File(f)

			df_stock := dataframe.ReadCSV(strings.NewReader(string(b)))

			df_stock = df_stock.FilterAggregation(
				dataframe.Or,
				dataframe.F{Colname: "corp_cls", Comparator: series.Eq, Comparando: "Y"},
				dataframe.F{Colname: "corp_cls", Comparator: series.Eq, Comparando: "K"},
			)

			for _, v := range df_stock.Names() {
				if v == "corp_code" || v == "stock_code" {
					padZero := 6
					if v == "corp_code" {
						padZero = 8
					}
					df_stock = utils.PadZeroColumn(df_stock, v, series.String, df_stock.Col(v), padZero)
				}
			}
			df_stock = df_stock.Select([]string{"corp_code", "stock_code", "acc_mt", "stock_name"})

			f = d.awsClient.RecentS3File("dart/acc_mt/")
			b = d.awsClient.GetS3File(f)

			df_acc_mt := dataframe.ReadCSV(strings.NewReader(string(b)))
			df_acc_mt = utils.AddDayColumn(df_acc_mt, "4qe", "4qs", series.Int, [3]int{0, -3, 0})

			log.Println(df_acc_mt)
			df := df_stock.InnerJoin(df_acc_mt, "acc_mt")
			df = df.Select([]string{"corp_code", "stock_name", "stock_code", "acc_mt", "1qs", "1qe",
				"2qs", "2qe", "3qs", "3qe", "4qs", "4qe"})
			df = utils.AddColumn(df, "target", series.String, "-")

			// 1분기보고서 : 11013 반기보고서 : 11012 3분기보고서 : 11014 사업보고서 : 11011
			reprt := make(map[string]string, 4)
			reprt["1q"] = "11013"
			reprt["2q"] = "11012"
			reprt["3q"] = "11014"
			reprt["4q"] = "11011"

			targetCheck := func(k string, m map[string]string) dataframe.DataFrame {
				a := make([]interface{}, df.Nrow())
				currentDate, err := strconv.Atoi(utils.GetCurrentDate())
				if err != nil {
					log.Fatalln(err)
				}
				t := df.Col("target")
				s := df.Col(fmt.Sprintf("%ss", k))
				e := df.Col(fmt.Sprintf("%se", k))

				for i := range a {
					sv, _ := s.Elem(i).Int()
					ev, _ := e.Elem(i).Int()
					tv := t.Elem(i).String()

					if sv <= currentDate && currentDate <= ev {
						a[i] = m[k]
					} else {
						a[i] = tv
					}
				}

				return df.Mutate(
					series.New(a, series.String, "target"),
				)
			}

			for k := range reprt {
				df = targetCheck(k, reprt)
			}

			df = df.Filter(
				dataframe.F{Colname: "target", Comparator: series.Neq, Comparando: "-"},
			)

			calculateBsnsYear := func(df dataframe.DataFrame) dataframe.DataFrame {
				reprtReverse := make(map[string]string, 4)
				reprtReverse["11013"] = "1qs"
				reprtReverse["11012"] = "2qs"
				reprtReverse["11014"] = "3qs"
				reprtReverse["11011"] = "4qs"

				a := make([]interface{}, df.Nrow())

				t := df.Col("target")

				for i := range a {
					tv := t.Elem(i).String()
					tq := reprtReverse[tv]
					d := df.Col(tq).Elem(i).String()
					t, err := strconv.Atoi(utils.ConvDateStringToDate(d).AddDate(0, -3, 0).Format("2006"))
					if err != nil {
						log.Fatalln(err)
					}
					a[i] = t
				}

				return df.Mutate(
					series.New(a, series.String, "bsns_year"),
				)
			}

			df = calculateBsnsYear(df)

			type DFStruct struct {
				CorpCode  string `json:"corp_code"`
				StockName string `json:"stock_name"`
				StockCode string `json:"stock_code"`
				AccMt     int    `json:"acc_mt"`
				Target    string `json:"target"`
				BsnsYear  string `json:"bsns_year"`
			}

			var buf bytes.Buffer
			bytes := io.Writer(&buf)

			df = df.Select([]string{"corp_code", "stock_name", "stock_code", "acc_mt", "target", "bsns_year"})
			df.WriteJSON(bytes)

			dfStruct := []DFStruct{}
			json.Unmarshal(buf.Bytes(), &dfStruct)

			for _, stock := range dfStruct {
				corpCode := stock.CorpCode
				stockName := stock.StockName
				stockCode := stock.StockCode
				accMt := stock.AccMt
				target := stock.Target
				bsnsYear := stock.BsnsYear
				var rceptNo string

				currentDate := utils.GetCurrentDate()
				y := utils.ConvDateStringToDate(currentDate).Format("2006")
				m := utils.ConvDateStringToDate(currentDate).Format("01")

				for _, div := range [2]string{"CFS", "OFS"} {
					prefix := fmt.Sprintf("/dart/financial_sheet/%s/%s/%s_%s.csv", y, m, corpCode, div)

					if !d.awsClient.IsExistS3File(prefix) {
						time.Sleep(500)
						params := url.Values{}
						availableApiKey, _ := d.availableApiKey()
						params.Add("crtfc_key", availableApiKey)
						params.Add("corp_code", corpCode)
						params.Add("bsns_year", bsnsYear)
						params.Add("reprt_code", target)
						params.Add("fs_div", div)

						uri := fmt.Sprintf("https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json?%s", params.Encode())
						req, _ := http.NewRequest("GET", uri, nil)
						req.Header.Add("User-Agent", "Mozilla/5.0")

						client := &http.Client{}
						res, err := client.Do(req)
						if err != nil {
							log.Fatalln(err)
						}
						d.increaseUseCount(availableApiKey)
						defer res.Body.Close()

						var body string
						if res.StatusCode == http.StatusOK {
							b, _ := io.ReadAll(res.Body)
							body = string(b)
						}

						report := Report{}
						err = json.Unmarshal([]byte(body), &report)
						if err != nil {
							log.Fatalln(err)
						}

						if report.Status != "000" {
							log.Println(report.Message)
							continue
						}

						df := dataframe.LoadStructs(report.List)
						df = utils.AddColumn(df, "stock_name", series.String, stockName)
						df = utils.AddColumn(df, "stock_code", series.String, stockCode)
						df = utils.AddColumn(df, "fs_div", series.String, div)
						df = utils.AddColumn(df, "acc_mt", series.Int, accMt)

						columns := df.Names()
						for i, c := range columns {
							columns[i] = strings.ToLower(c)
						}

						df, err = utils.RenameColumn(df, columns)
						if err != nil {
							log.Fatalln(err)
						}

						df = df.Select([]string{
							"rcept_no", "reprt_code", "bsns_year", "corp_code", "sj_div", "sj_nm", "account_id",
							"account_nm", "account_detail", "thstrm_nm", "thstrm_amount", "stock_name", "stock_code", "fs_div"})

						// s3 upload
						buf.Reset()
						bytes := io.Writer(&buf)
						df.WriteCSV(bytes)
						d.awsClient.PutS3File(buf.Bytes(), prefix)

						if err == nil {
							msg := fmt.Sprintf("다트 [%s]%s %s 재무제표 S3 업로드 완료", stockCode, stockName, div)
							d.bot.Send(msg)
						}

						columns = []string{
							"reprtCode", "rceptNo", "bsnsYear", "corpCode", "sjDiv", "sjNm", "accountId",
							"accountNm", "accountDetail", "thstrmNm", "thstrmAmount", "stockName", "stockCode", "fsDiv",
						}

						df, err = utils.RenameColumnTo(df, columns)
						if err != nil {
							log.Fatalln(err)
						}

						rceptNo = df.Col("rceptNo").Elem(0).String()

						buf.Reset()
						bytes = io.Writer(&buf)
						df.WriteJSON(bytes)

						// log.Println(string(buf.Bytes()))

						go d.Sender.Send("Report", buf.Bytes())
					}
				}

				if rceptNo != "" {
					stockParameter := StockParameter{RceptNo: rceptNo, CorpCode: corpCode, BsnsYear: bsnsYear, StockCode: stockCode, ReprtCode: target, Year: y, Month: m, StockName: stockName}
					d.footnote(stockParameter)
					d.bond(stockParameter)
					d.commercialPaperSecuritiesUnpaid(stockParameter)
					d.shortTermBondUnpaid(stockParameter)
					d.corporateBondUnpaid(stockParameter)
				}
			}

		}),

		gocron.WithTags("Report"),
	)

	log.Println(j.Tags(), "Initialize")
}
