package cron

import (
	"aws"
	"bot"
	"bytes"
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

type KrxCrawler struct {
	*Scheduler
	*Sender
	awsClient *aws.AWSClient
	bot       *bot.Bot
}

func NewKrxCrawler(s *Scheduler, sd *Sender, awsClient *aws.AWSClient, b *bot.Bot) *KrxCrawler {
	return &KrxCrawler{
		Scheduler: s,
		Sender:    sd,
		awsClient: awsClient,
		bot:       b,
	}
}

func (k *KrxCrawler) Init() {
	k.krxIndices()
	k.krxIndexComposition()
	k.krxPrice()
	k.krxIndexPrice()
}

func (k *KrxCrawler) krxIndices() {
	j, _ := k.Scheduler.s.NewJob(
		gocron.CronJob("CRON_TZ=Asia/Seoul 0 18 * * *", false),
		gocron.NewTask(func() {
			type Indices struct {
				Code string
				Name string
			}

			currentDate := utils.GetCurrentDate()
			y := utils.ConvDateStringToDate(currentDate).Format("2006")
			m := utils.ConvDateStringToDate(currentDate).Format("01")
			indices := []Indices{
				{Code: "10", Name: "에너지"},
				// {Code: "1010", Name: "에너지(에너지)"},
				{Code: "15", Name: "소재"},
				// {Code: "1510", Name: "소재(소재)"},
				{Code: "20", Name: "산업재"},
				// {Code: "2010", Name: "산업재(자본재)"},
				// {Code: "2020", Name: "산업재(상업전문서비스)"},
				// {Code: "2030", Name: "산업재(운송)"},
				{Code: "25", Name: "자유소비재"},
				// {Code: "2510", Name: "자유소비재(자동차및부품)"},
				// {Code: "2520", Name: "자유소비재(내구소비재및의류)"},
				// {Code: "2530", Name: "자유소비재(소비자서비스)"},
				// {Code: "2540", Name: "자유소비재(미디어)"},
				// {Code: "2550", Name: "자유소비재(소매)"},
				{Code: "30", Name: "필수소비재"},
				// {Code: "3010", Name: "필수소비재(음식료소매)"},
				// {Code: "3020", Name: "필수소비재(음식료담배)"},
				// {Code: "3030", Name: "필수소비재(가정및개인용품)"},
				{Code: "35", Name: "건강관리"},
				// {Code: "3510", Name: "건강관리(건강관리서비스및장비)"},
				// {Code: "3520", Name: "건강관리(제약,생명공학및생명과학)"},
				{Code: "40", Name: "금융"},
				// {Code: "4010", Name: "금융(은행)"},
				// {Code: "4020", Name: "금융(다각화된금융)"},
				// {Code: "4030", Name: "금융(보험)"},
				{Code: "45", Name: "정보기술"},
				// {Code: "4510", Name: "정보기술(소프트웨어및IT서비스)"},
				// {Code: "4520", Name: "정보기술(하드웨어및IT장비)"},
				// {Code: "4530", Name: "정보기술(반도체및반도체장비)"},
				{Code: "50", Name: "커뮤니케이션서비스"},
				// {Code: "5010", Name: "커뮤니케이션서비스(통신서비스)"},
				// {Code: "5020", Name: "커뮤니케이션서비스(미디어와엔터테인먼트)"},
				{Code: "55", Name: "유틸리티"},
				// {Code: "5510", Name: "유틸리티(유틸리티)"},
				{Code: "60", Name: "부동산"},
				// {Code: "6010", Name: "부동산(부동산)"},
			}

			for _, c := range indices {
				t := time.Now().UnixNano()
				millis := t / 1000000

				params := url.Values{}
				params.Add("bld", "MKD/03/0303/03030204/mkd03030204_03")
				params.Add("name", "form")
				params.Add("_", strconv.Itoa(int(millis)))

				uri := fmt.Sprintf("http://index.krx.co.kr/contents/COM/GenerateOTP.jspx?%s", params.Encode())
				req, _ := http.NewRequest("POST", uri, nil)
				req.Header.Add("User-Agent", "Mozilla/5.0")

				client := &http.Client{}
				res, err := client.Do(req)
				if err != nil {
					log.Fatalln(err)
				}

				defer res.Body.Close()

				var code string
				if res.StatusCode == http.StatusOK {
					b, _ := io.ReadAll(res.Body)
					code = string(b)
				}

				params = url.Values{}
				params.Add("mkt_tp_cd", "ALL")
				params.Add("gics_ind_grp_cd", c.Code)
				params.Add("pagePath", "/contents/MKD/03/0303/03030204/MKD03030204.jsp")
				params.Add("lang", "ko")
				params.Add("date", currentDate)
				params.Add("code", code)

				uri = fmt.Sprintf("http://index.krx.co.kr/contents/IDX/99/IDX99000001.jspx?%s", params.Encode())
				req, _ = http.NewRequest("POST", uri, nil)
				req.Header.Add("User-Agent", "Mozilla/5.0")

				client = &http.Client{}
				res, err = client.Do(req)
				if err != nil {
					log.Fatalln(err)
				}

				var body string
				if res.StatusCode == http.StatusOK {
					bytes, _ := io.ReadAll(res.Body)
					body = string(bytes)
					body = strings.Split(body, "[{")[1]
					body = strings.Split(body, "}]")[0]
					body = fmt.Sprintf("%s%s%s", "[{", body, "}]")
				}

				df := dataframe.ReadJSON(strings.NewReader(body))

				df = utils.AddColumn(df, "indices", series.String, c.Code)
				df = utils.AddColumn(df, "update_date", series.String, currentDate)
				for _, v := range df.Names() {
					if v != "isu_cd" && v != "isu_abbr" {
						df = utils.ChangeColumnType(df, v, ",", series.Float)
					}
				}

				// s3 upload
				var buf bytes.Buffer
				buf.Reset()
				bytes := io.Writer(&buf)
				df.WriteCSV(bytes)

				prefix := fmt.Sprintf("/krx/indices/%s/%s/%s.csv", y, m, currentDate)
				err = k.awsClient.PutS3File(buf.Bytes(), prefix)

				if err == nil {
					msg := "KRX 지수 수집 S3 업로드 완료"
					go k.bot.Send(msg)
				}

				columns := []string{
					"isuCd", "isuAbbr", "prsntprc", "cmpprevddFlucTp1", "cmpprevddPrc",
					"flucRt", "askord", "bidord", "trdvol", "trdval", "opnprc", "hgprc",
					"lwprc", "parval", "listshrCnt", "listMktcap", "indices", "updateDate",
				}

				df, err = utils.RenameColumnTo(df, columns)
				if err != nil {
					log.Fatalln(err)
				}

				buf.Reset()
				bytes = io.Writer(&buf)
				df.WriteJSON(bytes)

				k.Sender.Send("Indices", buf.Bytes())
			}

		}),
		gocron.WithTags("krxIndices"),
	)

	log.Println(j.Tags(), "Initialize")
}

func (k *KrxCrawler) krxIndexComposition() {
	j, _ := k.Scheduler.s.NewJob(
		gocron.CronJob("CRON_TZ=Asia/Seoul 0 18 * * *", false),
		gocron.NewTask(func() {

			type Parameter struct {
				tboxindIdx_finder_equidx0_1   string
				indIdx                        string
				indIdx2                       string
				codeNmindIdx_finder_equidx0_1 string
				index                         string
			}

			parameters := []Parameter{
				{tboxindIdx_finder_equidx0_1: "KRX 100", indIdx: "5", indIdx2: "042", codeNmindIdx_finder_equidx0_1: "KRX 100", index: "krx_100"},
				{tboxindIdx_finder_equidx0_1: "KRX 300", indIdx: "5", indIdx2: "300", codeNmindIdx_finder_equidx0_1: "KRX 300", index: "krx_300"},
				{tboxindIdx_finder_equidx0_1: "코스피 200", indIdx: "1", indIdx2: "028", codeNmindIdx_finder_equidx0_1: "코스피 200", index: "kospi_200"},
				{tboxindIdx_finder_equidx0_1: "코스닥 150", indIdx: "2", indIdx2: "203", codeNmindIdx_finder_equidx0_1: "코스닥 150", index: "kosdaq_150"},
			}

			currentDate := utils.GetCurrentDate()
			y := utils.ConvDateStringToDate(currentDate).Format("2006")
			m := utils.ConvDateStringToDate(currentDate).Format("01")

			for _, p := range parameters {
				params := url.Values{}
				params.Add("bld", "dbms/MDC/STAT/standard/MDCSTAT00601")
				params.Add("tboxindIdx_finder_equidx0_1", p.tboxindIdx_finder_equidx0_1)
				params.Add("indIdx", p.indIdx)
				params.Add("indIdx2", p.indIdx2)
				params.Add("codeNmindIdx_finder_equidx0_1", p.codeNmindIdx_finder_equidx0_1)
				params.Add("param1indIdx_finder_equidx0_1", "")
				params.Add("trdDd", currentDate)
				params.Add("money", "3")
				params.Add("csvxls_isNo", "false")

				uri := fmt.Sprintf("http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd?%s", params.Encode())
				req, _ := http.NewRequest("POST", uri, nil)
				req.Header.Add("User-Agent", "Mozilla/5.0")

				client := &http.Client{}
				res, err := client.Do(req)

				if err != nil {
					log.Fatalln(err)
				}

				var body string
				if res.StatusCode == http.StatusOK {
					bytes, _ := io.ReadAll(res.Body)
					body = string(bytes)
					body = strings.Split(body, "[{")[1]
					body = strings.Split(body, "}]")[0]
					body = fmt.Sprintf("%s%s%s", "[{", body, "}]")
				}

				df := dataframe.ReadJSON(strings.NewReader(body))

				columns := df.Names()
				for i, c := range columns {
					columns[i] = strings.ToLower(c)
				}

				df, err = utils.RenameColumn(df, columns)
				if err != nil {
					log.Fatalln(err)
				}

				if df.Nrow() == 0 {
					return
				}

				for _, v := range df.Names() {
					if v != "isu_srt_cd" && v != "isu_abbrv" {
						df = utils.ChangeColumnType(df, v, ",", series.Float)
					}
				}
				df = df.Arrange(dataframe.RevSort("mktcap"))
				df = utils.AddColumn(df, "update_date", series.String, currentDate)
				df = df.Mutate(series.New(utils.MakeRange(1, df.Nrow()), series.Int, "rank"))
				df = utils.AddColumn(df, "index", series.String, p.index)
				for _, v := range df.Names() {
					if v != "idx_nm" {
						df = utils.ChangeColumnType(df, v, ",", series.Float)
					}
				}

				// s3 upload
				var buf bytes.Buffer
				buf.Reset()
				bytes := io.Writer(&buf)
				df.WriteCSV(bytes)

				prefix := fmt.Sprintf("/krx/%s/%s/%s/%s.csv", p.index, y, m, currentDate)
				err = k.awsClient.PutS3File(buf.Bytes(), prefix)

				if err == nil {
					msg := "KRX 인덱스 지수 수집 S3 업로드 완료"
					go k.bot.Send(msg)
				}

				columns = []string{
					"isuSrtCd", "isuAbbrv", "tddClsPrc", "flucTpCd",
					"strCmpPrc", "flucRt", "mktCap", "updateDate",
					"rank", "index"}

				df, err = utils.RenameColumnTo(df, columns)
				if err != nil {
					log.Fatalln(err)
				}

				buf.Reset()
				bytes = io.Writer(&buf)
				df.WriteJSON(bytes)

				k.Sender.Send("IndexComposition", buf.Bytes())
			}

		}),
		gocron.WithTags("krxIndexComposition"),
	)

	log.Println(j.Tags(), "Initialize")
}

func (k *KrxCrawler) krxIndexPrice() {
	j, _ := k.Scheduler.s.NewJob(
		gocron.CronJob("CRON_TZ=Asia/Seoul 0 18 * * *", false),
		gocron.NewTask(func() {

			cls := [2]string{"02", "03"} // 02 KOSPI, 03 KOSDAQ
			currentDate := utils.GetCurrentDate()
			y := utils.ConvDateStringToDate(currentDate).Format("2006")
			m := utils.ConvDateStringToDate(currentDate).Format("01")
			for _, c := range cls {
				params := url.Values{}
				params.Add("bld", "dbms/MDC/STAT/standard/MDCSTAT00101")
				params.Add("locale", "ko_KR")
				params.Add("idxIndMidclssCd", c)
				params.Add("trdDd", currentDate)
				params.Add("share", "2")
				params.Add("money", "3")
				params.Add("csvxls_isNo", "false")

				uri := fmt.Sprintf("http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd?%s", params.Encode())
				req, _ := http.NewRequest("POST", uri, nil)
				req.Header.Add("User-Agent", "Mozilla/5.0")

				client := &http.Client{}
				res, err := client.Do(req)

				if err != nil {
					log.Fatalln(err)
				}

				var body string
				if res.StatusCode == http.StatusOK {
					bytes, _ := io.ReadAll(res.Body)
					body = string(bytes)
					body = strings.Split(body, "[{")[1]
					body = strings.Split(body, "}]")[0]
					body = fmt.Sprintf("%s%s%s", "[{", body, "}]")
				}

				df := dataframe.ReadJSON(strings.NewReader(body))

				columns := df.Names()
				for i, c := range columns {
					columns[i] = strings.ToLower(c)
				}

				df, err = utils.RenameColumn(df, columns)
				if err != nil {
					log.Fatalln(err)
				}

				df = df.Subset(utils.MakeRange(1, df.Nrow()-1))

				if df.Nrow() == 0 && df.Subset([]int{0}).Col("clsprc_idx").Elem(0).String() == "-" {
					return
				}

				for _, v := range df.Names() {
					if v != "idx_nm" {
						df = utils.ChangeColumnType(df, v, ",", series.Float)
					}
				}

				df = utils.AddColumn(df, "update_date", series.String, currentDate)
				df = utils.AddColumn(df, "market", series.String, c)

				// s3 upload
				var buf bytes.Buffer
				buf.Reset()
				bytes := io.Writer(&buf)
				df.WriteCSV(bytes)

				prefix := fmt.Sprintf("/krx/index_price/%s/%s/%s.csv", y, m, currentDate)
				err = k.awsClient.PutS3File(buf.Bytes(), prefix)

				if err == nil {
					msg := "KRX 인덱스 주가가격 수집 S3 업로드 완료"
					go k.bot.Send(msg)
				}

				columns = []string{
					"idxNm", "clsprcIdx", "flucTpCd", "cmpprevddIdx", "flucRt",
					"opnprcIdx", "hgprcIdx", "lwprcIdx", "accTrdvol", "accTrdval",
					"mktcap", "updateDate", "market"}

				df, err = utils.RenameColumnTo(df, columns)
				if err != nil {
					log.Fatalln(err)
				}

				buf.Reset()
				bytes = io.Writer(&buf)
				df.WriteJSON(bytes)

				k.Sender.Send("IndexPrice", buf.Bytes())
			}

		}),
		gocron.WithTags("krxIndexPrice"),
	)

	log.Println(j.Tags(), "Initialize")
}

func (k *KrxCrawler) krxPrice() {
	j, _ := k.Scheduler.s.NewJob(
		gocron.CronJob("CRON_TZ=Asia/Seoul 0 18 * * *", false),
		gocron.NewTask(func() {

			currentDate := utils.GetCurrentDate()
			y := utils.ConvDateStringToDate(currentDate).Format("2006")
			m := utils.ConvDateStringToDate(currentDate).Format("01")
			params := url.Values{}
			params.Add("mktId", "ALL")
			params.Add("share", "1")
			params.Add("money", "1")
			params.Add("trdDd", currentDate)
			params.Add("csvxls_isNo", "false")
			params.Add("name", "fileDown")
			params.Add("url", "dbms/MDC/STAT/standard/MDCSTAT01501")

			uri := fmt.Sprintf("http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd?%s", params.Encode())
			req, _ := http.NewRequest("POST", uri, nil)
			client := &http.Client{}
			res, err := client.Do(req)

			if err != nil {
				log.Fatalln(err)
			}

			var code string
			if res.StatusCode == http.StatusOK {
				bytes, _ := io.ReadAll(res.Body)
				code = string(bytes)
			}

			params = url.Values{}
			params.Add("code", code)

			uri = fmt.Sprintf("http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd?%s", params.Encode())
			req, _ = http.NewRequest("POST", uri, nil)
			req.Header.Add("User-Agent", "Mozilla/5.0")
			res, err = client.Do(req)
			if err != nil {
				log.Fatalln(err)
			}

			defer res.Body.Close()

			var body string
			if res.StatusCode == http.StatusOK {
				b, _ := io.ReadAll(res.Body)
				body = utils.ConvertUTF8(b)
			}

			df := dataframe.ReadCSV(strings.NewReader(body))

			columns := []string{"stock_code", "stock_full_nm", "classify", "stock_classify",
				"closing_price", "prepare", "fluctuation_rate",
				"open_price", "high_price", "low_price", "trading_volume", "transaction_amount",
				"market_cap", "number_of_listed_shares"}

			df, err = utils.RenameColumn(df, columns)
			df = utils.AddColumn(df, "update_date", series.String, currentDate)
			if err != nil {
				log.Fatalln(err)
			}

			// s3 upload
			var buf bytes.Buffer
			buf.Reset()
			bytes := io.Writer(&buf)
			df.WriteCSV(bytes)

			prefix := fmt.Sprintf("/krx/stocks_price/%s/%s/%s.csv", y, m, currentDate)
			err = k.awsClient.PutS3File(buf.Bytes(), prefix)

			if err == nil {
				msg := "KRX 주가가격 수집 S3 업로드 완료"
				go k.bot.Send(msg)
			}

			columns = []string{
				"stockCode", "stockFullName", "classify", "stockClassify", "closingPrice",
				"prepare", "fluctuationRate", "openPrice", "highPrice", "lowPrice",
				"tradingVolume", "transactionAmount", "marketCap", "numberOfListedShares",
				"updateDate"}

			df, err = utils.RenameColumnTo(df, columns)
			if err != nil {
				log.Fatalln(err)
			}

			buf.Reset()
			bytes = io.Writer(&buf)
			df.WriteJSON(bytes)

			k.Sender.Send("Price", buf.Bytes())

		}),
		gocron.WithTags("krxPrice"),
	)

	log.Println(j.Tags(), "Initialize")
}
