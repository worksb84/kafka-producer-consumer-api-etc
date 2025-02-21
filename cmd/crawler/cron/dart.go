package cron

import (
	"archive/zip"
	"bot"
	"bytes"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"sync"
	"time"

	"aws"

	"d.core.base/crawler/utils"
	"github.com/go-co-op/gocron/v2"
	"github.com/go-gota/gota/dataframe"
	"golang.org/x/exp/slices"
)

type DartCrawler struct {
	*Scheduler
	*Sender
	apiKeyList []ApiKey
	sync.Mutex
	awsClient *aws.AWSClient
	bot       *bot.Bot
}

type ApiKey struct {
	apiKey   string
	useCount int32
}

type Report struct {
	Status  string          `json:"status"`
	Message string          `json:"message"`
	List    []ReportElement `json:"list"`
}

type StockParameter struct {
	RceptNo   string
	CorpCode  string
	BsnsYear  string
	StockCode string
	ReprtCode string
	Year      string
	Month     string
	StockName string
}

type Company struct {
	Status      string `json:"status" dataframe:"status"`
	Message     string `json:"message" dataframe:"message"`
	CorpCode    string `json:"corp_code" dataframe:"corp_code"`
	CorpName    string `json:"corp_name" dataframe:"corp_name"`
	CorpNameEng string `json:"corp_name_eng" dataframe:"corp_name_eng"`
	StockName   string `json:"stock_name" dataframe:"stock_name"`
	StockCode   string `json:"stock_code" dataframe:"stock_code"`
	CeoNm       string `json:"ceo_nm" dataframe:"ceo_nm"`
	CorpCls     string `json:"corp_cls" dataframe:"corp_cls"`
	JurirNo     string `json:"jurir_no" dataframe:"jurir_no"`
	BizrNo      string `json:"bizr_no" dataframe:"bizr_no"`
	Adres       string `json:"adres" dataframe:"adres"`
	HmUrl       string `json:"hm_url" dataframe:"hm_url"`
	IrUrl       string `json:"ir_url" dataframe:"ir_url"`
	PhnNo       string `json:"phn_no" dataframe:"phn_no"`
	FaxNo       string `json:"fax_no" dataframe:"fax_no"`
	IndutyCode  string `json:"induty_code" dataframe:"induty_code"`
	EstDt       string `json:"est_dt" dataframe:"est_dt"`
	AccMt       string `json:"acc_mt" dataframe:"acc_mt"`
	UpdateDate  string `json:"update_date" dataframe:"update_date"`
}

type ReportElement struct {
	Rcept_no       string `json:"rcept_no"`
	Reprt_code     string `json:"reprt_code"`
	Bsns_year      string `json:"bsns_year"`
	Corp_code      string `json:"corp_code"`
	Sj_div         string `json:"sj_div"`
	Sj_nm          string `json:"sj_nm"`
	Account_id     string `json:"account_id"`
	Account_nm     string `json:"account_nm"`
	Account_detail string `json:"account_detail"`
	Thstrm_nm      string `json:"thstrm_nm"`
	Thstrm_amount  string `json:"thstrm_amount"`
	Frmtrm_nm      string `json:"frmtrm_nm"`
	Frmtrm_amount  string `json:"frmtrm_amount"`
	Ord            string `json:"ord"`
	Currency       string `json:"currency"`
}

type CorpCode struct {
	CorpCode   string `xml:"corp_code" dataframe:"corp_code"`
	CorpName   string `xml:"corp_name" dataframe:"corp_name"`
	StockCode  string `xml:"stock_code" dataframe:"stock_code"`
	ModifyDate string `xml:"modify_date" dataframe:"modify_date"`
}
type CorpCodes struct {
	Result []CorpCode `xml:"list"`
}

func initApikeyList() []ApiKey {
	var apiKeyList []ApiKey
	apiKeyList = append(apiKeyList, ApiKey{apiKey: "669db892f92b111a20dbfa710c49814e63b971b2", useCount: 0})
	apiKeyList = append(apiKeyList, ApiKey{apiKey: "6a8f6b8dda4061efbe279a904e490d0be953f7ca", useCount: 0})
	apiKeyList = append(apiKeyList, ApiKey{apiKey: "887f7bbc46ce3f978183af56adda787e189fb349", useCount: 0})
	apiKeyList = append(apiKeyList, ApiKey{apiKey: "01ce64c5b1f3faee1b85ca2a78d00706ed07cf9b", useCount: 0})
	apiKeyList = append(apiKeyList, ApiKey{apiKey: "a6178a344656ef1a74e5f5a8f8c8d259fc4bbbe0", useCount: 0})
	apiKeyList = append(apiKeyList, ApiKey{apiKey: "82dc3a5e61fbb35df4aa25b63beb37784df9bf56", useCount: 0})
	apiKeyList = append(apiKeyList, ApiKey{apiKey: "482cf1af97f9220910befb8c2ab507d0df96da79", useCount: 0})

	return apiKeyList
}

func NewDartCrawler(s *Scheduler, sd *Sender, awsClient *aws.AWSClient, b *bot.Bot) *DartCrawler {

	return &DartCrawler{
		Scheduler:  s,
		Sender:     sd,
		apiKeyList: initApikeyList(),
		awsClient:  awsClient,
		bot:        b,
	}
}

func (d *DartCrawler) initializeApikeyList() {
	j, _ := d.Scheduler.s.NewJob(
		gocron.CronJob("CRON_TZ=Asia/Seoul 0 0 * * *", false),
		gocron.NewTask(func() {
			d.apiKeyList = initApikeyList()
		}),
		gocron.WithTags("initializeApikeyList"),
	)

	log.Println(j.Tags(), "InitializeApikeyList")
}

func (d *DartCrawler) availableApiKey() (string, error) {
	d.Lock()
	sort.Slice(d.apiKeyList, func(i, j int) bool {
		return d.apiKeyList[i].useCount < d.apiKeyList[j].useCount
	})
	d.Unlock()
	if d.apiKeyList[0].useCount >= 20000 {
		return "", errors.New("")
	}
	return d.apiKeyList[0].apiKey, nil
}

func (d *DartCrawler) increaseUseCount(apiKey string) {
	d.Lock()
	idx := slices.IndexFunc(d.apiKeyList, func(c ApiKey) bool {
		return c.apiKey == apiKey
	})

	d.apiKeyList[idx].useCount += 1
	d.Unlock()
}

func (d *DartCrawler) Init() {
	d.initializeApikeyList()
	d.company()
	d.report()
}

func (d *DartCrawler) company() {
	j, _ := d.Scheduler.s.NewJob(
		gocron.CronJob("CRON_TZ=Asia/Seoul 50 23 * * *", false),
		gocron.NewTask(func() {

			params := url.Values{}
			availableApiKey, _ := d.availableApiKey()
			params.Add("crtfc_key", availableApiKey)

			uri := fmt.Sprintf("https://opendart.fss.or.kr/api/corpCode.xml?%s", params.Encode())
			req, _ := http.NewRequest("GET", uri, nil)

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

			zipReader, err := zip.NewReader(bytes.NewReader(body), int64(len(body)))
			if err != nil {
				log.Fatal(err)
			}

			f := func(zf *zip.File) ([]byte, error) {
				f, err := zf.Open()
				if err != nil {
					return nil, err
				}
				defer f.Close()
				return io.ReadAll(f)
			}

			var cB []byte

			for _, zF := range zipReader.File {
				uZF, err := f(zF)
				if err != nil {
					log.Println(err)
					continue
				}

				cB = uZF
			}

			var corpCodes CorpCodes

			xml.Unmarshal(cB, &corpCodes)

			corpCodeFilter := func(c []CorpCode) {
				var t []CorpCode
				for _, v := range c {
					corpCodes.Result = t
					r, _ := regexp.Compile("코크렙|스팩|금융|증권|손해보험|현대해상|한화생명|한국자산신탁|카드|화재|펀드|투자자문|캐피탈|은행|유한회사|리츠|기업인수목적|투자회사")

					if v.StockCode != " " && !r.MatchString(v.CorpName) {
						t = append(t, v)
					}
				}
				corpCodes.Result = t
			}

			corpCodeFilter(corpCodes.Result)
			currentDate := utils.GetCurrentDate()
			var companies []Company
			for _, v := range corpCodes.Result {
				params = url.Values{}
				availableApiKey, _ = d.availableApiKey()
				params.Add("crtfc_key", availableApiKey)
				params.Add("corp_code", v.CorpCode)

				uri := fmt.Sprintf("https://opendart.fss.or.kr/api/company.json?%s", params.Encode())
				req, _ := http.NewRequest("GET", uri, nil)

				client := &http.Client{}
				res, err := client.Do(req)
				if err != nil {
					log.Fatalln(err)
				}
				defer res.Body.Close()

				var body []byte
				if res.StatusCode == http.StatusOK {
					b, _ := io.ReadAll(res.Body)
					body = b
				}
				d.increaseUseCount(availableApiKey)

				var company Company
				json.Unmarshal(body, &company)

				if company.Status == "000" {
					company.UpdateDate = currentDate
					companies = append(companies, company)
				}

				time.Sleep(500)
			}

			df := dataframe.LoadStructs(companies)
			df = df.Select([]string{"corp_code", "corp_name", "corp_name_eng", "stock_name", "stock_code", "ceo_nm", "corp_cls",
				"jurir_no", "bizr_no", "adres", "hm_url", "ir_url", "phn_no", "fax_no", "induty_code", "est_dt", "acc_mt", "update_date"})

			var buf bytes.Buffer
			bytes := io.Writer(&buf)
			df.WriteCSV(bytes)

			key := fmt.Sprintf("dart/company/%s.csv", currentDate)
			err = d.awsClient.PutS3File(buf.Bytes(), key)
			if err == nil {
				go d.bot.Send("다트 회사 정보 S3 업로드 완료")
			}

		}),
		gocron.WithTags("Company"),
	)

	log.Println(j.Tags(), "Company")

}
