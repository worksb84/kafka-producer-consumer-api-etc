package cron

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"d.core.base/crawler/utils"
	"github.com/PuerkitoBio/goquery"
	"github.com/go-gota/gota/dataframe"
)

func (d *DartCrawler) footnote(stockParameter StockParameter) {
	time.Sleep(1500)

	type FootNote struct {
		RcpNo     string `json:"rcpNo" dataframe:"rcpNo"`
		CorpCode  string `json:"corp_code" dataframe:"corp_code"`
		StockCode string `json:"stock_code" dataframe:"stock_code"`
		BsnsYear  string `json:"bsns_year" dataframe:"bsns_year"`
		ReprtCode string `json:"reprt_code" dataframe:"reprt_code"`
		Remark1   string `json:"remark1" dataframe:"remark1"`
		Remark2   string `json:"remark2" dataframe:"remark2"`
		Remark3   string `json:"remark3" dataframe:"remark3"`
		Remark4   string `json:"remark4" dataframe:"remark4"`
	}

	fn := FootNote{
		RcpNo:     stockParameter.RceptNo,
		CorpCode:  stockParameter.CorpCode,
		StockCode: stockParameter.StockCode,
		BsnsYear:  stockParameter.BsnsYear,
		ReprtCode: stockParameter.ReprtCode,
		Remark1:   "",
		Remark2:   "",
		Remark3:   "",
		Remark4:   "",
	}

	params := url.Values{}
	params.Add("rcpNo", fn.RcpNo)

	uri := fmt.Sprintf("http://dart.fss.or.kr/dsaf001/main.do?%s", params.Encode())
	req, _ := http.NewRequest("GET", uri, nil)
	req.Header.Add("User-Agent", "Mozilla/5.0")

	client := &http.Client{}
	res, err := client.Do(req)
	if err != nil {
		log.Fatalln(err)
	}

	if res.StatusCode != http.StatusOK {
		return
	}

	if err != nil {
		log.Fatalln(err)
	}

	doc, err := goquery.NewDocumentFromReader(res.Body)
	if err != nil {
		log.Fatal(err)
	}

	// log.Println(doc)
	doc_script := doc.Find("script")
	doc_script.Each(func(i int, s *goquery.Selection) {
		tr, _ := regexp.Compile("재무제표 주석")
		if tr.MatchString(s.Text()) {
			scriptLine := strings.Split(s.Text(), "\n")
			type ScriptStruct struct {
				RcpNo  string
				DcmNo  string
				EleId  string
				Offset string
				Length string
				Dtd    string
				Title  string
			}
			f := func(title string, line string, idx int, script []string) (ScriptStruct, bool) {
				tr, _ := regexp.Compile(title)
				nr := regexp.MustCompile("[^0-9]")
				cr := regexp.MustCompile("[^0-9|a-z|A-Z|.]")
				if tr.MatchString(line) {
					scriptStruct := ScriptStruct{
						RcpNo:  nr.ReplaceAllString(strings.Split(script[idx+2], "=")[1], ""),
						DcmNo:  nr.ReplaceAllString(strings.Split(script[idx+3], "=")[1], ""),
						EleId:  nr.ReplaceAllString(strings.Split(script[idx+4], "=")[1], ""),
						Offset: nr.ReplaceAllString(strings.Split(script[idx+5], "=")[1], ""),
						Length: nr.ReplaceAllString(strings.Split(script[idx+6], "=")[1], ""),
						Dtd:    cr.ReplaceAllString(strings.Split(script[idx+7], "=")[1], ""),
						Title:  strings.Trim(title, " "),
					}
					return scriptStruct, true
				}
				return ScriptStruct{}, false
			}

			var d ScriptStruct
			var b bool

			detailNote := func(scriptStruct ScriptStruct) string {
				params := url.Values{}
				params.Add("rcpNo", scriptStruct.RcpNo)
				params.Add("dcmNo", scriptStruct.DcmNo)
				params.Add("eleId", scriptStruct.EleId)
				params.Add("offset", scriptStruct.Offset)
				params.Add("length", scriptStruct.Length)
				params.Add("dtd", scriptStruct.Dtd)

				uri := fmt.Sprintf("http://dart.fss.or.kr/report/viewer.do?%s", params.Encode())
				req, _ := http.NewRequest("GET", uri, nil)
				req.Header.Add("User-Agent", "Mozilla/5.0")

				client := &http.Client{}
				res, err := client.Do(req)
				if err != nil {
					log.Fatalln(err)
				}

				defer res.Body.Close()

				doc, err := goquery.NewDocumentFromReader(res.Body)
				if err != nil {
					log.Fatal(err)
				}

				// log.Println(doc)
				doc_body := doc.Find("html")

				doc_body.ChildrenFiltered("head").Remove()
				body, _ := doc_body.Html()
				body = strings.Trim(body, "\n")
				return body
			}

			for i, v := range scriptLine {
				d, b = f(" 연결재무제표 주석", v, i, scriptLine)
				if b {
					// log.Println(d)
					fn.Remark1 = detailNote(d)
				}
				d, b = f(" 재무제표 주석", v, i, scriptLine)
				if b {
					// log.Println(d)
					fn.Remark2 = detailNote(d)
				}
				d, b = f(" 기타 재무에 관한 사항", v, i, scriptLine)
				if b {
					// log.Println(d)
					fn.Remark3 = detailNote(d)
				}
				d, b = f(" 주주에 관한 사항", v, i, scriptLine)
				if b {
					// log.Println(d)
					fn.Remark4 = detailNote(d)
				}
			}
		}

	})
	fns := []FootNote{}
	fns = append(fns, fn)
	df := dataframe.LoadStructs(fns)

	// s3 upload
	var buf bytes.Buffer
	buf.Reset()
	bytes := io.Writer(&buf)
	df.WriteCSV(bytes)

	prefix := fmt.Sprintf("/dart/footnote/%s/%s/%s.csv", stockParameter.Year, stockParameter.Month, stockParameter.CorpCode)
	err = d.awsClient.PutS3File(buf.Bytes(), prefix)

	if err == nil {
		msg := fmt.Sprintf("다트 [%s]%s 재무제표 주석 S3 업로드 완료", stockParameter.StockCode, stockParameter.StockName)
		go d.bot.Send(msg)
	}

	columns := []string{
		"rcpNo", "corpCode", "stockCode", "bsnsYear", "reprtCode",
		"remark1", "remark2", "remark3", "remark4",
	}

	df, err = utils.RenameColumnTo(df, columns)
	if err != nil {
		log.Fatalln(err)
	}

	buf.Reset()
	bytes = io.Writer(&buf)
	df.WriteJSON(bytes)

	d.Sender.Send("Footnote", buf.Bytes())

	// 카프카 전송

}
