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
	"regexp"
	"strconv"
	"strings"
	"time"

	"d.core.base/crawler/utils"
	"github.com/PuerkitoBio/goquery"
	"github.com/go-co-op/gocron/v2"
	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
)

type KindCrawler struct {
	*Scheduler
	*Sender
	awsClient *aws.AWSClient
	bot       *bot.Bot
}

func NewKindCrawler(s *Scheduler, sd *Sender, awsClient *aws.AWSClient, b *bot.Bot) *KindCrawler {
	return &KindCrawler{
		Scheduler: s,
		Sender:    sd,
		awsClient: awsClient,
		bot:       b,
	}
}

func (k *KindCrawler) Init() {
	k.kindDisclosure()
}

func (k *KindCrawler) kindDisclosure() {
	j, _ := k.Scheduler.s.NewJob(
		gocron.CronJob("CRON_TZ=Asia/Seoul */5 * * * *", false),
		// gocron.DurationJob(time.Second),
		gocron.NewTask(func() {
			currentDate := utils.GetCurrentDate()
			currentDateTime, prevDatetime := utils.GetPreviousDateTime("5m")
			// currentDateTime, prevDatetime = fmt.Sprintf("%s%s", prevDatetime[0:11], "0"), fmt.Sprintf("%s%s", currentDateTime[0:11], "0")
			// log.Println(currentDateTime, prevDatetime)
			// 시장조치(0313:불성실공시, 0311:매매거래정지, 0350:관리종목, 0356:투자주의환기종목)
			cls := [4]string{"0313", "0311", "0350", "0356"}
			// cls := [1]string{"0313"}
			for _, c := range cls {
				params := url.Values{}
				params.Add("method", "searchDetailsSub")
				params.Add("currentPageSize", "100")
				params.Add("pageIndex", "1")
				params.Add("orderMode", "1")
				params.Add("orderStat", "D")
				params.Add("forward", "details_sub")
				params.Add("disclosureType01", "")
				params.Add("disclosureType02", c)
				params.Add("disclosureType03", "")
				params.Add("disclosureType04", "")
				params.Add("disclosureType05", "")
				params.Add("disclosureType06", "")
				params.Add("disclosureType07", "")
				params.Add("disclosureType08", "")
				params.Add("disclosureType09", "")
				params.Add("disclosureType10", "")
				params.Add("disclosureType11", "")
				params.Add("disclosureType13", "")
				params.Add("disclosureType14", "")
				params.Add("disclosureType20", "")
				params.Add("pDisclosureType01", "")
				params.Add("pDisclosureType02", c)
				params.Add("pDisclosureType03", "")
				params.Add("pDisclosureType04", "")
				params.Add("pDisclosureType05", "")
				params.Add("pDisclosureType06", "")
				params.Add("pDisclosureType07", "")
				params.Add("pDisclosureType08", "")
				params.Add("pDisclosureType09", "")
				params.Add("pDisclosureType10", "")
				params.Add("pDisclosureType11", "")
				params.Add("pDisclosureType13", "")
				params.Add("pDisclosureType14", "")
				params.Add("pDisclosureType20", "")
				params.Add("searchCodeType", "")
				params.Add("repIsuSrtCd", "")
				params.Add("allRepIsuSrtCd", "")
				params.Add("oldSearchCorpName", "")
				params.Add("disclosureType", "")
				params.Add("disTypevalue", "")
				params.Add("reportNm", "")
				params.Add("reportCd", "")
				params.Add("searchCorpName", "")
				params.Add("business", "")
				params.Add("marketType", "")
				params.Add("settlementMonth", "")
				params.Add("securities", "")
				params.Add("submitOblgNm", "")
				params.Add("enterprise", "")
				params.Add("fromDate", currentDate)
				params.Add("toDate", currentDate)
				params.Add("reportNmTemp", "")
				params.Add("reportNmPop", "")
				params.Add("disclosureTypeArr02", c)

				uri := fmt.Sprintf("https://kind.krx.co.kr/disclosure/details.do?%s", params.Encode())
				req, _ := http.NewRequest("POST", uri, nil)
				req.Header.Add("User-Agent", "Mozilla/5.0")

				client := &http.Client{}
				res, err := client.Do(req)

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

				doc_tr := doc.Find("tbody > tr")
				r := regexp.MustCompile(`[-]?\d[\d,]*[\.]?[\d{2}]*`)

				var elems [][]string
				doc_tr.Each(func(i int, s *goquery.Selection) {

					var td_elems []string
					td := s.Find("td")
					td.Each(func(i int, s *goquery.Selection) {
						text := strings.Trim(s.Text(), " ")
						td_elems = append(td_elems, text)

						if i == 1 {
							t, _ := time.Parse("2006-01-02 15:04", text)
							td_elems[i] = t.Format("200601021504")
						}

						attr, exists := s.Find("a").Attr("onclick")
						if exists {
							if i == 2 {
								n, _ := strconv.Atoi(r.FindAllString(attr, -1)[0])
								code := fmt.Sprintf("%06d", n)
								td_elems = append(td_elems, code)
							}
							if i == 3 {
								td_elems = append(td_elems, r.FindAllString(attr, -1)[0])
							}
						}
					})
					elems = append(elems, td_elems)
				})

				if elems[0][0] == "조회된 결과값이 없습니다." {
					return
				}

				var contents []map[string]interface{}

				for _, v := range elems {
					content := map[string]interface{}{
						"disclosure_time": v[1],
						"stock_nm":        v[2],
						"reason":          v[4],
						"rcpt_no":         v[5],
						"stock_code":      v[3],
						"disclosure_data": c,
						"doc_no":          "",
						"detail_url":      "",
						"detail":          "",
					}

					disclosureTime := v[1]
					if prevDatetime <= disclosureTime && disclosureTime < currentDateTime {
						// if true {

						params := url.Values{}
						params.Add("acptno", content["rcpt_no"].(string))

						uri := fmt.Sprintf("https://kind.krx.co.kr/common/disclsviewer.do?method=search&%s", params.Encode())
						req, _ := http.NewRequest("GET", uri, nil)
						req.Header.Add("User-Agent", "Mozilla/5.0")

						res, err := client.Do(req)

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

						doc_option := doc.Find("table.viewer-search select#mainDoc > option")
						doc_option.Each(func(i int, s *goquery.Selection) {
							attr, exists := s.Attr("value")
							if exists {
								if attr != "" {
									content["doc_no"] = strings.Split(attr, "|")[0]
								}
							}
						})

						params = url.Values{}
						params.Add("docNo", content["doc_no"].(string))
						uri = fmt.Sprintf("https://kind.krx.co.kr/common/disclsviewer.do?method=searchContents&%s", params.Encode())
						req, _ = http.NewRequest("GET", uri, nil)
						req.Header.Add("User-Agent", "Mozilla/5.0")

						res, err = client.Do(req)

						if res.StatusCode != http.StatusOK {
							return
						}

						if err != nil {
							log.Fatalln(err)
						}

						doc, err = goquery.NewDocumentFromReader(res.Body)
						if err != nil {
							log.Fatal(err)
						}

						doc_script := doc.Find("script")
						doc_script.Each(func(i int, s *goquery.Selection) {
							if i == 4 {
								match, _ := regexp.MatchString("setPath", s.Text())

								if match {
									sl := regexp.MustCompile("https://").FindIndex([]byte(s.Text()))[0]
									el := regexp.MustCompile("htm").FindIndex([]byte(s.Text()))[1]
									detail_uri := s.Text()[sl:el]
									content["detail_url"] = detail_uri
								}
							}
						})

						req, _ = http.NewRequest("GET", content["detail_url"].(string), nil)
						req.Header.Add("User-Agent", "Mozilla/5.0")

						res, err = client.Do(req)

						if res.StatusCode != http.StatusOK {
							return
						}

						if err != nil {
							log.Fatalln(err)
						}

						doc, err = goquery.NewDocumentFromReader(res.Body)
						if err != nil {
							log.Fatal(err)
						}

						doc_table := doc.Find("div.xforms > div")
						// log.Println(doc_table.Html())
						doc_table.Each(func(i int, s *goquery.Selection) {
							s.Each(func(i int, s *goquery.Selection) {
								s.ChildrenFiltered("div").Remove()
								s.ChildrenFiltered("span").Remove()
								table, _ := s.Html()
								content["detail"] = strings.Trim(strings.ReplaceAll(table, "\n", ""), " ")
							})
						})

						contents = append(contents, content)
					}
				}

				df := dataframe.LoadMaps(contents,
					dataframe.DefaultType(series.String),
					dataframe.WithTypes(map[string]series.Type{
						"disclosure_time": series.String,
						"stock_nm":        series.String,
						"reason":          series.String,
						"rcpt_no":         series.String,
						"stock_code":      series.String,
						"disclosure_data": series.String,
						"doc_no":          series.String,
						"detail_url":      series.String,
						"detail":          series.String,
					}))

				columns := []string{
					"disclosureTime", "stockNm", "reason", "rcptNo", "stockCode",
					"docNo", "detailUrl", "detail", "disclosureData"}

				df, err = utils.RenameColumnTo(df, columns)
				if err != nil {
					log.Fatalln(err)
				}

				var buf bytes.Buffer
				buf.Reset()
				bytes := io.Writer(&buf)
				df.WriteJSON(bytes)

				k.Sender.Send("Disclosure", buf.Bytes())
			}
		}),
		gocron.WithTags("kindDisclosure"),
	)

	log.Println(j.Tags(), "Initialize")
}
