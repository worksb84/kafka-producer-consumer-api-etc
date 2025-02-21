package utils

import (
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
	"golang.org/x/text/encoding/korean"
	"golang.org/x/text/transform"
)

func IndexOf(element string, data []string) int {
	for k, v := range data {
		if element == v {
			return k
		}
	}
	return -1 //not found.
}

func GetCurrentDate() string {
	t := time.Now()
	return t.Format("20060102")
}

func ConvDateFormat(d string) string {
	t, _ := time.Parse("20060102", d)
	return t.Format("20060102")
}

func ConvDateStringToDate(d string) time.Time {
	t, _ := time.Parse("20060102", d)
	return t
}

func GetPreviousDateTime(minutes string) (string, string) {
	p, _ := time.ParseDuration(minutes)
	t := time.Now()
	return t.Format("200601021504"), t.Add(-p).Format("200601021504")
}

func ConvertUTF8(b []byte) string {
	r, _, _ := transform.String(korean.EUCKR.NewDecoder(), string(b))
	return r
}

func RenameColumn(df dataframe.DataFrame, newName []string) (dataframe.DataFrame, error) {
	index := 0
	newNameLength := len(newName)
	oldNameLength := len(df.Names())

	if newNameLength != oldNameLength {
		return dataframe.DataFrame{}, errors.New("")
	}
	var f func(df dataframe.DataFrame, newName []string, oldName []string) (dataframe.DataFrame, error)

	f = func(df dataframe.DataFrame, newName []string, oldName []string) (dataframe.DataFrame, error) {
		if index == oldNameLength {
			return df, nil
		}

		df = df.Rename(newName[index], oldName[index])
		index++

		return f(df, newName, df.Names())
	}

	return f(df, newName, df.Names())
}

func RenameColumnTo(df dataframe.DataFrame, newName []string) (dataframe.DataFrame, error) {
	index := 0
	var oldIndex int
	var newIndex int
	newNameLength := len(newName)
	oldNameLength := len(df.Names())

	if newNameLength != oldNameLength {
		return dataframe.DataFrame{}, errors.New("")
	}
	var f func(df dataframe.DataFrame, newName []string, oldName []string) (dataframe.DataFrame, error)

	f = func(df dataframe.DataFrame, newName []string, oldName []string) (dataframe.DataFrame, error) {
		var ntmp []string
		var otmp []string

		if index == oldNameLength {
			return df, nil
		}

		for _, v := range newName {
			ntmp = append(ntmp, strings.ToLower(v))
		}

		for _, v := range oldName {
			oldNameReplace := strings.ReplaceAll(v, "_", "")
			otmp = append(otmp, strings.ToLower(oldNameReplace))
		}

		oldIndex = IndexOf(otmp[index], otmp)
		if oldIndex == -1 {
			return df, nil
		}

		newIndex = IndexOf(otmp[oldIndex], ntmp)
		if newIndex == -1 {
			return df, nil
		}

		// if oldNameReplace != strings.ToLower(newName[index]) {
		// 	return df, nil
		// }

		new := newName[newIndex]
		old := oldName[oldIndex]

		df = df.Rename(new, old)
		index++

		return f(df, newName, df.Names())
	}

	return f(df, newName, df.Names())
}

func MakeRange(min, max int) []int {
	a := make([]int, max-min+1)
	for i := range a {
		a[i] = min + i
	}
	return a
}

func AddColumn(df dataframe.DataFrame, column string, seriesType series.Type, value interface{}) dataframe.DataFrame {
	a := make([]interface{}, df.Nrow())

	for i := range a {
		a[i] = value
	}

	return df.Mutate(
		series.New(a, seriesType, column),
	)
}

func PadZeroColumn(df dataframe.DataFrame, column string, seriesType series.Type, value series.Series, padZero int) dataframe.DataFrame {
	a := make([]interface{}, df.Nrow())
	for i := range a {
		v, err := value.Elem(i).Int()
		if err != nil {
			log.Fatalln(err)
		}
		if padZero == 6 {
			a[i] = fmt.Sprintf("%06d", v)
		} else if padZero == 8 {
			a[i] = fmt.Sprintf("%08d", v)
		}
	}

	return df.Mutate(
		series.New(a, seriesType, column),
	)
}

func AddDayColumn(df dataframe.DataFrame, from_column string, to_column string, seriesType series.Type, value [3]int) dataframe.DataFrame {
	a := make([]interface{}, df.Nrow())
	f := df.Col(from_column)
	for i := range a {
		v := f.Elem(i).String()
		t, err := strconv.Atoi(ConvDateStringToDate(v).AddDate(value[0], value[1], value[2]).Format("20060102"))
		if err != nil {
			log.Fatalln(err)
		}
		a[i] = t
	}

	return df.Mutate(
		series.New(a, seriesType, to_column),
	)
}

func ChangeColumnType(df dataframe.DataFrame, column string, delimeter string, seriesType series.Type) dataframe.DataFrame {
	a := make([]interface{}, df.Nrow())
	t := df.Col(column)

	for i := range a {
		e := strings.ReplaceAll(t.Elem(i).String(), delimeter, "")
		switch seriesType {
		case series.String:
			a[i] = string(e)
		case series.Int:
			if n, err := strconv.ParseInt(e, 10, 32); err == nil {
				a[i] = n
			}
		case series.Float:
			if f, err := strconv.ParseFloat(e, 32); err == nil {
				a[i] = f
			}
		default:
			a[i] = e
		}
	}

	return df.Mutate(
		series.New(a, seriesType, column),
	)
}
