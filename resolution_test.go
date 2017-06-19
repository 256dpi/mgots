package mgots

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBasicResolutionSplitAndJoin(t *testing.T) {
	ts := parseTime("Jul 15 15:15:15")

	table := []struct {
		r Resolution
		s string
		k string
		t string
	}{
		{r: OneMinuteOf60Seconds, s: "Jul 15 15:15:00", k: "15", t: "Jul 15 15:15:15"},
		{r: OneHourOf60Minutes, s: "Jul 15 15:00:00", k: "15", t: "Jul 15 15:15:00"},
		{r: OneDayOf24Hours, s: "Jul 15 00:00:00", k: "15", t: "Jul 15 15:00:00"},
		{r: OneMonthOfUpTo31Days, s: "Jul  1 00:00:00", k: "15", t: "Jul 15 00:00:00"},
		{r: OneHourOf3600Seconds, s: "Jul 15 15:00:00", k: "915", t: "Jul 15 15:15:15"},
		{r: OneDayOf1440Minutes, s: "Jul 15 00:00:00", k: "915", t: "Jul 15 15:15:00"},
	}

	for _, e := range table {
		start, key := e.r.Split(ts)
		assert.Equal(t, e.s, start.Format(time.Stamp))
		assert.Equal(t, e.k, key)

		ts2 := e.r.Join(start, key)
		assert.Equal(t, e.t, ts2.Format(time.Stamp))
	}
}

func TestBasicResolutionSetSize(t *testing.T) {
	table := []struct {
		r Resolution
		n int
	}{
		{r: OneMinuteOf60Seconds, n: 60},
		{r: OneHourOf60Minutes, n: 60},
		{r: OneDayOf24Hours, n: 24},
		{r: OneMonthOfUpTo31Days, n: 31},
		{r: OneHourOf3600Seconds, n: 3600},
		{r: OneDayOf1440Minutes, n: 1440},
	}

	for _, e := range table {
		assert.Equal(t, e.n, e.r.SetSize())
	}
}

func TestBasicResolutionSetRange(t *testing.T) {
	table := []struct {
		r  Resolution
		rs string
		re string
		tf string
		tc string
		tl string
		l  int
	}{
		{r: OneMinuteOf60Seconds, rs: "Jul 15 15:15:15", re: "Jul 15 16:16:15", l: 62, tf: "Jul 15 15:15:00", tc: "Jul 15 15:46:00", tl: "Jul 15 16:16:00"},
		{r: OneHourOf60Minutes, rs: "Jul 15 15:15:15", re: "Jul 16 16:15:15", l: 26, tf: "Jul 15 15:00:00", tc: "Jul 16 04:00:00", tl: "Jul 16 16:00:00"},
		{r: OneDayOf24Hours, rs: "Jul 15 15:15:15", re: "Aug 16 15:15:15", l: 33, tf: "Jul 15 00:00:00", tc: "Jul 31 00:00:00", tl: "Aug 16 00:00:00"},
		{r: OneMonthOfUpTo31Days, rs: "Jul 15 15:15:15", re: "Sep 15 15:15:15", l: 3, tf: "Jul  1 00:00:00", tc: "Aug  1 00:00:00", tl: "Sep  1 00:00:00"},
		{r: OneHourOf3600Seconds, rs: "Jul 15 15:15:15", re: "Jul 16 16:15:15", l: 26, tf: "Jul 15 15:00:00", tc: "Jul 16 04:00:00", tl: "Jul 16 16:00:00"},
		{r: OneDayOf1440Minutes, rs: "Jul 15 15:15:15", re: "Aug 16 15:15:15", l: 33, tf: "Jul 15 00:00:00", tc: "Jul 31 00:00:00", tl: "Aug 16 00:00:00"},
	}

	for _, e := range table {
		list := e.r.SetRange(parseTime(e.rs), parseTime(e.re))
		assert.Len(t, list, e.l)
		assert.Equal(t, e.tf, list[0].Format(time.Stamp), list[0].Format(time.Stamp))
		assert.Equal(t, e.tc, list[e.l/2].Format(time.Stamp), list[e.l/2].Format(time.Stamp))
		assert.Equal(t, e.tl, list[e.l-1].Format(time.Stamp), list[e.l-1].Format(time.Stamp))
	}
}

func TestBasicResolutionSampleRange(t *testing.T) {
	table := []struct {
		r  Resolution
		rs string
		re string
		tf string
		tc string
		tl string
		l  int
	}{
		{r: OneMinuteOf60Seconds, rs: "Jul 15 15:15:15", re: "Jul 15 15:16:16", l: 61, tf: "Jul 15 15:15:15", tc: "Jul 15 15:15:45", tl: "Jul 15 15:16:15"},
		{r: OneHourOf60Minutes, rs: "Jul 15 15:15:15", re: "Jul 15 16:16:16", l: 62, tf: "Jul 15 15:15:00", tc: "Jul 15 15:46:00", tl: "Jul 15 16:16:00"},
		{r: OneDayOf24Hours, rs: "Jul 15 15:15:15", re: "Jul 16 16:16:16", l: 26, tf: "Jul 15 15:00:00", tc: "Jul 16 04:00:00", tl: "Jul 16 16:00:00"},
		{r: OneMonthOfUpTo31Days, rs: "Jul 15 15:15:15", re: "Aug 16 16:16:16", l: 33, tf: "Jul 15 00:00:00", tc: "Jul 31 00:00:00", tl: "Aug 16 00:00:00"},
		{r: OneHourOf3600Seconds, rs: "Jul 15 15:15:15", re: "Jul 15 15:16:16", l: 61, tf: "Jul 15 15:15:15", tc: "Jul 15 15:15:45", tl: "Jul 15 15:16:15"},
		{r: OneDayOf1440Minutes, rs: "Jul 15 15:15:15", re: "Jul 15 16:16:16", l: 62, tf: "Jul 15 15:15:00", tc: "Jul 15 15:46:00", tl: "Jul 15 16:16:00"},
	}

	for _, e := range table {
		list := e.r.SampleRange(parseTime(e.rs), parseTime(e.re))
		assert.Len(t, list, e.l)
		assert.Equal(t, e.tf, list[0].Format(time.Stamp), list[0].Format(time.Stamp))
		assert.Equal(t, e.tc, list[e.l/2].Format(time.Stamp), list[e.l/2].Format(time.Stamp))
		assert.Equal(t, e.tl, list[e.l-1].Format(time.Stamp), list[e.l-1].Format(time.Stamp))
	}
}
