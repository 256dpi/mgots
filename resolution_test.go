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

	for i, e := range table {
		start, key := e.r.Split(ts)
		assert.Equal(t, e.s, start.Format(time.Stamp), "%d", i)
		assert.Equal(t, e.k, key, "%d", i)

		ts2 := e.r.Join(start, key)
		assert.Equal(t, e.t, ts2.Format(time.Stamp), "%d", i)
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

	for i, e := range table {
		assert.Equal(t, e.n, e.r.SetSize(), "%d", i)
	}
}

func TestBasicResolutionSetTimestamps(t *testing.T) {
	table := []struct {
		res    Resolution
		start  string
		end    string
		len    int
		first  string
		middle string
		last   string
	}{
		{
			res:    OneMinuteOf60Seconds,
			start:  "Jul 15 15:15:15",
			end:    "Jul 15 16:16:15",
			len:    62,
			first:  "Jul 15 15:15:00",
			middle: "Jul 15 15:46:00",
			last:   "Jul 15 16:16:00",
		},
		{
			res:    OneHourOf60Minutes,
			start:  "Jul 15 15:15:15",
			end:    "Jul 16 16:15:15",
			len:    26,
			first:  "Jul 15 15:00:00",
			middle: "Jul 16 04:00:00",
			last:   "Jul 16 16:00:00",
		},
		{res: OneDayOf24Hours,
			start:  "Jul 15 15:15:15",
			end:    "Aug 16 15:15:15",
			len:    33,
			first:  "Jul 15 00:00:00",
			middle: "Jul 31 00:00:00",
			last:   "Aug 16 00:00:00",
		},
		{
			res:    OneMonthOfUpTo31Days,
			start:  "Jul 15 15:15:15",
			end:    "Sep 15 15:15:15",
			len:    3,
			first:  "Jul  1 00:00:00",
			middle: "Aug  1 00:00:00",
			last:   "Sep  1 00:00:00",
		},
		{
			res:    OneHourOf3600Seconds,
			start:  "Jul 15 15:15:15",
			end:    "Jul 16 16:15:15",
			len:    26,
			first:  "Jul 15 15:00:00",
			middle: "Jul 16 04:00:00",
			last:   "Jul 16 16:00:00",
		},
		{
			res:    OneDayOf1440Minutes,
			start:  "Jul 15 15:15:15",
			end:    "Aug 16 15:15:15",
			len:    33,
			first:  "Jul 15 00:00:00",
			middle: "Jul 31 00:00:00",
			last:   "Aug 16 00:00:00",
		},
	}

	for i, e := range table {
		list := e.res.SetTimestamps(parseTime(e.start), parseTime(e.end))
		assert.Len(t, list, e.len, "%d", i)
		assert.Equal(t, e.first, list[0].Format(time.Stamp), "%d", i)
		assert.Equal(t, e.middle, list[e.len/2].Format(time.Stamp), "%d", i)
		assert.Equal(t, e.last, list[e.len-1].Format(time.Stamp), "%d", i)
	}
}

func TestBasicResolutionSampleTimestamps(t *testing.T) {
	table := []struct {
		res    Resolution
		start  string
		end    string
		len    int
		first  string
		middle string
		last   string
	}{
		{
			res:    OneMinuteOf60Seconds,
			start:  "Jul 15 15:15:15",
			end:    "Jul 15 15:16:16",
			len:    62,
			first:  "Jul 15 15:15:15",
			middle: "Jul 15 15:15:46",
			last:   "Jul 15 15:16:16",
		},
		{
			res:    OneHourOf60Minutes,
			start:  "Jul 15 15:15:15",
			end:    "Jul 15 16:16:16",
			len:    62,
			first:  "Jul 15 15:15:00",
			middle: "Jul 15 15:46:00",
			last:   "Jul 15 16:16:00",
		},
		{
			res:    OneDayOf24Hours,
			start:  "Jul 15 15:15:15",
			end:    "Jul 16 16:16:16",
			len:    26,
			first:  "Jul 15 15:00:00",
			middle: "Jul 16 04:00:00",
			last:   "Jul 16 16:00:00",
		},
		{
			res:    OneMonthOfUpTo31Days,
			start:  "Jul 15 15:15:15",
			end:    "Aug 16 16:16:16",
			len:    33,
			first:  "Jul 15 00:00:00",
			middle: "Jul 31 00:00:00",
			last:   "Aug 16 00:00:00",
		},
		{
			res:    OneHourOf3600Seconds,
			start:  "Jul 15 15:15:15",
			end:    "Jul 15 15:16:16",
			len:    62,
			first:  "Jul 15 15:15:15",
			middle: "Jul 15 15:15:46",
			last:   "Jul 15 15:16:16",
		},
		{
			res:    OneDayOf1440Minutes,
			start:  "Jul 15 15:15:15",
			end:    "Jul 15 16:16:16",
			len:    62,
			first:  "Jul 15 15:15:00",
			middle: "Jul 15 15:46:00",
			last:   "Jul 15 16:16:00",
		},
	}

	for i, e := range table {
		list := e.res.SampleTimestamps(parseTime(e.start), parseTime(e.end))
		assert.Len(t, list, e.len, "%d", i)
		assert.Equal(t, e.first, list[0].Format(time.Stamp), "%d", i)
		assert.Equal(t, e.middle, list[e.len/2].Format(time.Stamp), "%d", i)
		assert.Equal(t, e.last, list[e.len-1].Format(time.Stamp), "%d", i)
	}
}
