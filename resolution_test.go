package mgots

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBasicResolution(t *testing.T) {
	ts, err := time.Parse(time.Stamp, "Jul 15 15:15:15")
	assert.NoError(t, err)

	table := []struct {
		r Resolution
		n int
		s string
		k string
		t string
	}{
		{r: OneMinuteOf60Seconds, n: 60, s: "Jul 15 15:15:00", k: "15", t: "Jul 15 15:15:15"},
		{r: OneHourOf60Minutes, n: 60, s: "Jul 15 15:00:00", k: "15", t: "Jul 15 15:15:00"},
		{r: OneDayOf24Hours, n: 24, s: "Jul 15 00:00:00", k: "15", t: "Jul 15 15:00:00"},
		{r: OneMonthOfUpTo31Days, n: 31, s: "Jul  1 00:00:00", k: "15", t: "Jul 15 00:00:00"},
		{r: OneHourOf3600Seconds, n: 3600, s: "Jul 15 15:00:00", k: "915", t: "Jul 15 15:15:15"},
		{r: OneDayOf1440Minutes, n: 1440, s: "Jul 15 00:00:00", k: "915", t: "Jul 15 15:15:00"},
	}

	for _, e := range table {
		assert.Equal(t, e.n, e.r.SetSize())

		start, key := e.r.Split(ts)
		assert.Equal(t, e.s, start.Format(time.Stamp))
		assert.Equal(t, e.k, key)

		ts2 := e.r.Join(start, key)
		assert.Equal(t, e.t, ts2.Format(time.Stamp))
	}
}
