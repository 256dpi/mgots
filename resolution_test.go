package mgots

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBasicResolutionSplit(t *testing.T) {
	t1, err := time.Parse(time.Stamp, "Jan 11 15:15:15")
	assert.NoError(t, err)

	table := []struct {
		r Resolution
		t time.Time
		s string
		k string
	}{
		{r: OneMinuteOf60Seconds, t: t1, s: "Jan 11 15:15:00", k: "15"},
		{r: OneHourOf60Minutes, t: t1, s: "Jan 11 15:00:00", k: "15"},
		{r: OneDayOf24Hours, t: t1, s: "Jan 11 00:00:00", k: "15"},
		{r: OneMonthOfUpTo31Days, t: t1, s: "Jan  1 00:00:00", k: "11"},
		{r: OneHourOf3600Seconds, t: t1, s: "Jan 11 15:00:00", k: "915"},
		{r: OneDayOf1440Minutes, t: t1, s: "Jan 11 00:00:00", k: "915"},
	}

	for _, e := range table {
		start, key := e.r.Split(e.t)
		assert.Equal(t, e.s, start.Format(time.Stamp))
		assert.Equal(t, e.k, key)
	}
}

func TestBasicResolutionJoin(t *testing.T) {

}

func TestBasicResolutionSetSize(t *testing.T) {
	table := map[Resolution]int{
		OneMinuteOf60Seconds: 60,
		OneHourOf60Minutes:   60,
		OneDayOf24Hours:      24,
		OneMonthOfUpTo31Days: 31,
		OneHourOf3600Seconds: 3600,
		OneDayOf1440Minutes:  1440,
	}

	for res, size := range table {
		assert.Equal(t, size, res.SetSize())
	}
}
