package mgots

import (
	"strconv"
	"time"
)

// Resolution defines the granularity of the saved metrics.
type Resolution string

const (
	Second Resolution = "s"
	Minute            = "m"
	Hour              = "h"
	Day               = "D"
	Month             = "M"
)

func (r Resolution) extractStartAndKey(t time.Time) (time.Time, string) {
	switch r {
	case Second:
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), 0, 0, t.Location()), strconv.Itoa(t.Second())
	case Minute:
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, t.Location()), strconv.Itoa(t.Minute())
	case Hour:
		return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location()), strconv.Itoa(t.Hour())
	case Day:
		return time.Date(t.Year(), t.Month(), 0, 0, 0, 0, 0, t.Location()), strconv.Itoa(t.Day())
	case Month:
		return time.Date(t.Year(), 0, 0, 0, 0, 0, 0, t.Location()), strconv.Itoa(int(t.Month()))
	}

	panic("invalid resolution")
}

func (r Resolution) combineStartAndKey(t time.Time, key string) time.Time {
	i, err := strconv.Atoi(key)
	if err != nil {
		panic(err)
	}

	switch r {
	case Second:
		return t.Add(time.Duration(i) * time.Second)
	case Minute:
		return t.Add(time.Duration(i) * time.Minute)
	case Hour:
		return t.Add(time.Duration(i) * time.Hour)
	case Day:
		return t.AddDate(0, 0, i)
	case Month:
		return t.AddDate(0, i, 0)
	}

	panic("invalid resolution")
}

func (r Resolution) estimatedPoints() int {
	switch r {
	case Second, Minute:
		return 60
	case Hour:
		return 24
	case Day:
		return 30
	case Month:
		return 24
	}

	panic("invalid resolution")
}
