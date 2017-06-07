package mgots

import (
	"strconv"
	"time"
)

// Resolution defines the granularity of the saved metrics.
type Resolution string

const (
	// A resolution in seconds will store 60 values in a document per minute.
	Second Resolution = "s"

	// A resolution in minutes will stored 60 values in a document per hour.
	Minute = "m"

	// A resolution in hours will store 24 values in a document per day.
	Hour = "h"

	// A resolution in days will stores 31 values in a document per month.
	Day = "d"
)

// Split will return the beginning of a batch and the key of the value as
// defined by the given resolution.
func (r Resolution) Split(t time.Time) (time.Time, string) {
	switch r {
	case Second:
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), 0, 0, t.Location()), strconv.Itoa(t.Second())
	case Minute:
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, t.Location()), strconv.Itoa(t.Minute())
	case Hour:
		return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location()), strconv.Itoa(t.Hour())
	case Day:
		return time.Date(t.Year(), t.Month(), 0, 0, 0, 0, 0, t.Location()), strconv.Itoa(t.Day())
	}

	panic("invalid resolution")
}

// Join will return the timestamp of a single point based on the start of a
// batch and the key of the value as defined by the given resolution.
func (r Resolution) Join(start time.Time, key string) time.Time {
	i, err := strconv.Atoi(key)
	if err != nil {
		panic(err)
	}

	switch r {
	case Second:
		return start.Add(time.Duration(i) * time.Second)
	case Minute:
		return start.Add(time.Duration(i) * time.Minute)
	case Hour:
		return start.Add(time.Duration(i) * time.Hour)
	case Day:
		return start.AddDate(0, 0, i)
	}

	panic("invalid resolution")
}

// BatchSize will return the amount of points per batch for the given resolution.
func (r Resolution) BatchSize() int {
	switch r {
	case Second, Minute:
		return 60
	case Hour:
		return 24
	case Day:
		return 31
	}

	panic("invalid resolution")
}
