package mgots

import (
	"strconv"
	"time"
)

// A Resolution specifies the granularity of saved samples and the organization
// in sets.
type Resolution interface {
	// Split should return the beginning of a set and the key of the sample.
	Split(t time.Time) (time.Time, string)

	// Join should return the timestamp of a single sample based on the start of a
	// set and the key of the sample.
	Join(start time.Time, key string) time.Time

	// SetSize should return the total amount of samples per set.
	SetSize() int
}

// BasicResolution defines the granularity of the saved metrics.
type BasicResolution string

// The following basic resolutions are available:
// - A resolution in seconds will store up to 60 samples in a document per minute.
// - A resolution in minutes will store up to 60 samples in a document per hour.
// - A resolution in hours will store up to 24 samples in a document per day.
// - A resolution in days will store up to 31 samples in a document per month.
const (
	Second BasicResolution = "s"
	Minute                 = "m"
	Hour                   = "h"
	Day                    = "d"
)

// Split will return the beginning of a set and the key of the sample.
func (r BasicResolution) Split(t time.Time) (time.Time, string) {
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

// Join will return the timestamp of a single sample based on the start of a
// set and the key of the sample.
func (r BasicResolution) Join(start time.Time, key string) time.Time {
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

// SetSize will return the total amount of samples per set.
func (r BasicResolution) SetSize() int {
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
