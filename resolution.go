package mgots

import (
	"strconv"
	"time"
)

// A Resolution specifies the granularity of saved samples and the organization
// in sets.
type Resolution interface {
	// Split should return the timestamp of a set and the key of the sample.
	Split(t time.Time) (time.Time, string)

	// Join should return the timestamp of a single sample based on the timestamp
	// of a set and the key of the sample.
	Join(start time.Time, key string) time.Time

	// SetSize should return the total amount of samples per set.
	SetSize() int

	// SetTimestamps should return a list of all set timestamps for the given
	// range.
	SetTimestamps(start time.Time, end time.Time) []time.Time

	// SampleTimestamps should return a list of all sample timestamps for the
	// given range.
	SampleTimestamps(start, end time.Time) []time.Time
}

// BasicResolution defines the granularity of the saved metrics.
type BasicResolution int

// The following basic resolutions are available:
const (
	OneMinuteOf60Seconds BasicResolution = iota
	OneHourOf60Minutes
	OneDayOf24Hours
	OneMonthOfUpTo31Days
	OneHourOf3600Seconds
	OneDayOf1440Minutes
)

// Split will return the beginning of a set and the key of the sample.
func (r BasicResolution) Split(t time.Time) (time.Time, string) {
	switch r {
	case OneMinuteOf60Seconds:
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), 0, 0, t.Location()), strconv.Itoa(t.Second())
	case OneHourOf60Minutes:
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, t.Location()), strconv.Itoa(t.Minute())
	case OneDayOf24Hours:
		return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location()), strconv.Itoa(t.Hour())
	case OneMonthOfUpTo31Days:
		return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, t.Location()), strconv.Itoa(t.Day())
	case OneHourOf3600Seconds:
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, t.Location()), strconv.Itoa(t.Minute()*60 + t.Second())
	case OneDayOf1440Minutes:
		return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location()), strconv.Itoa(t.Hour()*60 + t.Minute())
	}

	panic("invalid resolution")
}

// Join will return the timestamp of a single sample based on the start of a
// set and the key of the sample.
func (r BasicResolution) Join(start time.Time, key string) time.Time {
	// convert key to integer
	i, _ := strconv.Atoi(key)

	switch r {
	case OneMinuteOf60Seconds:
		return start.Add(time.Duration(i) * time.Second)
	case OneHourOf60Minutes:
		return start.Add(time.Duration(i) * time.Minute)
	case OneDayOf24Hours:
		return start.Add(time.Duration(i) * time.Hour)
	case OneMonthOfUpTo31Days:
		return start.AddDate(0, 0, i-1)
	case OneHourOf3600Seconds:
		return start.Add(time.Duration(i) * time.Second)
	case OneDayOf1440Minutes:
		return start.Add(time.Duration(i) * time.Minute)
	}

	panic("invalid resolution")
}

// SetSize will return the total amount of samples per set.
func (r BasicResolution) SetSize() int {
	switch r {
	case OneMinuteOf60Seconds:
		return 60
	case OneHourOf60Minutes:
		return 60
	case OneDayOf24Hours:
		return 24
	case OneMonthOfUpTo31Days:
		return 31
	case OneHourOf3600Seconds:
		return 3600
	case OneDayOf1440Minutes:
		return 1440
	}

	panic("invalid resolution")
}

// SetTimestamps will return a list of all set timestamps for the given range.
func (r BasicResolution) SetTimestamps(start, end time.Time) []time.Time {
	firstSet, _ := r.Split(start)
	curSet := firstSet
	list := make([]time.Time, 0)

	for curSet.Before(end) {
		list = append(list, curSet)

		switch r {
		case OneMinuteOf60Seconds:
			curSet = curSet.Add(1 * time.Minute)
		case OneHourOf60Minutes:
			curSet = curSet.Add(1 * time.Hour)
		case OneDayOf24Hours:
			curSet = curSet.AddDate(0, 0, 1)
		case OneMonthOfUpTo31Days:
			curSet = curSet.AddDate(0, 1, 0)
		case OneHourOf3600Seconds:
			curSet = curSet.Add(1 * time.Hour)
		case OneDayOf1440Minutes:
			curSet = curSet.AddDate(0, 0, 1)
		}
	}

	return list
}

// SampleTimestamps will return a list of all sample timestamps for the given
// range.
func (r BasicResolution) SampleTimestamps(start, end time.Time) []time.Time {
	firstSet, setKey := r.Split(start)
	firstSample := r.Join(firstSet, setKey)
	curSample := firstSample
	list := make([]time.Time, 0)

	for curSample.Before(end) {
		list = append(list, curSample)

		switch r {
		case OneMinuteOf60Seconds:
			curSample = curSample.Add(1 * time.Second)
		case OneHourOf60Minutes:
			curSample = curSample.Add(1 * time.Minute)
		case OneDayOf24Hours:
			curSample = curSample.Add(1 * time.Hour)
		case OneMonthOfUpTo31Days:
			curSample = curSample.AddDate(0, 0, 1)
		case OneHourOf3600Seconds:
			curSample = curSample.Add(1 * time.Second)
		case OneDayOf1440Minutes:
			curSample = curSample.Add(1 * time.Minute)
		}
	}

	return list
}
