package mgots

import (
	"strconv"
	"time"
)

// A Resolution specifies the granularity of saved samples and the organization
// in sets.
type Resolution interface {
	// Split should return the set timestamp and sample key for the given time.
	Split(t time.Time) (time.Time, string)

	// Join should return the timestamp of a single sample based on the timestamp
	// of a set and the key of the sample.
	Join(t time.Time, key string) time.Time

	// SetSize should return the number of samples per set.
	SetSize() int

	// SetTimestamp should return the set timestamp for the given time.
	SetTimestamp(t time.Time) time.Time

	// SetTimestamps should return a list set timestamps for the given time range.
	SetTimestamps(first time.Time, last time.Time) []time.Time

	// SampleKey should return the sample key for given time.
	SampleKey(t time.Time) string

	// SampleTimestamp should return the sample timestamp for the given time.
	SampleTimestamp(t time.Time) time.Time

	// SampleTimestamps should return a list sample timestamps for the given time
	// range.
	SampleTimestamps(first, last time.Time) []time.Time
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

// Split will return the set timestamp and sample key for the given time.
func (r BasicResolution) Split(t time.Time) (time.Time, string) {
	return r.SetTimestamp(t), r.SampleKey(t)
}

// Join will return the timestamp of a single sample based on the start of a
// set and the key of the sample.
func (r BasicResolution) Join(t time.Time, key string) time.Time {
	// convert key to integer
	i, _ := strconv.Atoi(key)

	ts := time.Time{}

	switch r {
	case OneMinuteOf60Seconds:
		ts = t.Add(time.Duration(i) * time.Second)
	case OneHourOf60Minutes:
		ts = t.Add(time.Duration(i) * time.Minute)
	case OneDayOf24Hours:
		ts = t.Add(time.Duration(i) * time.Hour)
	case OneMonthOfUpTo31Days:
		ts = t.AddDate(0, 0, i-1)
	case OneHourOf3600Seconds:
		ts = t.Add(time.Duration(i) * time.Second)
	case OneDayOf1440Minutes:
		ts = t.Add(time.Duration(i) * time.Minute)
	}

	return ts
}

// SetSize will return the number of samples per set.
func (r BasicResolution) SetSize() int {
	size := 0

	switch r {
	case OneMinuteOf60Seconds:
		size = 60
	case OneHourOf60Minutes:
		size = 60
	case OneDayOf24Hours:
		size = 24
	case OneMonthOfUpTo31Days:
		size = 31
	case OneHourOf3600Seconds:
		size = 3600
	case OneDayOf1440Minutes:
		size = 1440
	}

	return size
}

// SetTimestamp will return the set timestamp for the given time.
func (r BasicResolution) SetTimestamp(t time.Time) time.Time {
	ts := time.Time{}

	switch r {
	case OneMinuteOf60Seconds:
		ts = time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), 0, 0, t.Location())
	case OneHourOf60Minutes:
		ts = time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, t.Location())
	case OneDayOf24Hours:
		ts = time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
	case OneMonthOfUpTo31Days:
		ts = time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, t.Location())
	case OneHourOf3600Seconds:
		ts = time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, t.Location())
	case OneDayOf1440Minutes:
		ts = time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
	}

	return ts
}

// SetTimestamps will return a list set timestamps for the given time range.
func (r BasicResolution) SetTimestamps(first, last time.Time) []time.Time {
	firstSet := r.SetTimestamp(first)
	curSet := firstSet
	list := make([]time.Time, 0)

	for curSet.Before(last) || curSet.Equal(last) {
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

// SampleKey will return the sample key for given time.
func (r BasicResolution) SampleKey(t time.Time) string {
	key := ""

	switch r {
	case OneMinuteOf60Seconds:
		key = strconv.Itoa(t.Second())
	case OneHourOf60Minutes:
		key = strconv.Itoa(t.Minute())
	case OneDayOf24Hours:
		key = strconv.Itoa(t.Hour())
	case OneMonthOfUpTo31Days:
		key = strconv.Itoa(t.Day())
	case OneHourOf3600Seconds:
		key = strconv.Itoa(t.Minute()*60 + t.Second())
	case OneDayOf1440Minutes:
		key = strconv.Itoa(t.Hour()*60 + t.Minute())
	}

	return key
}

// SampleTimestamp will return the sample timestamp for the given time.
func (r BasicResolution) SampleTimestamp(t time.Time) time.Time {
	ts := time.Time{}

	switch r {
	case OneMinuteOf60Seconds:
		ts = time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), 0, t.Location())
	case OneHourOf60Minutes:
		ts = time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), 0, 0, t.Location())
	case OneDayOf24Hours:
		ts = time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, t.Location())
	case OneMonthOfUpTo31Days:
		ts = time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
	case OneHourOf3600Seconds:
		ts = time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), 0, t.Location())
	case OneDayOf1440Minutes:
		ts = time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), 0, 0, t.Location())
	}

	return ts
}

// SampleTimestamps will return a list sample timestamps for the given time range.
func (r BasicResolution) SampleTimestamps(first, last time.Time) []time.Time {
	firstSample := r.SampleTimestamp(first)
	curSample := firstSample
	list := make([]time.Time, 0)

	for curSample.Before(last) || curSample.Equal(last) {
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
