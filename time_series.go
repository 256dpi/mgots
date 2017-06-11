package mgots

import (
	"math"
	"time"
)

// TODO: Add fill function.

// A Sample is a single aggregated sample.
type Sample struct {
	Start time.Time
	Max   float64
	Min   float64
	Num   int
	Total float64
}

// A TimeSeries is a list of samples.
type TimeSeries struct {
	Start   time.Time
	End     time.Time
	Samples []Sample
}

// Avg returns the average value for the given time series.
func (ts *TimeSeries) Avg() float64 {
	var total float64

	for _, p := range ts.Samples {
		total += p.Total / float64(p.Num)
	}

	return total / float64(len(ts.Samples))
}

// Min returns the minimum value for the given time series.
func (ts *TimeSeries) Min() float64 {
	min := ts.Samples[0].Min

	for _, p := range ts.Samples {
		min = math.Min(min, p.Min)
	}

	return min
}

// Max returns the maximum value for the given time series.
func (ts *TimeSeries) Max() float64 {
	max := ts.Samples[0].Max

	for _, p := range ts.Samples {
		max = math.Max(max, p.Max)
	}

	return max
}
