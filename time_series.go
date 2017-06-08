package mgots

import (
	"math"
	"time"
)

// TODO: Add support for percentiles.

// TODO: Add support for median and other statistical functions?

// TODO: Add fill function.

// A Point is a single aggregated point in a TimeSeries.
type Point struct {
	Timestamp time.Time
	Max       float64
	Min       float64
	Num       int
	Total     float64
}

// A TimeSeries is a list of points.
type TimeSeries struct {
	Start  time.Time
	End    time.Time
	Points []Point
}

// Avg returns the average value for the given time series.
func (ts *TimeSeries) Avg() float64 {
	var total float64

	for _, p := range ts.Points {
		total += p.Total / float64(p.Num)
	}

	return total / float64(len(ts.Points))
}

// Min returns the minimum value for the given time series.
func (ts *TimeSeries) Min() float64 {
	min := ts.Points[0].Min

	for _, p := range ts.Points {
		min = math.Min(min, p.Min)
	}

	return min
}

// Max returns the maximum value for the given time series.
func (ts *TimeSeries) Max() float64 {
	max := ts.Points[0].Max

	for _, p := range ts.Points {
		max = math.Max(max, p.Max)
	}

	return max
}
