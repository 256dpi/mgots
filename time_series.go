package mgots

import (
	"math"
	"time"
)

// A Metric is a single aggregated metric in a sample.
type Metric struct {
	Max   float64
	Min   float64
	Num   int
	Total float64
}

// A Sample is a single aggregated sample in a time series.
type Sample struct {
	Start   time.Time
	Metrics map[string]Metric
}

// A TimeSeries is a list of samples.
type TimeSeries struct {
	Start   time.Time
	End     time.Time
	Samples []Sample
}

// Avg returns the average value for the given time series.
func (ts *TimeSeries) Avg(metric string) float64 {
	var total float64

	for _, p := range ts.Samples {
		total += p.Metrics[metric].Total / float64(p.Metrics[metric].Num)
	}

	return total / float64(len(ts.Samples))
}

// Min returns the minimum value for the given time series.
func (ts *TimeSeries) Min(metric string) float64 {
	min := ts.Samples[0].Metrics[metric].Min

	for _, p := range ts.Samples {
		min = math.Min(min, p.Metrics[metric].Min)
	}

	return min
}

// Max returns the maximum value for the given time series.
func (ts *TimeSeries) Max(metric string) float64 {
	max := ts.Samples[0].Metrics[metric].Max

	for _, p := range ts.Samples {
		max = math.Max(max, p.Metrics[metric].Max)
	}

	return max
}
