package mgots

import (
	"math"
	"sort"
	"time"
)

type Point struct {
	Timestamp time.Time
	Value     float64
	Max       float64
	Min       float64
	Num       int
	Total     float64
}

func sortPoints(points []Point) []Point {
	sort.Slice(points, func(i, j int)bool{
		return points[i].Timestamp.Before(points[j].Timestamp)
	})

	return points
}

type TimeSeries struct {
	Points []Point
}

func (ts *TimeSeries) Avg() float64 {
	var total float64

	for _, p := range ts.Points {
		total += p.Value
	}

	return total / float64(len(ts.Points))
}

func (ts *TimeSeries) Min() float64 {
	min := ts.Points[0].Value

	for _, p := range ts.Points {
		min = math.Min(min, p.Value)
	}

	return min
}

func (ts *TimeSeries) Max() float64 {
	max := ts.Points[0].Value

	for _, p := range ts.Points {
		max = math.Max(max, p.Value)
	}

	return max
}

func (ts *TimeSeries) Values() []float64 {
	values := make([]float64, len(ts.Points))

	for i, point := range ts.Points {
		values[i] = point.Value
	}

	return values
}

func (ts *TimeSeries) Timestamps() []time.Time {
	timestamps := make([]time.Time, len(ts.Points))

	for i, point := range ts.Points {
		timestamps[i] = point.Timestamp
	}

	return timestamps
}
