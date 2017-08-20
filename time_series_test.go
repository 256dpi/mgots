package mgots

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTimeSeriesNull1(t *testing.T) {
	tl := OneMinuteOf60Seconds.SampleTimestamps(parseTime("Jul 15 15:15:15"), parseTime("Jul 15 15:15:18"))
	assert.Len(t, tl, 3)

	ts := &TimeSeries{
		Start: tl[0],
		End:   tl[2],
		Samples: []Sample{
			{
				Start: tl[1],
				Metrics: map[string]Metric{
					"value": {Max: 10, Min: 1, Num: 2, Total: 11},
				},
			},
			{
				Start: tl[2],
				Metrics: map[string]Metric{
					"value": {Max: 20, Min: 2, Num: 2, Total: 22},
				},
			},
		},
	}

	ts2 := ts.Null(tl, []string{"value"})
	assert.Len(t, ts2.Samples, 3)
	assert.Equal(t, ts.Start, ts2.Start)
	assert.Equal(t, ts.End, ts2.End)
	assert.Equal(t, &TimeSeries{
		Start: parseTime("Jul 15 15:15:15"),
		End:   parseTime("Jul 15 15:15:17"),
		Samples: []Sample{
			{
				Start: parseTime("Jul 15 15:15:15"),
				Metrics: map[string]Metric{
					"value": {Max: 0, Min: 0, Num: 0, Total: 0},
				},
			},
			{
				Start: parseTime("Jul 15 15:15:16"),
				Metrics: map[string]Metric{
					"value": {Max: 10, Min: 1, Num: 2, Total: 11},
				},
			},
			{
				Start: parseTime("Jul 15 15:15:17"),
				Metrics: map[string]Metric{
					"value": {Max: 20, Min: 2, Num: 2, Total: 22},
				},
			},
		},
	}, forceUTCTimeSeries(ts2))
}

func TestTimeSeriesNull2(t *testing.T) {
	tl := OneMinuteOf60Seconds.SampleTimestamps(parseTime("Jul 15 15:15:15"), parseTime("Jul 15 15:15:18"))
	assert.Len(t, tl, 3)

	ts := &TimeSeries{
		Start: tl[0],
		End:   tl[2],
		Samples: []Sample{
			{
				Start: tl[0],
				Metrics: map[string]Metric{
					"value": {Max: 10, Min: 1, Num: 2, Total: 11},
				},
			},
			{
				Start: tl[2],
				Metrics: map[string]Metric{
					"value": {Max: 20, Min: 2, Num: 2, Total: 22},
				},
			},
		},
	}

	ts2 := ts.Null(tl, []string{"value"})
	assert.Len(t, ts2.Samples, 3)
	assert.Equal(t, ts.Start, ts2.Start)
	assert.Equal(t, ts.End, ts2.End)
	assert.Equal(t, &TimeSeries{
		Start: parseTime("Jul 15 15:15:15"),
		End:   parseTime("Jul 15 15:15:17"),
		Samples: []Sample{
			{
				Start: parseTime("Jul 15 15:15:15"),
				Metrics: map[string]Metric{
					"value": {Max: 10, Min: 1, Num: 2, Total: 11},
				},
			},
			{
				Start: parseTime("Jul 15 15:15:16"),
				Metrics: map[string]Metric{
					"value": {Max: 0, Min: 0, Num: 0, Total: 0},
				},
			},
			{
				Start: parseTime("Jul 15 15:15:17"),
				Metrics: map[string]Metric{
					"value": {Max: 20, Min: 2, Num: 2, Total: 22},
				},
			},
		},
	}, forceUTCTimeSeries(ts2))
}

func TestTimeSeriesNull3(t *testing.T) {
	tl := OneMinuteOf60Seconds.SampleTimestamps(parseTime("Jul 15 15:15:15"), parseTime("Jul 15 15:15:18"))
	assert.Len(t, tl, 3)

	ts := &TimeSeries{
		Start: tl[0],
		End:   tl[2],
		Samples: []Sample{
			{
				Start: tl[0],
				Metrics: map[string]Metric{
					"value": {Max: 10, Min: 1, Num: 2, Total: 11},
				},
			},
			{
				Start: tl[1],
				Metrics: map[string]Metric{
					"value": {Max: 20, Min: 2, Num: 2, Total: 22},
				},
			},
		},
	}

	ts2 := ts.Null(tl, []string{"value"})
	assert.Len(t, ts2.Samples, 3)
	assert.Equal(t, ts.Start, ts2.Start)
	assert.Equal(t, ts.End, ts2.End)
	assert.Equal(t, &TimeSeries{
		Start: parseTime("Jul 15 15:15:15"),
		End:   parseTime("Jul 15 15:15:17"),
		Samples: []Sample{
			{
				Start: parseTime("Jul 15 15:15:15"),
				Metrics: map[string]Metric{
					"value": {Max: 10, Min: 1, Num: 2, Total: 11},
				},
			},
			{
				Start: parseTime("Jul 15 15:15:16"),
				Metrics: map[string]Metric{
					"value": {Max: 20, Min: 2, Num: 2, Total: 22},
				},
			},
			{
				Start: parseTime("Jul 15 15:15:17"),
				Metrics: map[string]Metric{
					"value": {Max: 0, Min: 0, Num: 0, Total: 0},
				},
			},
		},
	}, forceUTCTimeSeries(ts2))
}
