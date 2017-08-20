package mgots

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTimeSeriesNull1(t *testing.T) {
	tl := OneMinuteOf60Seconds.SampleTimestamps(parseTime("Jul 15 15:15:15"), parseTime("Jul 15 15:15:18"))

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
	assert.JSONEq(t, `{
		"Start": "2017-07-15T15:15:15Z",
		"End": "2017-07-15T15:15:17Z",
		"Samples":[
			{
				"Start": "2017-07-15T15:15:15Z",
				"Metrics": {
					"value": {
						"Max": 0,
						"Min": 0,
						"Num": 0,
						"Total": 0
					}
				}
			}, {
				"Start": "2017-07-15T15:15:16Z",
				"Metrics": {
					"value": {
						"Max": 10,
						"Min": 1,
						"Num": 2,
						"Total": 11

					}
				}
			}, {
				"Start": "2017-07-15T15:15:17Z",
				"Metrics": {
					"value": {
						"Max": 20,
						"Min": 2,
						"Num": 2,
						"Total": 22
					}
				}
			}
		]
	}`, jsonString(ts2))
}

func TestTimeSeriesNull2(t *testing.T) {
	tl := OneMinuteOf60Seconds.SampleTimestamps(parseTime("Jul 15 15:15:15"), parseTime("Jul 15 15:15:18"))

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
	assert.JSONEq(t, `{
		"Start": "2017-07-15T15:15:15Z",
		"End": "2017-07-15T15:15:17Z",
		"Samples":[
			{
				"Start": "2017-07-15T15:15:15Z",
				"Metrics": {
					"value": {
						"Max": 10,
						"Min": 1,
						"Num": 2,
						"Total": 11
					}
				}
			}, {
				"Start": "2017-07-15T15:15:16Z",
				"Metrics": {
					"value": {
						"Max": 0,
						"Min": 0,
						"Num": 0,
						"Total": 0
					}
				}
			}, {
				"Start": "2017-07-15T15:15:17Z",
				"Metrics": {
					"value": {
						"Max": 20,
						"Min": 2,
						"Num": 2,
						"Total": 22
					}
				}
			}
		]
	}`, jsonString(ts2))
}

func TestTimeSeriesNull3(t *testing.T) {
	tl := OneMinuteOf60Seconds.SampleTimestamps(parseTime("Jul 15 15:15:15"), parseTime("Jul 15 15:15:18"))

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
	assert.JSONEq(t, `{
		"Start": "2017-07-15T15:15:15Z",
		"End": "2017-07-15T15:15:17Z",
		"Samples":[
			{
				"Start": "2017-07-15T15:15:15Z",
				"Metrics": {
					"value": {
						"Max": 10,
						"Min": 1,
						"Num": 2,
						"Total": 11
					}
				}
			}, {
				"Start": "2017-07-15T15:15:16Z",
				"Metrics": {
					"value": {
						"Max": 20,
						"Min": 2,
						"Num": 2,
						"Total": 22
					}
				}
			}, {
				"Start": "2017-07-15T15:15:17Z",
				"Metrics": {
					"value": {
						"Max": 0,
						"Min": 0,
						"Num": 0,
						"Total": 0

					}
				}
			}
		]
	}`, jsonString(ts2))
}
