package mgots

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"gopkg.in/mgo.v2/bson"
)

func TestCollectionInsert(t *testing.T) {
	dbc := db.C("test-coll-insert")
	tsc := Wrap(dbc, OneMinuteOf60Seconds)

	now := parseTime("Jul 15 15:15:15")

	err := tsc.Insert(now, map[string]float64{
		"value": 10.0,
	}, nil)
	assert.NoError(t, err)

	var data []bson.M
	err = dbc.Find(nil).Select(bson.M{"_id": 0}).All(&data)
	assert.NoError(t, err)

	assert.Equal(t, []bson.M{
		{
			"num": bson.M{
				"value": int(1),
			},
			"total": bson.M{
				"value": float64(10),
			},
			"max": bson.M{
				"value": float64(10),
			},
			"min": bson.M{
				"value": float64(10),
			},
			"start": parseTime("Jul 15 15:15:00"),
			"tags":  bson.M{},
			"samples": bson.M{
				"15": bson.M{
					"start": parseTime("Jul 15 15:15:15"),
					"value": bson.M{
						"total": float64(10),
						"num":   int(1),
						"max":   float64(10),
						"min":   float64(10),
					},
				},
			},
		},
	}, forceUTCSlice(data))
}

func TestCollectionBulkInsert(t *testing.T) {
	dbc := db.C("test-coll-bulk-insert")
	tsc := Wrap(dbc, OneMinuteOf60Seconds)
	bulk := tsc.Bulk()

	now := parseTime("Jul 15 15:15:15")

	for i := 0; i < 2; i++ {
		bulk.Insert(now, map[string]float64{
			"value": float64(i),
		}, nil)
	}

	err := bulk.Run()
	assert.NoError(t, err)

	var data []bson.M
	err = dbc.Find(nil).Select(bson.M{"_id": 0}).All(&data)
	assert.NoError(t, err)

	assert.Equal(t, []bson.M{
		{
			"start": parseTime("Jul 15 15:15:00"),
			"tags":  bson.M{},
			"samples": bson.M{
				"15": bson.M{
					"start": parseTime("Jul 15 15:15:15"),
					"value": bson.M{
						"total": float64(1),
						"num":   int(2),
						"max":   float64(1),
						"min":   float64(0),
					},
				},
			},
			"total": bson.M{
				"value": float64(1),
			},
			"num": bson.M{
				"value": int(2),
			},
			"max": bson.M{
				"value": float64(1),
			},
			"min": bson.M{
				"value": float64(0),
			},
		},
	}, forceUTCSlice(data))
}

func TestCollectionAggregateSamples(t *testing.T) {
	dbc := db.C("test-coll-aggregate-samples")
	tsc := Wrap(dbc, OneMinuteOf60Seconds)

	bulk := tsc.Bulk()

	now := parseTime("Jul 15 15:15:15")

	for i := 0; i < 3; i++ {
		bulk.Insert(now.Add(time.Duration(i)*time.Second), map[string]float64{
			"value": float64(i),
		}, bson.M{
			"foo":  "bar",
			"host": "one",
		})
	}

	for i := 0; i < 3; i++ {
		bulk.Insert(now.Add(time.Duration(i)*time.Second), map[string]float64{
			"value": float64(10 + i),
		}, bson.M{
			"foo":  "bar",
			"host": "two",
		})
	}

	for i := 0; i < 3; i++ {
		bulk.Insert(now.Add(time.Duration(i)*time.Second), map[string]float64{
			"value": float64(20 + i),
		}, bson.M{
			"foo":  "bar",
			"host": "three",
		})
	}

	err := bulk.Run()
	assert.NoError(t, err)

	ts, err := tsc.AggregateSamples(now, now.Add(2*time.Second), []string{"value"}, bson.M{
		"foo": "bar",
	})
	assert.NoError(t, err)
	assert.Equal(t, &TimeSeries{
		Start: parseTime("Jul 15 15:15:15"),
		End:   parseTime("Jul 15 15:15:17"),
		Samples: []Sample{
			{
				Start: parseTime("Jul 15 15:15:15"),
				Metrics: map[string]Metric{
					"value": {Max: 20, Min: 0, Num: 3, Total: 30},
				},
			},
			{
				Start: parseTime("Jul 15 15:15:16"),
				Metrics: map[string]Metric{
					"value": {Max: 21, Min: 1, Num: 3, Total: 33},
				},
			},
			{
				Start: parseTime("Jul 15 15:15:17"),
				Metrics: map[string]Metric{
					"value": {Max: 22, Min: 2, Num: 3, Total: 36},
				},
			},
		},
	}, forceUTCTimeSeries(ts))
}

func TestCollectionAggregateSets(t *testing.T) {
	dbc := db.C("test-coll-aggregate-sets")
	tsc := Wrap(dbc, OneMinuteOf60Seconds)

	bulk := tsc.Bulk()

	now := parseTime("Jul 15 15:15:15")

	for i := 0; i < 3; i++ {
		bulk.Insert(now.Add(time.Duration(i)*time.Minute), map[string]float64{
			"value": float64(i),
		}, bson.M{
			"foo":  "bar",
			"host": "one",
		})
	}

	for i := 0; i < 3; i++ {
		bulk.Insert(now.Add(time.Duration(i)*time.Minute), map[string]float64{
			"value": float64(10 + i),
		}, bson.M{
			"foo":  "bar",
			"host": "two",
		})
	}

	for i := 0; i < 3; i++ {
		bulk.Insert(now.Add(time.Duration(i)*time.Minute), map[string]float64{
			"value": float64(20 + i),
		}, bson.M{
			"foo":  "bar",
			"host": "three",
		})
	}

	err := bulk.Run()
	assert.NoError(t, err)

	ts, err := tsc.AggregateSets(now, now.Add(3*time.Minute), []string{"value"}, bson.M{
		"foo": "bar",
	})
	assert.NoError(t, err)
	assert.Equal(t, &TimeSeries{
		Start: parseTime("Jul 15 15:15:15"),
		End:   parseTime("Jul 15 15:18:15"),
		Samples: []Sample{
			{
				Start: parseTime("Jul 15 15:15:00"),
				Metrics: map[string]Metric{
					"value": {Max: 20, Min: 0, Num: 3, Total: 30},
				},
			},
			{
				Start: parseTime("Jul 15 15:16:00"),
				Metrics: map[string]Metric{
					"value": {Max: 21, Min: 1, Num: 3, Total: 33},
				},
			},
			{
				Start: parseTime("Jul 15 15:17:00"),
				Metrics: map[string]Metric{
					"value": {Max: 22, Min: 2, Num: 3, Total: 36},
				},
			},
		},
	}, forceUTCTimeSeries(ts))
}
