package mgots

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"gopkg.in/mgo.v2/bson"
)

func TestCollectionInsert(t *testing.T) {
	dbc := db.C("test-coll-insert")
	tsc := Wrap(dbc, Second)

	now := time.Time{}

	err := tsc.Insert(now, map[string]float64{
		"value": 10.0,
	}, nil)
	assert.NoError(t, err)

	var data []bson.M
	err = dbc.Find(nil).Select(bson.M{"_id": 0}).All(&data)
	assert.NoError(t, err)

	assert.JSONEq(t, `[{
		"start": "0001-01-01T00:00:00Z",
		"tags": {},
		"total": { "value": 10 },
		"max": { "value": 10 },
		"min": { "value": 10 },
		"num": { "value": 1 },
		"samples": {
			"0": {
				"start":"0001-01-01T00:00:00Z",
				"value": {
					"max": 10,
					"min": 10,
					"num": 1,
					"total": 10
				}
			}
		}
	}]`, jsonString(data))
}

func TestCollectionBulkInsert(t *testing.T) {
	dbc := db.C("test-coll-add")
	tsc := Wrap(dbc, Second)
	bulk := tsc.Bulk()

	now := time.Time{}

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

	assert.JSONEq(t, `[{
		"start": "0001-01-01T00:00:00Z",
		"tags": {},
		"total": { "value": 1 },
		"max": { "value": 1 },
		"min": { "value": 0 },
		"num": { "value": 2 },
		"samples": {
			"0": {
				"start":"0001-01-01T00:00:00Z",
				"value": {
					"max": 1,
					"min": 0,
					"num": 2,
					"total": 1
				}
			}
		}
	}]`, jsonString(data))
}

func TestCollectionAggregateSamples(t *testing.T) {
	dbc := db.C("test-coll-aggregate")
	tsc := Wrap(dbc, Second)

	bulk := tsc.Bulk()

	now := time.Time{}

	for i := 0; i < 3; i++ {
		bulk.Insert(now.Add(time.Duration(i)*time.Second), map[string]float64{
			"value": float64(i),
		}, bson.M{
			"host": "one",
		})
	}

	for i := 0; i < 3; i++ {
		bulk.Insert(now.Add(time.Duration(i)*time.Second), map[string]float64{
			"value": float64(10 + i),
		}, bson.M{
			"host": "two",
		})
	}

	for i := 0; i < 3; i++ {
		bulk.Insert(now.Add(time.Duration(i)*time.Second), map[string]float64{
			"value": float64(20 + i),
		}, bson.M{
			"host": "three",
		})
	}

	err := bulk.Run()
	assert.NoError(t, err)

	ts, err := tsc.AggregateSamples(now, now.Add(2*time.Second), []string{"value"}, nil)
	assert.NoError(t, err)
	assert.JSONEq(t, `{
		"Start": "0001-01-01T00:00:00Z",
		"End": "0001-01-01T00:00:02Z",
		"Samples":[
			{
				"Start": "0001-01-01T00:00:00Z",
				"Metrics": {
					"value": {
						"Max": 20,
						"Min": 0,
						"Num": 3,
						"Total": 30
					}
				}
			}, {
				"Start": "0001-01-01T01:00:01+01:00",
				"Metrics": {
					"value": {
						"Max": 21,
						"Min": 1,
						"Num": 3,
						"Total": 33
					}
				}
			}, {
				"Start": "0001-01-01T01:00:02+01:00",
				"Metrics": {
					"value": {
						"Max": 22,
						"Min": 2,
						"Num": 3,
						"Total": 36
					}
				}

			}
		]
	}`, jsonString(ts))
}

func TestCollectionAggregateSets(t *testing.T) {
	dbc := db.C("test-coll-macro-aggregate")
	tsc := Wrap(dbc, Second)

	bulk := tsc.Bulk()

	now := time.Time{}

	for i := 0; i < 3; i++ {
		bulk.Insert(now.Add(time.Duration(i)*time.Minute), map[string]float64{
			"value": float64(i),
		}, bson.M{
			"host": "one",
		})
	}

	for i := 0; i < 3; i++ {
		bulk.Insert(now.Add(time.Duration(i)*time.Minute), map[string]float64{
			"value": float64(10 + i),
		}, bson.M{
			"host": "two",
		})
	}

	for i := 0; i < 3; i++ {
		bulk.Insert(now.Add(time.Duration(i)*time.Minute), map[string]float64{
			"value": float64(20 + i),
		}, bson.M{
			"host": "three",
		})
	}

	err := bulk.Run()
	assert.NoError(t, err)

	ts, err := tsc.AggregateSets(now, now.Add(3*time.Minute), []string{"value"}, nil)
	assert.NoError(t, err)
	assert.JSONEq(t, `{
		"Start": "0001-01-01T00:00:00Z",
		"End": "0001-01-01T00:03:00Z",
		"Samples":[
			{
				"Start": "0001-01-01T00:00:00Z",
				"Metrics": {
					"value": {
						"Max": 20,
						"Min": 0,
						"Num": 3,
						"Total": 30
					}
				}
			}, {
				"Start": "0001-01-01T01:01:00+01:00",
				"Metrics": {
					"value": {
						"Max": 21,
						"Min": 1,
						"Num": 3,
						"Total": 33
					}
				}
			}, {
				"Start": "0001-01-01T01:02:00+01:00",
				"Metrics": {
					"value": {
						"Max": 22,
						"Min": 2,
						"Num": 3,
						"Total": 36
					}
				}
			}
		]
	}`, jsonString(ts))
}
