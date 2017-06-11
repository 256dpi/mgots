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
		"min": { "value":10 },
		"num": { "value":1 },
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

func TestCollectionAdd(t *testing.T) {
	dbc := db.C("test-coll-add")
	tsc := Wrap(dbc, Second)
	bulk := tsc.Bulk()

	now := time.Time{}

	for i := 0; i < 2; i++ {
		bulk.Add(now, map[string]float64{
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

func TestCollectionAverage(t *testing.T) {
	dbc := db.C("test-coll-average")
	tsc := Wrap(dbc, Second)

	bulk := tsc.Bulk()

	now := time.Now()

	for i := 1; i < 10; i++ {
		bulk.Add(now.Add(time.Duration(i)*time.Second), map[string]float64{
			"value": float64(i),
		}, bson.M{
			"host": "one",
		})
	}

	for i := 1; i < 10; i++ {
		bulk.Add(now.Add(time.Duration(i)*time.Second), map[string]float64{
			"value": float64(10 + i),
		}, bson.M{
			"host": "two",
		})
	}

	for i := 1; i < 10; i++ {
		bulk.Add(now.Add(time.Duration(i)*time.Second), map[string]float64{
			"value": float64(20 + i),
		}, bson.M{
			"host": "three",
		})
	}

	err := bulk.Run()
	assert.NoError(t, err)

	_, err = tsc.Aggregate(now, now.Add(10*time.Second), "value", nil)
	assert.NoError(t, err)
}
