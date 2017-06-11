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

	err := tsc.Insert(time.Now(), map[string]float64{
		"value": 10.0,
	}, nil)
	assert.NoError(t, err)

	// TODO: Verify data layout.
}

func TestCollectionAdd(t *testing.T) {
	dbc := db.C("test-coll-add")
	tsc := Wrap(dbc, Second)

	bulk := tsc.Bulk()

	for i := 1; i < 10; i++ {
		bulk.Add(time.Now(), map[string]float64{
			"value": float64(i),
		}, nil)
	}

	err := bulk.Run()
	assert.NoError(t, err)

	// TODO: Verify data layout.
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
