package mgots

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCollectionInsert(t *testing.T) {
	dbc := db.C("test-coll-insert")
	tsc := Wrap(dbc, Second)

	now := time.Now()

	err := tsc.Insert(now, map[string]float64{
		"value": 10.0,
	}, nil)
	assert.NoError(t, err)

	ts, err := tsc.Fetch(now.Add(-1*time.Second), now.Add(1*time.Second), "value", nil)
	assert.NoError(t, err)
	assert.Equal(t, now.Truncate(time.Second), ts.Points[0].Timestamp)
	assert.Equal(t, 10.0, ts.Points[0].Max)
	assert.Equal(t, 10.0, ts.Points[0].Min)
	assert.Equal(t, 1, ts.Points[0].Num)
	assert.Equal(t, 10.0, ts.Points[0].Total)
}

func TestCollectionAdd(t *testing.T) {
	dbc := db.C("test-coll-add")
	tsc := Wrap(dbc, Second)

	now := time.Now()

	bulk := tsc.Bulk()

	bulk.Add(now, map[string]float64{
		"value": 10.0,
	}, nil)

	err := bulk.Run()
	assert.NoError(t, err)

	ts, err := tsc.Fetch(now.Add(-1*time.Second), now.Add(1*time.Second), "value", nil)
	assert.NoError(t, err)
	assert.Equal(t, now.Truncate(time.Second), ts.Points[0].Timestamp)
	assert.Equal(t, 10.0, ts.Points[0].Max)
	assert.Equal(t, 10.0, ts.Points[0].Min)
	assert.Equal(t, 1, ts.Points[0].Num)
	assert.Equal(t, 10.0, ts.Points[0].Total)
}
