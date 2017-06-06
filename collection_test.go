package mgots

import (
	"testing"
	"time"

	"github.com/influxdata/influxdb/client/v2"
	"github.com/stretchr/testify/assert"
	"gopkg.in/mgo.v2"
)

var db *mgo.Database

func init() {
	sess, err := mgo.Dial("mongodb://localhost/mgots-test")
	if err != nil {
		panic(err)
	}

	db = sess.DB("")

	err = db.DropDatabase()
	if err != nil {
		panic(err)
	}
}

func TestCollectionInsert(t *testing.T) {
	coll := C(db.C("test1"), Second)

	now := time.Now()

	err := coll.Insert("test", 10.0, now, nil)
	assert.NoError(t, err)

	ts, err := coll.Fetch("test", now.Add(-1*time.Second), now.Add(1*time.Second), nil)
	assert.NoError(t, err)
	assert.Equal(t, 10.0, ts.Points[0].Value)
}

func BenchmarkInfluxDBBatch(b *testing.B) {
	influx, err := client.NewHTTPClient(client.HTTPConfig{
		Addr: "http://localhost:8086",
	})
	if err != nil {
		panic(err)
	}

	_, err = influx.Query(client.Query{
		Command:   "DROP SERIES FROM bench",
		Database:  "mgots",
		Precision: "s",
	})
	if err != nil {
		panic(err)
	}

	batch, err := client.NewBatchPoints(client.BatchPointsConfig{
		Precision: "s",
		Database:  "mgots",
	})
	if err != nil {
		panic(err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		point, err := client.NewPoint("bench", nil, map[string]interface{}{
			"value": float64(i),
		}, time.Now())
		if err != nil {
			panic(err)
		}

		batch.AddPoint(point)

		if i%200 == 0 {
			err = influx.Write(batch)
			if err != nil {
				panic(err)
			}

			batch, err = client.NewBatchPoints(client.BatchPointsConfig{
				Precision: "s",
				Database:  "mgots",
			})
			if err != nil {
				panic(err)
			}
		}
	}
}

func BenchmarkCollectionInsert(b *testing.B) {
	coll := C(db.C("bench1"), Second)

	err := coll.EnsureIndexes()
	if err != nil {
		panic(err)
	}

	b.ResetTimer()

	now := time.Now()
	for i := 0; i < b.N; i++ {
		err = coll.Insert("bench", float64(i), now, nil)
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkCollectionAdd(b *testing.B) {
	coll := C(db.C("bench2"), Second)

	err := coll.EnsureIndexes()
	if err != nil {
		panic(err)
	}

	bulk := db.C("bench2").Bulk()

	b.ResetTimer()

	now := time.Now()
	for i := 0; i < b.N; i++ {
		coll.Add(bulk, "bench", float64(i), now, nil)

		if i%200 == 0 {
			_, err := bulk.Run()
			if err != nil {
				panic(err)
			}

			bulk = db.C("bench2").Bulk()
		}
	}
}
