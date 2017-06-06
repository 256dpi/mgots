package mgots

import (
	"testing"
	"time"

	"github.com/influxdata/influxdb/client/v2"
)

func BenchmarkInfluxDBBatch(b *testing.B) {
	b.ReportAllocs()

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
	b.ReportAllocs()

	coll := C(db.C("bench1"), Second)

	err := coll.EnsureIndexes()
	if err != nil {
		panic(err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err = coll.Insert("bench", float64(i), time.Now(), nil)
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkCollectionAdd(b *testing.B) {
	b.ReportAllocs()

	coll := C(db.C("bench2"), Second)

	err := coll.EnsureIndexes()
	if err != nil {
		panic(err)
	}

	bulk := db.C("bench2").Bulk()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		coll.Add(bulk, "bench", float64(i), time.Now(), nil)

		if i%200 == 0 {
			_, err := bulk.Run()
			if err != nil {
				panic(err)
			}

			bulk = db.C("bench2").Bulk()
		}
	}
}
