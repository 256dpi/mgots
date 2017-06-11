package mgots

import (
	"testing"
	"time"
)

func BenchmarkCollectionInsert(b *testing.B) {
	b.ReportAllocs()

	coll := Wrap(db.C("bench1"), OneMinuteOf60Seconds)

	err := coll.EnsureIndexes(0)
	if err != nil {
		panic(err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err = coll.Insert(time.Now(), map[string]float64{
			"value": float64(i),
		}, nil)
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkCollectionBulkInsert(b *testing.B) {
	b.ReportAllocs()

	coll := Wrap(db.C("bench2"), OneMinuteOf60Seconds)

	err := coll.EnsureIndexes(0)
	if err != nil {
		panic(err)
	}

	bulk := coll.Bulk()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		bulk.Insert(time.Now(), map[string]float64{
			"value": float64(i),
		}, nil)

		if i%200 == 0 {
			err := bulk.Run()
			if err != nil {
				panic(err)
			}

			bulk = coll.Bulk()
		}
	}
}
