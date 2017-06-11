package mgots

import (
	"testing"
	"time"
)

func BenchmarkCollectionInsert(b *testing.B) {
	b.ReportAllocs()

	coll := Wrap(db.C("bench-coll-insert"), OneMinuteOf60Seconds)

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

	coll := Wrap(db.C("bench-coll-bulk-insert"), OneMinuteOf60Seconds)

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

func BenchmarkCollectionBulkInsert1000(b *testing.B) {
	b.ReportAllocs()

	coll := Wrap(db.C("bench-coll-bulk-insert-1000"), OneMinuteOf60Seconds)

	err := coll.EnsureIndexes(0)
	if err != nil {
		panic(err)
	}

	bulk := coll.Bulk()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for j := 0; j < 1000; j++ {
			bulk.Insert(time.Now(), map[string]float64{
				"value": float64(j),
			}, nil)
		}

		err := bulk.Run()
		if err != nil {
			panic(err)
		}

		bulk = coll.Bulk()
	}
}

func BenchmarkCollectionAggregateSamples(b *testing.B) {
	b.ReportAllocs()

	coll := Wrap(db.C("bench-coll-aggregate-samples"), OneMinuteOf60Seconds)

	err := coll.EnsureIndexes(0)
	if err != nil {
		panic(err)
	}

	bulk := coll.Bulk()

	now := time.Now()

	for i := 0; i < 3600; i++ {
		bulk.Insert(now.Add(time.Duration(i)*time.Second), map[string]float64{
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

	err = bulk.Run()
	if err != nil {
		panic(err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := coll.AggregateSamples(now, now.Add(1*time.Hour), []string{"value"}, nil)
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkCollectionAggregateSets(b *testing.B) {
	b.ReportAllocs()

	coll := Wrap(db.C("bench-coll-aggregate-sets"), OneMinuteOf60Seconds)

	err := coll.EnsureIndexes(0)
	if err != nil {
		panic(err)
	}

	bulk := coll.Bulk()

	now := time.Now()

	for i := 0; i < 3600; i++ {
		bulk.Insert(now.Add(time.Duration(i)*time.Second), map[string]float64{
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

	err = bulk.Run()
	if err != nil {
		panic(err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := coll.AggregateSets(now, now.Add(1*time.Hour), []string{"value"}, nil)
		if err != nil {
			panic(err)
		}
	}
}
