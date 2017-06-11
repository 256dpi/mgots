package mgots

import (
	"fmt"
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

func Example() {
	// connect to database
	sess, err := mgo.Dial("mongodb://localhost/mgots")
	if err != nil {
		panic(err)
	}

	// get db
	db := sess.DB("")

	// clean database
	err = db.DropDatabase()
	if err != nil {
		panic(err)
	}

	// get time series collection
	coll := Wrap(db.C("metrics.hourly"), Second)

	// ensure indexes
	err = coll.EnsureIndexes()
	if err != nil {
		panic(err)
	}

	// prepare tags
	tags := bson.M{"server": "localhost"}

	// add some metrics
	from := time.Now()
	to := time.Now()
	for i := 0; i <= 60; i++ {
		coll.Insert(to, map[string]float64{
			"value": float64(i),
		}, tags)
		to = to.Add(time.Second)
	}

	// get avg
	avg, err := coll.Avg(from, to, "value", tags)
	if err != nil {
		panic(err)
	}

	// get min
	min, err := coll.Min(from, to, "value", tags)
	if err != nil {
		panic(err)
	}

	// get max
	max, err := coll.Max(from, to, "value", tags)
	if err != nil {
		panic(err)
	}

	// get data
	ts, err := coll.Aggregate(from.Add(10*time.Second), to.Add(-10*time.Second), "value", tags)
	if err != nil {
		panic(err)
	}

	// print
	fmt.Println(min)
	fmt.Println(avg)
	fmt.Println(max)
	fmt.Println(len(ts.Samples))
	fmt.Println(ts.Min())
	fmt.Println(ts.Avg())
	fmt.Println(ts.Max())

	// Output:
	// 0
	// 30
	// 60
	// 41
	// 11
	// 31
	// 51
}
