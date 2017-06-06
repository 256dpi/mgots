package mgots

import (
	"fmt"
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

func Example() {
	// connect to database
	sess, err := mgo.Dial("mongodb://localhost/mgots-test")
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
	coll := C(db.C("metrics.hourly"), Second)

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
		coll.Insert("requests", float64(i), to, tags)
		to = to.Add(time.Second)
	}

	// get avg
	avg, err := coll.Avg("requests", from, to, tags)
	if err != nil {
		panic(err)
	}

	// get min
	min, err := coll.Min("requests", from, to, tags)
	if err != nil {
		panic(err)
	}

	// get max
	max, err := coll.Max("requests", from, to, tags)
	if err != nil {
		panic(err)
	}

	// fetch timeSeries
	timeSeries, err := coll.Fetch("requests", from.Add(10*time.Second), to.Add(-10*time.Second), tags)
	if err != nil {
		panic(err)
	}

	// print
	fmt.Println(min)
	fmt.Println(avg)
	fmt.Println(max)
	fmt.Println(len(timeSeries.Points))
	fmt.Println(timeSeries.Min())
	fmt.Println(timeSeries.Avg())
	fmt.Println(timeSeries.Max())
	fmt.Println(timeSeries.Points[9].Value)

	// Output:
	// 0
	// 30
	// 60
	// 41
	// 11
	// 31
	// 51
	// 20
}
