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

	// get data
	ts, err := coll.AggregateSamples(from.Add(10*time.Second), to.Add(-10*time.Second), []string{"value"}, tags)
	if err != nil {
		panic(err)
	}

	// print
	fmt.Println(len(ts.Samples))
	fmt.Println(ts.Min("value"))
	fmt.Println(ts.Avg("value"))
	fmt.Println(ts.Max("value"))

	// Output:
	// 41
	// 11
	// 31
	// 51
}
