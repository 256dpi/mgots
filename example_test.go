package mgots

import (
	"fmt"
	"time"

	"github.com/globalsign/mgo/bson"
)

func Example() {
	// get time series collection
	coll := Wrap(db.C("metrics"), OneMinuteOf60Seconds)

	// ensure indexes
	err := coll.EnsureIndexes(0)
	if err != nil {
		panic(err)
	}

	// prepare tags
	tags := bson.M{"server": "localhost"}

	// add some metrics
	from := time.Now()
	to := time.Now()
	for i := 0; i < 100; i++ {
		coll.Insert(to, map[string]float64{
			"value": float64(i),
		}, tags)

		to = to.Add(time.Second)
	}

	// get data
	ts, err := coll.AggregateSamples(from, to, []string{"value"}, tags)
	if err != nil {
		panic(err)
	}

	// print
	fmt.Println(ts.Num("value"))
	fmt.Println(ts.Sum("value"))
	fmt.Println(ts.Min("value"))
	fmt.Println(ts.Max("value"))
	fmt.Println(ts.Avg("value"))

	// Output:
	// 100
	// 4950
	// 0
	// 99
	// 49.5
}
