package mgots

import (
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

var db *mgo.Database

func init() {
	// create session
	sess, err := mgo.Dial("mongodb://localhost/mgots")
	if err != nil {
		panic(err)
	}

	// save db reference
	db = sess.DB("")

	// drop database
	err = db.DropDatabase()
	if err != nil {
		panic(err)
	}

	// force recreation
	err = db.C("foo").Insert(bson.M{"foo": "bar"})
	if err != nil {
		panic(err)
	}
}

func parseTime(str string) time.Time {
	t, err := time.Parse(time.Stamp, str)
	if err != nil {
		panic(err)
	}

	t = t.AddDate(2017, 0, 0)

	return t.UTC()
}

func forceUTCSlice(s []bson.M) []bson.M {
	for _, m := range s {
		forceUTCMap(m)
	}

	return s
}

func forceUTCMap(m bson.M) bson.M {
	for key, value := range m {
		if v, ok := value.(bson.M); ok {
			forceUTCMap(v)
		} else if v, ok := value.([]bson.M); ok {
			forceUTCSlice(v)
		} else if v, ok := value.(time.Time); ok {
			m[key] = v.UTC()
		}
	}

	return m
}

func forceUTCTimeSeries(ts *TimeSeries) *TimeSeries {
	ts.Start = ts.Start.UTC()
	ts.End = ts.End.UTC()

	for i, s := range ts.Samples {
		ss := s
		ss.Start = ss.Start.UTC()
		ts.Samples[i] = ss
	}

	return ts
}
