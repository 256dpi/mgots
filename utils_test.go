package mgots

import (
	"encoding/json"
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

func jsonString(val interface{}) string {
	// marshal as json
	buf, err := json.Marshal(val)
	if err != nil {
		panic(err)
	}

	return string(buf)
}

func parseTime(str string) time.Time {
	t, err := time.Parse(time.Stamp, str)
	if err != nil {
		panic(err)
	}

	return t
}
