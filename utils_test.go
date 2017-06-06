package mgots

import (
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

var db *mgo.Database

func init() {
	// create session
	sess, err := mgo.Dial("mongodb://localhost/mgots-test")
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
	err = db.C("foo").Insert(bson.M{"foo":"bar"})
	if err != nil {
		panic(err)
	}
}

