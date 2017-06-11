# mgots
    
[![Build Status](https://travis-ci.org/256dpi/mgots.svg?branch=master)](https://travis-ci.org/256dpi/mgots)
[![Coverage Status](https://coveralls.io/repos/github/256dpi/mgots/badge.svg?branch=master)](https://coveralls.io/github/256dpi/mgots?branch=master)
[![GoDoc](https://godoc.org/github.com/256dpi/mgots?status.svg)](http://godoc.org/github.com/256dpi/mgots)
[![Release](https://img.shields.io/github/release/256dpi/mgots.svg)](https://github.com/256dpi/mgots/releases)
[![Go Report Card](https://goreportcard.com/badge/github.com/256dpi/mgots)](https://goreportcard.com/report/github.com/256dpi/mgots)

**A wrapper for [mgo](https://github.com/go-mgo/mgo) that turns MongoDB into a time series database.**

## Example

```go
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
```
