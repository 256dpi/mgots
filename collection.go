// Package mgots is a wrapper for mgo that turns MongoDB into a time series
// database.
package mgots

import (
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

// A Bulk represents an operation that can be used to add multiple metrics at
// once. It is a wrapper around the mgo.Bulk type.
type Bulk struct {
	coll *Collection
	bulk *mgo.Bulk
}

// Insert will queue the insert in the bulk operation.
func (b *Bulk) Insert(timestamp time.Time, metrics map[string]float64, tags bson.M) {
	b.bulk.Upsert(b.coll.upsertSample(timestamp, metrics, tags))
}

// Run will insert all queued insert operations.
func (b *Bulk) Run() error {
	_, err := b.bulk.Run()
	return err
}

// A Collection represents a time series enabled collection. It is a wrapper
// around the mgo.Collection type.
type Collection struct {
	coll *mgo.Collection
	res  Resolution
}

// Wrap will take a mgo.Collection and return a Collection.
func Wrap(coll *mgo.Collection, res Resolution) *Collection {
	return &Collection{
		coll: coll,
		res:  res,
	}
}

// Insert will immediately write the specified metrics to the collection.
func (c *Collection) Insert(timestamp time.Time, metrics map[string]float64, tags bson.M) error {
	_, err := c.coll.Upsert(c.upsertSample(timestamp, metrics, tags))
	return err
}

// Bulk will return a new bulk operation.
func (c *Collection) Bulk() *Bulk {
	// create new bulk operation
	bulk := c.coll.Bulk()
	bulk.Unordered()

	return &Bulk{coll: c, bulk: bulk}
}

func (c *Collection) upsertSample(t time.Time, metrics map[string]float64, tags bson.M) (bson.M, bson.M) {
	// get set start and name key
	start, key := c.res.Split(t)

	// prepare query
	query := bson.M{
		"start": start,
		"tags":  tags,
	}

	// prepare update
	update := bson.M{
		"$set": bson.M{},
		"$inc": bson.M{},
		"$max": bson.M{},
		"$min": bson.M{},
	}

	// add statements
	for name, value := range metrics {
		update["$set"].(bson.M)["samples."+key+".start"] = c.res.Join(start, key)
		update["$inc"].(bson.M)["samples."+key+"."+name+".total"] = value
		update["$inc"].(bson.M)["samples."+key+"."+name+".num"] = 1
		update["$max"].(bson.M)["samples."+key+"."+name+".max"] = value
		update["$min"].(bson.M)["samples."+key+"."+name+".min"] = value
		update["$inc"].(bson.M)["total."+name] = value
		update["$inc"].(bson.M)["num."+name] = 1
		update["$max"].(bson.M)["max."+name] = value
		update["$min"].(bson.M)["min."+name] = value
	}

	return query, update
}

// AggregateSamples will aggregate all samples within sets that match the
// specified time range and tags.
func (c *Collection) AggregateSamples(first, last time.Time, metrics []string, tags bson.M) (*TimeSeries, error) {
	// get first and last sample
	firstSample := c.res.SampleTimestamp(first)
	lastSample := c.res.SampleTimestamp(last)

	// prepare aggregation pipeline
	pipeline := []bson.M{
		// get all matching sets
		{
			"$match": c.matchSets(firstSample, lastSample, tags),
		},
		// turn samples into an array
		{
			"$addFields": bson.M{
				"samples": bson.M{"$objectToArray": "$samples"},
			},
		},
		// create a document for each sample
		{
			"$unwind": "$samples",
		},
		// make the sample the main document
		{
			"$replaceRoot": bson.M{"newRoot": "$samples.v"},
		},
		// match the exact time range
		{
			"$match": bson.M{
				"start": bson.M{
					"$gte": firstSample,
					"$lte": lastSample,
				},
			},
		},
		// group samples
		{
			"$group": bson.M{
				"_id": "$start",
				// more fields added below
			},
		},
		// finalize layout
		{
			"$project": bson.M{
				"_id":     false,
				"start":   "$_id",
				"metrics": bson.M{
				// fields added below
				},
			},
		},
		// sort samples
		{
			"$sort": bson.M{"start": 1},
		},
	}

	// update pipeline
	for _, name := range metrics {
		// add group fields
		pipeline[5]["$group"].(bson.M)["max_"+name] = bson.M{"$max": "$" + name + ".max"}
		pipeline[5]["$group"].(bson.M)["min_"+name] = bson.M{"$min": "$" + name + ".min"}
		pipeline[5]["$group"].(bson.M)["num_"+name] = bson.M{"$sum": "$" + name + ".num"}
		pipeline[5]["$group"].(bson.M)["total_"+name] = bson.M{"$sum": "$" + name + ".total"}

		// add project fields
		pipeline[6]["$project"].(bson.M)["metrics"].(bson.M)[name] = bson.M{
			"max":   "$max_" + name,
			"min":   "$min_" + name,
			"num":   "$num_" + name,
			"total": "$total_" + name,
		}
	}

	// fetch result
	var samples []Sample
	err := c.coll.Pipe(pipeline).All(&samples)
	if err != nil {
		return nil, err
	}

	return &TimeSeries{samples}, nil
}

// AggregateSets will aggregate only set level metrics matching the specified
// time range and tags.
func (c *Collection) AggregateSets(first, last time.Time, metrics []string, tags bson.M) (*TimeSeries, error) {
	// prepare aggregation pipeline
	pipeline := []bson.M{
		// get all matching sets
		{
			"$match": c.matchSets(first, last, tags),
		},
		// group samples
		{
			"$group": bson.M{
				"_id": "$start",
				// more fields added below
			},
		},
		// finalize layout
		{
			"$project": bson.M{
				"_id":     false,
				"start":   "$_id",
				"metrics": bson.M{
				// fields added below
				},
			},
		},
		// sort samples
		{
			"$sort": bson.M{"start": 1},
		},
	}

	// update pipeline
	for _, name := range metrics {
		// add group fields
		pipeline[1]["$group"].(bson.M)["max_"+name] = bson.M{"$max": "$max." + name}
		pipeline[1]["$group"].(bson.M)["min_"+name] = bson.M{"$min": "$min." + name}
		pipeline[1]["$group"].(bson.M)["num_"+name] = bson.M{"$sum": "$num." + name}
		pipeline[1]["$group"].(bson.M)["total_"+name] = bson.M{"$sum": "$total." + name}

		// add project fields
		pipeline[2]["$project"].(bson.M)["metrics"].(bson.M)[name] = bson.M{
			"max":   "$max_" + name,
			"min":   "$min_" + name,
			"num":   "$num_" + name,
			"total": "$total_" + name,
		}
	}

	// fetch result
	var samples []Sample
	err := c.coll.Pipe(pipeline).All(&samples)
	if err != nil {
		return nil, err
	}

	return &TimeSeries{samples}, nil
}

func (c *Collection) matchSets(first, last time.Time, tags bson.M) bson.M {
	// get first and last set
	firstSet, _ := c.res.Split(first)
	lastSet, _ := c.res.Split(last)

	// create basic matcher
	match := bson.M{
		"start": bson.M{
			"$gte": firstSet,
			"$lte": lastSet,
		},
	}

	// add tags
	for key, value := range tags {
		match["tags."+key] = value
	}

	return match
}

// EnsureIndexes will ensure that the necessary indexes have been created. If
// removeAfter is specified, sets are automatically removed when their start
// timestamp falls behind the specified duration.
func (c *Collection) EnsureIndexes(removeAfter time.Duration) error {
	// ensure start index
	err := c.coll.EnsureIndex(mgo.Index{
		Key:         []string{"start"},
		ExpireAfter: removeAfter,
	})
	if err != nil {
		return err
	}

	// ensure tags index
	err = c.coll.EnsureIndex(mgo.Index{
		Key: []string{"tags"},
	})
	if err != nil {
		return err
	}

	return nil
}
