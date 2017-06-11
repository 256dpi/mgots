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

func (c *Collection) upsertSample(timestamp time.Time, metrics map[string]float64, tags bson.M) (bson.M, bson.M) {
	// get set start and name key
	start, key := c.res.Split(timestamp)

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

// Avg returns the average value for the given range.
//
// Note: This function will operate over full sets.
func (c *Collection) Avg(start, end time.Time, metric string, tags bson.M) (float64, error) {
	// create aggregation pipeline
	pipe := c.coll.Pipe([]bson.M{
		{
			"$match": c.matchSets(start, end, tags),
		},
		{
			"$group": bson.M{
				"_id": nil,
				"num": bson.M{
					"$sum": "$num." + metric,
				},
				"total": bson.M{
					"$sum": "$total." + metric,
				},
			},
		},
	})

	// fetch result
	var res bson.M
	err := pipe.One(&res)
	if err != nil {
		return 0, err
	}

	// calculate average
	avg := res["total"].(float64) / float64(res["num"].(int))

	return avg, nil
}

// Min returns the minimum value for the given range.
//
// Note: This function will operate over full sets.
func (c *Collection) Min(start, end time.Time, metric string, tags bson.M) (float64, error) {
	return c.minMax("min", start, end, metric, tags)
}

// Max returns the maximum for the given range.
//
// Note: This function will operate over full sets.
func (c *Collection) Max(start, end time.Time, metric string, tags bson.M) (float64, error) {
	return c.minMax("max", start, end, metric, tags)
}

func (c *Collection) minMax(method string, start, end time.Time, metric string, tags bson.M) (float64, error) {
	// create aggregation pipeline
	pipe := c.coll.Pipe([]bson.M{
		{
			"$match": c.matchSets(start, end, tags),
		},
		{
			"$group": bson.M{
				"_id": nil,
				method: bson.M{
					"$" + method: "$" + method + "." + metric,
				},
			},
		},
	})

	// fetch result
	var res bson.M
	err := pipe.One(&res)
	if err != nil {
		return 0, err
	}

	return res[method].(float64), nil
}

// TODO: AggregateSamples should handle multiple metrics.

// TODO: Support some kind of additional grouping during aggregation?

// AggregateSamples will aggregate all samples within sets that match the
// specified time range and tags.
func (c *Collection) AggregateSamples(start, end time.Time, metric string, tags bson.M) (*TimeSeries, error) {
	// create aggregation pipeline
	pipeline := []bson.M{
		// get all matching sets
		{
			"$match": c.matchSets(start, end, tags),
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
					"$gte": start,
					"$lte": end,
				},
			},
		},
		// group samples
		{
			"$group": bson.M{
				"_id":   "$start",
				"max":   bson.M{"$max": "$" + metric + ".max"},
				"min":   bson.M{"$min": "$" + metric + ".min"},
				"num":   bson.M{"$sum": "$" + metric + ".num"},
				"total": bson.M{"$sum": "$" + metric + ".total"},
			},
		},
		// finalize layout
		{
			"$project": bson.M{
				"_id":   false,
				"start": "$_id",
				"max":   true,
				"min":   true,
				"num":   true,
				"total": true,
			},
		},
		// sort samples
		{
			"$sort": bson.M{"start": 1},
		},
	}

	// fetch result
	var samples []Sample
	err := c.coll.Pipe(pipeline).All(&samples)
	if err != nil {
		return nil, err
	}

	return &TimeSeries{
		Start:   start,
		End:     end,
		Samples: samples,
	}, nil
}

// AggregateSets will aggregate only set level metrics matching the specified
// time range and tags.
func (c *Collection) AggregateSets(start, end time.Time, metric string, tags bson.M) (*TimeSeries, error) {
	// create aggregation pipeline
	pipeline := []bson.M{
		// get all matching sets
		{
			"$match": c.matchSets(start, end, tags),
		},
		// group samples
		{
			"$group": bson.M{
				"_id":   "$start",
				"max":   bson.M{"$max": "$" + "max." + metric},
				"min":   bson.M{"$min": "$" + "min." + metric},
				"num":   bson.M{"$sum": "$" + "num." + metric},
				"total": bson.M{"$sum": "$" + "total." + metric},
			},
		},
		// finalize layout
		{
			"$project": bson.M{
				"_id":   false,
				"start": "$_id",
				"max":   true,
				"min":   true,
				"num":   true,
				"total": true,
			},
		},
		// sort samples
		{
			"$sort": bson.M{"start": 1},
		},
	}

	// fetch result
	var samples []Sample
	err := c.coll.Pipe(pipeline).All(&samples)
	if err != nil {
		return nil, err
	}

	return &TimeSeries{
		Start:   start,
		End:     end,
		Samples: samples,
	}, nil
}

func (c *Collection) matchSets(start, end time.Time, tags bson.M) bson.M {
	// get first and last set start point
	setStart, _ := c.res.Split(start)
	setEnd, _ := c.res.Split(end)

	// create basic matcher
	match := bson.M{
		"start": bson.M{
			"$gte": setStart,
			"$lte": setEnd,
		},
	}

	// add tags
	for key, value := range tags {
		match["tags."+key] = value
	}

	return match
}

// TODO: Support TTL indexes for automatic removal?

// EnsureIndexes will ensure that the necessary indexes have been created.
func (c *Collection) EnsureIndexes() error {
	// ensure start index
	err := c.coll.EnsureIndex(mgo.Index{
		Key: []string{"start"},
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
