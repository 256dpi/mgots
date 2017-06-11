package mgots

import (
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

// A Bulk operation can be used to add multiple samples at once.
type Bulk struct {
	coll *Collection
	bulk *mgo.Bulk
}

// Add will add the insert command to the passed Bulk operation.
func (b *Bulk) Add(timestamp time.Time, samples map[string]float64, tags bson.M) {
	b.bulk.Upsert(b.coll.upsertSample(timestamp, samples, tags))
}

// Run will insert the added operations.
func (b *Bulk) Run() error {
	_, err := b.bulk.Run()
	return err
}

// A Collection represents a time series enabled collection.
type Collection struct {
	coll *mgo.Collection
	res  Resolution
}

// Wrap will take a mgo.Collection and return a mgots.Collection.
func Wrap(coll *mgo.Collection, res Resolution) *Collection {
	return &Collection{
		coll: coll,
		res:  res,
	}
}

// Insert will write a new sample to the collection.
func (c *Collection) Insert(timestamp time.Time, samples map[string]float64, tags bson.M) error {
	_, err := c.coll.Upsert(c.upsertSample(timestamp, samples, tags))
	return err
}

// Bulk will return a wrapped bulk operation.
func (c *Collection) Bulk() *Bulk {
	// create new bulk operation
	bulk := c.coll.Bulk()
	bulk.Unordered()

	return &Bulk{coll: c, bulk: bulk}
}

func (c *Collection) upsertSample(timestamp time.Time, samples map[string]float64, tags bson.M) (bson.M, bson.M) {
	// get set start and field key
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
	for field, value := range samples {
		update["$set"].(bson.M)["samples."+key+".start"] = c.res.Join(start, key)
		update["$inc"].(bson.M)["samples."+key+"."+field+".total"] = value
		update["$inc"].(bson.M)["samples."+key+"."+field+".num"] = 1
		update["$max"].(bson.M)["samples."+key+"."+field+".max"] = value
		update["$min"].(bson.M)["samples."+key+"."+field+".min"] = value
		update["$inc"].(bson.M)["total."+field] = value
		update["$inc"].(bson.M)["num."+field] = 1
		update["$max"].(bson.M)["max."+field] = value
		update["$min"].(bson.M)["min."+field] = value
	}

	return query, update
}

// Avg returns the average value for the given range.
//
// Note: This function will operate over full sets.
func (c *Collection) Avg(start, end time.Time, field string, tags bson.M) (float64, error) {
	// create aggregation pipeline
	pipe := c.coll.Pipe([]bson.M{
		{
			"$match": c.matchSets(start, end, tags),
		},
		{
			"$group": bson.M{
				"_id": nil,
				"num": bson.M{
					"$sum": "$num." + field,
				},
				"total": bson.M{
					"$sum": "$total." + field,
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
func (c *Collection) Min(start, end time.Time, field string, tags bson.M) (float64, error) {
	return c.minMax("min", start, end, field, tags)
}

// Max returns the maximum for the given range.
//
// Note: This function will operate over full sets.
func (c *Collection) Max(start, end time.Time, field string, tags bson.M) (float64, error) {
	return c.minMax("max", start, end, field, tags)
}

func (c *Collection) minMax(method string, start, end time.Time, field string, tags bson.M) (float64, error) {
	// create aggregation pipeline
	pipe := c.coll.Pipe([]bson.M{
		{
			"$match": c.matchSets(start, end, tags),
		},
		{
			"$group": bson.M{
				"_id": nil,
				method: bson.M{
					"$" + method: "$" + method + "." + field,
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

// TODO: AggregateSamples should handle multiple fields.

// TODO: Support some kind of additional grouping during aggregation?

// AggregateSamples will aggregate all samples that match the specified time
// range and tags.
func (c *Collection) AggregateSamples(start, end time.Time, field string, tags bson.M) (*TimeSeries, error) {
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
				"max":   bson.M{"$max": "$" + field + ".max"},
				"min":   bson.M{"$min": "$" + field + ".min"},
				"num":   bson.M{"$sum": "$" + field + ".num"},
				"total": bson.M{"$sum": "$" + field + ".total"},
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

// AggregateSets will aggregate all sets matching the specified parameters and
// return a time series.
func (c *Collection) AggregateSets(start, end time.Time, field string, tags bson.M) (*TimeSeries, error) {
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
				"max":   bson.M{"$max": "$" + "max." + field},
				"min":   bson.M{"$min": "$" + "min." + field},
				"num":   bson.M{"$sum": "$" + "num." + field},
				"total": bson.M{"$sum": "$" + "total." + field},
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
