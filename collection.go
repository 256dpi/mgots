package mgots

import (
	"sort"
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

// TODO: Support aggregation for multiple tag combinations.

// TODO: Support querying by fields and tags.

// TODO: Support joining time series?

// TODO: Support some kind of grouping?

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

// Insert will write a new point to the collection.
func (c *Collection) Insert(value float64, timestamp time.Time, tags bson.M) error {
	_, err := c.coll.Upsert(c.selectAndUpdate(value, timestamp, tags))
	return err
}

// Add will add the insert command to the passed Bulk operation.
func (c *Collection) Add(bulk *mgo.Bulk, value float64, timestamp time.Time, tags bson.M) {
	bulk.Upsert(c.selectAndUpdate(value, timestamp, tags))
}

func (c *Collection) selectAndUpdate(value float64, timestamp time.Time, tags bson.M) (bson.M, bson.M) {
	// get batch start and sample key
	start, key := c.res.Split(timestamp)

	return bson.M{
			"start": start,
			"tags":  tags,
		}, bson.M{
			"$inc": bson.M{
				"samples." + key + ".total": value,
				"samples." + key + ".num":   1,
				"num":   1,
				"total": value,
			},
			"$max": bson.M{
				"samples." + key + ".max": value,
				"max": value,
			},
			"$min": bson.M{
				"samples." + key + ".min": value,
				"min": value,
			},
		}
}

// Avg returns the average value for the given range.
//
// Note: This function will operate over full batches.
func (c *Collection) Avg(start, end time.Time, tags bson.M) (float64, error) {
	// create aggregation pipeline
	pipe := c.coll.Pipe([]bson.M{
		{
			"$match": c.batchMatcher(start, end, tags),
		},
		{
			"$group": bson.M{
				"_id": nil,
				"num": bson.M{
					"$sum": "$num",
				},
				"total": bson.M{
					"$sum": "$total",
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
// Note: This function will operate over full batches.
func (c *Collection) Min(start, end time.Time, tags bson.M) (float64, error) {
	return c.minMax("min", start, end, tags)
}

// Max returns the maximum for the given range.
//
// Note: This function will operate over full batches.
func (c *Collection) Max(start, end time.Time, tags bson.M) (float64, error) {
	return c.minMax("max", start, end, tags)
}

func (c *Collection) minMax(method string, start, end time.Time, tags bson.M) (float64, error) {
	// create aggregation pipeline
	pipe := c.coll.Pipe([]bson.M{
		{
			"$match": c.batchMatcher(start, end, tags),
		},
		{
			"$group": bson.M{
				"_id": nil,
				method: bson.M{
					"$" + method: "$" + method,
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

// A Sample is a single sample in a Batch.
type Sample struct {
	Max   float64
	Min   float64
	Num   int
	Total float64
}

// A Batch is a list of Samples and a Sample itself.
type Batch struct {
	Sample
	Name    string
	Start   time.Time
	Tags    bson.M
	Samples map[string]Sample
}

// Fetch will load all points and construct and return a time series.
func (c *Collection) Fetch(start, end time.Time, tags bson.M) (*TimeSeries, error) {
	// load all batches matching in the provided time range
	var batches []Batch
	err := c.coll.Find(c.batchMatcher(start, end, tags)).All(&batches)
	if err != nil {
		return nil, err
	}

	// allocated a slice of points
	points := make([]Point, 0, c.res.BatchSize()*len(batches))

	// iterate through all batches
	for _, batch := range batches {
		// iterate through all samples in a batch
		for key, sample := range batch.Samples {
			// get original timestamp of the sample
			timestamp := c.res.Join(batch.Start, key)

			// add point if timestamps is in the requested time range
			if (timestamp.Equal(start) || timestamp.After(start)) && timestamp.Before(end) {
				points = append(points, Point{
					Timestamp: timestamp,
					Sample:    sample,
				})
			}
		}
	}

	// sort points by time
	sort.Slice(points, func(i, j int) bool {
		return points[i].Timestamp.Before(points[j].Timestamp)
	})

	return &TimeSeries{
		Start:  start,
		End:    end,
		Points: points,
	}, nil
}

func (c *Collection) batchMatcher(start, end time.Time, tags bson.M) bson.M {
	// get first and last batch start point
	batchStart, _ := c.res.Split(start)
	batchEnd, _ := c.res.Split(end)

	// create basic matcher
	match := bson.M{
		"start": bson.M{
			"$gte": batchStart,
			"$lte": batchEnd,
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
