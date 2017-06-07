package mgots

import (
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
func (c *Collection) Insert(name string, value float64, timestamp time.Time, tags bson.M) error {
	_, err := c.coll.Upsert(c.selectAndUpdate(name, value, timestamp, tags))
	return err
}

// Add will add the insert command to the passed Bulk operation.
func (c *Collection) Add(bulk *mgo.Bulk, name string, value float64, timestamp time.Time, tags bson.M) {
	bulk.Upsert(c.selectAndUpdate(name, value, timestamp, tags))
}

func (c *Collection) selectAndUpdate(name string, value float64, timestamp time.Time, tags bson.M) (bson.M, bson.M) {
	start, key := c.res.extractStartAndKey(timestamp)

	return bson.M{
			"start": start,
			"name":  name,
			"tags":  tags,
		}, bson.M{
			"$inc": bson.M{
				"values." + key + ".total": value,
				"values." + key + ".num":   1,
				"num":   1,
				"total": value,
			},
			"$max": bson.M{
				"values." + key + ".max": value,
				"max": value,
			},
			"$min": bson.M{
				"values." + key + ".min": value,
				"min": value,
			},
		}
}

// Avg returns the average value for the given range.
//
// Note: This function will operate over full batches of the used resolution.
func (c *Collection) Avg(name string, from, to time.Time, tags bson.M) (float64, error) {
	pipe := c.coll.Pipe([]bson.M{
		{
			"$match": c.matchSeries(name, from, to, tags),
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

	var res bson.M
	err := pipe.One(&res)
	if err != nil {
		return 0, err
	}

	avg := res["total"].(float64) / float64(res["num"].(int))

	return avg, nil
}

// Min returns the minimum value for the given range.
//
// Note: This function will operate over full batches of the used resolution.
func (c *Collection) Min(name string, from, to time.Time, tags bson.M) (float64, error) {
	return c.minMax("min", name, from, to, tags)
}

// Max returns the maximum for the given range.
//
// Note: This function will operate over full batches of the used resolution.
func (c *Collection) Max(name string, from, to time.Time, tags bson.M) (float64, error) {
	return c.minMax("max", name, from, to, tags)
}

func (c *Collection) minMax(method string, name string, from, to time.Time, tags bson.M) (float64, error) {
	pipe := c.coll.Pipe([]bson.M{
		{
			"$match": c.matchSeries(name, from, to, tags),
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

	var res bson.M
	err := pipe.One(&res)
	if err != nil {
		return 0, err
	}

	return res[method].(float64), nil
}

// Fetch will load all points and construct and return a time series.
func (c *Collection) Fetch(name string, from, to time.Time, tags bson.M) (*TimeSeries, error) {
	var res []bson.M
	err := c.coll.Find(c.matchSeries(name, from, to, tags)).All(&res)
	if err != nil {
		return nil, err
	}

	points := make([]Point, 0, c.res.estimatedPoints()*len(res))

	for _, doc := range res {
		for key, value := range doc["values"].(bson.M) {
			m := value.(bson.M)
			timestamp := c.res.combineStartAndKey(doc["start"].(time.Time), key)

			if (timestamp.Equal(from) || timestamp.After(from)) && timestamp.Before(to) {
				total := m["total"].(float64)
				num := m["num"].(int)

				points = append(points, Point{
					Timestamp:  timestamp,
					Resolution: c.res,
					Value:      total / float64(num),
					Min:        m["min"].(float64),
					Max:        m["max"].(float64),
					Num:        num,
					Total:      total,
				})
			}
		}
	}

	return &TimeSeries{
		Points: sortPoints(points),
	}, nil
}

func (c *Collection) matchSeries(name string, from, to time.Time, tags bson.M) bson.M {
	_from, _ := c.res.extractStartAndKey(from)
	_to, _ := c.res.extractStartAndKey(to)

	match := bson.M{
		"name": name,
		"start": bson.M{
			"$gte": _from,
			"$lte": _to,
		},
	}

	for key, value := range tags {
		match["tags."+key] = value
	}

	return match
}

// TODO: Support TTL indexes for automatic removal?

// TODO: Also index tags?

func (c *Collection) EnsureIndexes() error {
	err := c.coll.EnsureIndex(mgo.Index{
		Key: []string{"name"},
	})
	if err != nil {
		return err
	}

	err = c.coll.EnsureIndex(mgo.Index{
		Key: []string{"start"},
	})
	if err != nil {
		return err
	}

	return nil
}
