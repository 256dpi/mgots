package mgots

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCollectionInsert(t *testing.T) {
	coll := C(db.C("test1"), Second)

	now := time.Now()

	err := coll.Insert("test", 10.0, now, nil)
	assert.NoError(t, err)

	ts, err := coll.Fetch("test", now.Add(-1*time.Second), now.Add(1*time.Second), nil)
	assert.NoError(t, err)
	assert.Equal(t, 10.0, ts.Points[0].Value)
}
