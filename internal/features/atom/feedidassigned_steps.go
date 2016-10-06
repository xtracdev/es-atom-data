package atom

import (
	. "github.com/gucumber/gucumber"
	"github.com/stretchr/testify/assert"
	ad "github.com/xtracdev/es-atom-data"
	"github.com/xtracdev/goes"
	"github.com/xtracdev/orapub"
	"os"
)

func init() {
	var atomProcessor orapub.EventProcessor
	var initFailure bool
	_, db, err := initializeEnvironment()
	if err != nil {
		initFailure = false
	}

	Given(`^some initial events and no archived events and no feeds$`, func() {
		if assert.False(T, initFailure) {
			return
		}

		if assert.False(T, initFailure) {
			return
		}

		atomProcessor = ad.NewESAtomPubProcessor()
		err := atomProcessor.Initialize(db)
		assert.Nil(T, err)

		_, err = db.Exec("delete from recent")
		assert.Nil(T, err)
		_, err = db.Exec("delete from archive")
		assert.Nil(T, err)
		_, err = db.Exec("delete from feeds")
		assert.Nil(T, err)

		eventPtr := &goes.Event{
			Source:   "agg1",
			Version:  1,
			TypeCode: "foo",
			Payload:  []byte("ok"),
		}
		atomProcessor.Processor(db, eventPtr)

		eventPtr = &goes.Event{
			Source:   "agg2",
			Version:  1,
			TypeCode: "foo",
			Payload:  []byte("ok?"),
		}
		atomProcessor.Processor(db, eventPtr)

	})

	When(`^the feed page threshold is reached$`, func() {
		os.Setenv("FEED_THRESHOLD", "2")
		ad.ReadFeedThresholdFromEnv()
		assert.Equal(T, 2, ad.FeedThreshold)
	})
}
