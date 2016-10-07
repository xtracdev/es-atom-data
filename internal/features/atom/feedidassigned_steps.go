package atom

import (
	. "github.com/gucumber/gucumber"
	"github.com/stretchr/testify/assert"
	ad "github.com/xtracdev/es-atom-data"
	"github.com/xtracdev/goes"
	"github.com/xtracdev/orapub"
	"os"
	//"database/sql"
	"database/sql"
	log "github.com/Sirupsen/logrus"
)

func init() {
	var atomProcessor orapub.EventProcessor
	var initFailed bool

	log.Info("Init test envionment")
	_, db, err := initializeEnvironment()
	if err != nil {
		log.Warnf("Failed environment init: %s", err.Error())
		initFailed = true
	}

	Given(`^some initial events and no feeds$`, func() {
		log.Info("check init")
		if initFailed {
			T.Errorf("Failed init")
			return
		}

		log.Info("Create atom pub processor")
		atomProcessor = ad.NewESAtomPubProcessor()
		err := atomProcessor.Initialize(db)
		assert.Nil(T, err, "Failed to initialize atom publisher")

		log.Info("clean out tables")
		_, err = db.Exec("delete from atom_event")
		assert.Nil(T, err)
		_, err = db.Exec("delete from feed")
		assert.Nil(T, err)

		log.Info("add some events")
		eventPtr := &goes.Event{
			Source:   "agg1",
			Version:  1,
			TypeCode: "foo",
			Payload:  []byte("ok"),
		}

		err = atomProcessor.Processor(db, eventPtr)
		assert.Nil(T, err)

	})

	When(`^the feed page threshold is reached$`, func() {
		os.Setenv("FEED_THRESHOLD", "2")
		ad.ReadFeedThresholdFromEnv()
		assert.Equal(T, 2, ad.FeedThreshold)

		eventPtr := &goes.Event{
			Source:   "agg2",
			Version:  1,
			TypeCode: "foo",
			Payload:  []byte("ok?"),
		}

		err = atomProcessor.Processor(db, eventPtr)
		assert.Nil(T, err)
	})

	Then(`^feed is updated with a new feedid with a null previous feed$`, func() {
		var count int
		err := db.QueryRow("select count(*) from feeds").Scan(&count)
		assert.Nil(T, err)
		assert.Equal(T, 1, count)

		var feedid sql.NullString
		err = db.QueryRow("select feedid from feed").Scan(&feedid)
		assert.Nil(T, err)
		assert.True(T, feedid.Valid, "Feed id is not valid")
		assert.True(T, feedid.String != "", "Feed id is empty")
	})
}
