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
	var feedid sql.NullString

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
		err := db.QueryRow("select count(*) from feed").Scan(&count)
		assert.Nil(T, err)
		assert.Equal(T, 1, count, "Expected a single feed entry")


		err = db.QueryRow("select feedid from feed").Scan(&feedid)
		assert.Nil(T, err)
		assert.True(T, feedid.Valid, "Feed id is not valid")
		assert.True(T, feedid.String != "", "Feed id is empty")
	})

	And(`^the recent items with a null id are updated with the feedid$`, func() {
		rows, err := db.Query("select aggregate_id, feedid from atom_event")
		if assert.Nil(T,err) {
			defer rows.Close()

			var aggid string
			var eventFeedId sql.NullString
			var rowCount int
			for rows.Next() {
				rowCount += 1
				err := rows.Scan(&aggid,&eventFeedId)
				assert.Nil(T,err)
				if assert.True(T,eventFeedId.Valid) {
					assert.Equal(T,feedid.String, eventFeedId.String)
				}

			}

			assert.Equal(T, 2, rowCount, "Expected two events to be read from atom_event")
		}
	})

	Given(`^some initial events and some feeds$`, func() {
		//From the previous test run
	})

	When(`^the feed page threshold is reached again$`, func() {
		eventPtr := &goes.Event{
			Source:   "agg3",
			Version:  1,
			TypeCode: "foo",
			Payload:  []byte("ok?"),
		}

		err = atomProcessor.Processor(db, eventPtr)
		assert.Nil(T, err)

		eventPtr = &goes.Event{
			Source:   "agg4",
			Version:  1,
			TypeCode: "foo",
			Payload:  []byte("ok?"),
		}

		err = atomProcessor.Processor(db, eventPtr)
		assert.Nil(T, err)
	})

	Then(`^feed is updated with a new feedid with the previous feed id as previous$`, func() {
		var current, previous sql.NullString
		err := db.QueryRow("select feedid, previous from feed where id = (select max(id) from feed)").Scan(&current,&previous)
		if assert.Nil(T,err) {
			assert.True(T, current.Valid)
			if assert.True(T, previous.Valid) {
				assert.Equal(T, previous.String, feedid.String)
			}
		}
	})

	And(`^the most recent items with a null id are updated with the new feedid$`, func() {
		var nullCount int = -1
		err := db.QueryRow("select count(*) from atom_event where feedid is null").Scan(&nullCount)
		assert.Nil(T,err)
	})

}
