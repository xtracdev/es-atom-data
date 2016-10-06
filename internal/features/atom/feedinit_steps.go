package atom

import (
	"database/sql"
	. "github.com/gucumber/gucumber"
	_ "github.com/mattn/go-oci8"
	"github.com/stretchr/testify/assert"
	ap "github.com/xtracdev/es-atom-data"
	"github.com/xtracdev/goes"
	"github.com/xtracdev/orapub"
)

func init() {
	var initFailure bool
	env, db, err := initializeEnvironment()
	if env != nil {
		initFailure = false
	}

	var atomProcessor orapub.EventProcessor

	Given(`^a new feed environment$`, func() {
		if assert.False(T, initFailure) {
			return
		}

		if assert.Nil(T, err) {
			_, err = db.Exec("delete from recent")
			assert.Nil(T, err)
			_, err = db.Exec("delete from archive")
			assert.Nil(T, err)
			_, err = db.Exec("delete from feeds")
			assert.Nil(T, err)
		}
	})

	When(`^we start up the feed processor$`, func() {
		atomProcessor = ap.NewESAtomPubProcessor()
		err := atomProcessor.Initialize(db)
		assert.Nil(T, err)
	})

	And(`^some events are published$`, func() {
		eventPtr := &goes.Event{
			Source:   "agg1",
			Version:  1,
			TypeCode: "foo",
			Payload:  []byte("ok"),
		}
		atomProcessor.Processor(db, eventPtr)
	})

	And(`^the number of events is lower than the feed threshold$`, func() {
		//Here we use the known starting state with the assumption our feed threshold is > 1
	})

	Then(`^the events are stored in the recent table with a null feed id$`, func() {
		var feedid sql.NullString
		err := db.QueryRow("select feedid from recent where aggregate_id = 'agg1'").Scan(&feedid)
		assert.Nil(T, err)
		assert.False(T, feedid.Valid)
	})

	And(`^there are no archived events$`, func() {
		var count = -1
		err := db.QueryRow("select count(*) from archive").Scan(&count)
		if assert.Nil(T, err) {
			assert.Equal(T, count, 0)
		}
	})

	And(`^there are no records in the feeds table$`, func() {
		var count = -1
		err := db.QueryRow("select count(*) from feeds").Scan(&count)
		if assert.Nil(T, err) {
			assert.Equal(T, count, 0)
		}
	})
}
