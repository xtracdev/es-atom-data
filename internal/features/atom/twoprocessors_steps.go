package atom

import (
	"database/sql"
	"fmt"
	log "github.com/Sirupsen/logrus"
	. "github.com/gucumber/gucumber"
	"github.com/stretchr/testify/assert"
	ad "github.com/xtracdev/es-atom-data"
	"github.com/xtracdev/goes"
	"github.com/xtracdev/orapub"
	"os"
	"strings"
	"sync"
)

//Every once in a while I see test failures, all of which are due to connectivity errors
//from the test driver, probably because I am sharing the same db connection between
//go routines. This routine is used below in the test go routines to recover/retry
//so all the requisite operations the test verification assume have happened
//are performed.
func connectError(s string) bool {
	log.Infof("Do we have a failure to communicate: %s", s)
	return strings.HasPrefix(s, "ORA-12170") || strings.HasPrefix(s, "ORA-12547") ||
		strings.HasPrefix(s, "ORA-12560")
}

func init() {
	var initFailed bool
	var p1, p2 orapub.EventProcessor

	_, db, err := initializeEnvironment()
	if err != nil {
		initFailed = true
	}

	Given(`^two concurrent atom feed event processors$`, func() {
		if initFailed {
			T.Errorf("Failed init")
			return
		}

		p1 = ad.NewESAtomPubProcessor()
		err := p1.Initialize(db)
		assert.Nil(T, err, "Failed to initialize atom publisher")

		p2 = ad.NewESAtomPubProcessor()
		err = p2.Initialize(db)
		assert.Nil(T, err, "Failed to initialize atom publisher")

		if assert.Nil(T, err) {
			_, err = db.Exec("delete from t_aeae_atom_event")
			assert.Nil(T, err)
			_, err = db.Exec("delete from t_aefd_feed")
			assert.Nil(T, err)
		}

	})

	When(`^40 events are evenly distributed to the processors$`, func() {
		var threshold = 2
		os.Setenv("FEED_THRESHOLD", fmt.Sprintf("%d", threshold))
		ad.ReadFeedThresholdFromEnv()
		assert.Equal(T, threshold, ad.FeedThreshold)

		var wg sync.WaitGroup
		wg.Add(20 * threshold)

		for i := 0; i < 20*threshold; i++ {
			eventPtr := &goes.Event{
				Source:   fmt.Sprintf("agg%d", i),
				Version:  1,
				TypeCode: "foo",
				Payload:  []byte("ok?"),
			}

			if i%2 == 0 {
				go func() {
				doit:
					err := p2.Processor(db, eventPtr)
					if err != nil {
						log.Warn(err)
						if connectError(err.Error()) {
							log.Info("retrying...")
							goto doit
						}
					}
					wg.Done()
				}()
			} else {
				go func() {
				doit:
					err := p1.Processor(db, eventPtr)
					if err != nil {
						log.Warn(err)
						if connectError(err.Error()) {
							log.Info("retrying...")
							goto doit
						}
					}
					wg.Done()
				}()
			}
		}

		wg.Wait()
	})

	And(`^the event threshold is (\d+)$`, func(i1 int) {
	})

	Then(`^(\d+) feeds are created$`, func(numfeeds int) {
		var feedCount = -1
		err := db.QueryRow("select count(*) from t_aefd_feed").Scan(&feedCount)
		assert.Nil(T, err)
		assert.Equal(T, 20, feedCount)
	})

	And(`^two events belong to each feed$`, func() {
		rows, err := db.Query("select feedid, count(*) count from t_aeae_atom_event group by feedid")
		if assert.Nil(T, err) {

			defer rows.Close()

			for rows.Next() {
				var count int
				var feedid sql.NullString
				rowerr := rows.Scan(&feedid, &count)
				assert.Nil(T, rowerr)
				assert.True(T, feedid.Valid)
				assert.Equal(T, 2, count)
			}
		}
	})
}
