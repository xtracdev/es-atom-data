package esatompub

import (
	"database/sql"
	logs "github.com/Sirupsen/logrus"
	"github.com/xtracdev/goes"
	"github.com/xtracdev/orapub"
	"os"
	"strconv"
)

const defaultFeedThreshold = 100

var FeedThreshold = defaultFeedThreshold

func ReadFeedThresholdFromEnv() {
	thresholdOverride := os.Getenv("FEED_THRESHOLD")
	if thresholdOverride != "" {
		threshold, err := strconv.Atoi(thresholdOverride)
		if err != nil {
			logs.Warnf("Attempted to override threshold with non integer: %s", thresholdOverride)
			return
		}

		logs.Infof("Overriding default feed threshold with %d", threshold)
		FeedThreshold = threshold
	}
}

func NewESAtomPubProcessor() orapub.EventProcessor {
	return orapub.EventProcessor{
		Initialize: func(db *sql.DB) error {
			return nil
		},
		Processor: func(db *sql.DB, event *goes.Event) error {
			_, err := db.Exec("insert into recent (aggregate_id, version,typecode, payload) values(:1,:2,:3,:4)",
				event.Source, event.Version, event.TypeCode, event.Payload)
			return err
		},
	}
}
