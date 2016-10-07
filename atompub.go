package esatompub

import (
	"crypto/rand"
	"database/sql"
	"fmt"
	log "github.com/Sirupsen/logrus"
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
			log.Warnf("Attempted to override threshold with non integer: %s", thresholdOverride)
			return
		}

		log.Infof("Overriding default feed threshold with %d", threshold)
		FeedThreshold = threshold
	}
}

func readPreviousFeedId(tx *sql.Tx) (sql.NullString, error) {
	//TODO: Need to lock feeds or concurrent processes might overwrite each other's feed update
	log.Info("Select last feed id")
	var feedid sql.NullString
	rows, err := tx.Query("select feedid from feed where event_time = (select max(event_time) from feed)")
	if err != nil {
		log.Warn(err.Error())
		return feedid, err
	}

	defer rows.Close()
	for rows.Next() {
		//Only one row can be returned at mpst
		if err := rows.Scan(&feedid); err != nil {
			return feedid, err
		}
	}

	return feedid, nil
}

func writeEventToAtomEventTable(tx *sql.Tx, event *goes.Event) error {
	log.Info("insert event into atom_event")
	_, err := tx.Exec("insert into atom_event (aggregate_id, version,typecode, payload) values(:1,:2,:3,:4)",
		event.Source, event.Version, event.TypeCode, event.Payload)
	return err
}

func getRecentFeedCount(tx *sql.Tx) (int, error) {
	log.Info("get current count")
	var count int
	err := tx.QueryRow("select count(*) from atom_event where feedid is null").Scan(&count)
	return count, err
}

func createNewFeed(tx *sql.Tx, currentFeedId sql.NullString) error {
	log.Infof("Feed threshold of %d met", FeedThreshold)
	var prevFeedId sql.NullString
	uuidStr, err := uuid()
	if err != nil {
		return nil
	}

	if currentFeedId.Valid {
		prevFeedId = currentFeedId

	}
	currentFeedId = sql.NullString{String: uuidStr, Valid: true}

	log.Info("Update feed ids")
	_, err = tx.Exec("update atom_event set feedid = :1 where feedid is null", currentFeedId)
	if err != nil {
		return err
	}

	log.Infof("Insert into feed %v, %v", currentFeedId, prevFeedId)
	_, err = tx.Exec("insert into feed (feedid, previous) values (:1, :2)",
		currentFeedId, prevFeedId)
	return err
}

func processEvent(db *sql.DB, event *goes.Event) error {
	log.Info("Processor invoked")

	//Need a transaction to group the work in this method
	log.Info("create transaction")
	tx, err := db.Begin()
	if err != nil {
		return err
	}

	defer tx.Rollback()

	//Get the current feed id
	feedid, err := readPreviousFeedId(tx)
	log.Infof("previous feed id is %s", feedid.String)

	//Insert current row
	err = writeEventToAtomEventTable(tx, event)
	if err != nil {
		return err
	}

	//Get current count of records in the current feed
	count, err := getRecentFeedCount(tx)
	if err != nil {
		return err
	}
	log.Infof("current count is %d", count)

	//Threshold met
	if count == FeedThreshold {
		err := createNewFeed(tx, feedid)
		if err != nil {
			return err
		}
	}

	log.Info("commit txn")
	tx.Commit()

	return nil
}

func NewESAtomPubProcessor() orapub.EventProcessor {
	return orapub.EventProcessor{
		Initialize: func(db *sql.DB) error {
			return nil
		},
		Processor: func(db *sql.DB, event *goes.Event) error {
			return processEvent(db, event)
		},
	}
}

func uuid() (string, error) {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%X-%X-%X-%X-%X", b[0:4], b[4:6], b[6:8], b[8:10], b[10:]), nil

}
