package esatompub

import (
	"crypto/rand"
	"database/sql"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/armon/go-metrics"
	"github.com/xtracdev/goes"
	"github.com/xtracdev/orapub"
	"os"
	"strconv"
	"time"
)

const defaultFeedThreshold = 100

const (
	sqlLatestFeedId        = `select feedid from feed where id = (select max(id) from feed)`
	sqlInsertEventIntoFeed = `insert into atom_event (aggregate_id, version,typecode, payload) values(:1,:2,:3,:4)`
	sqlRecentFeedCount = `select count(*) from atom_event where feedid is null`
	sqlUpdateFeedIds = `update atom_event set feedid = :1 where feedid is null`
	sqlInsertFeed = `insert into feed (feedid, previous) values (:1, :2)`
	sqlLockTable = `lock table feed in exclusive mode`
)

var FeedThreshold = defaultFeedThreshold

func logDatabaseTimingStats(sql string, start time.Time, err error) {
	duration := time.Now().Sub(start)
	go func(sql string, duration time.Duration, err error) {
		ms := float32(duration.Nanoseconds()) / 1000.0 / 1000.0
		if err != nil {
			key := []string{"es-atom-data", "db", fmt.Sprintf("%s-error", sql)}
			metrics.AddSample(key, float32(ms))
			metrics.IncrCounter(key, 1)
		} else {
			key := []string{"es-atom-data", "db", sql}
			metrics.AddSample(key, float32(ms))
			metrics.IncrCounter(key, 1)
		}
	}(sql,duration,err)
}

func writeProcessEventStats(start time.Time, err error) {
	duration := time.Now().Sub(start)
	go func(duration time.Duration, err error) {
		ms := float32(duration.Nanoseconds()) / 1000.0 / 1000.0
		if err != nil {
			key := []string{"es-atom-data", "process-event", "error"}
			metrics.AddSample(key, float32(ms))
			metrics.IncrCounter(key, 1)
		} else {
			key := []string{"es-atom-data", "process-event", "ok"}
			metrics.AddSample(key, float32(ms))
			metrics.IncrCounter(key, 1)
		}
	}(duration,err)
}

func ReadFeedThresholdFromEnv() {
	thresholdOverride := os.Getenv("FEED_THRESHOLD")
	if thresholdOverride != "" {
		threshold, err := strconv.Atoi(thresholdOverride)
		if err != nil {
			log.Warnf("Attempted to override threshold with non integer: %s", thresholdOverride)
			log.Warnf("Defaulting to %d", defaultFeedThreshold)
			FeedThreshold = defaultFeedThreshold
			return
		}

		log.Infof("Overriding default feed threshold with %d", threshold)
		FeedThreshold = threshold
	}
}

func selectLatestFeed(tx *sql.Tx) (sql.NullString, error) {
	log.Debug("Select last feed id")

	var feedid sql.NullString
	start := time.Now()
	rows, err := tx.Query(sqlLatestFeedId)
	if err != nil {
		logDatabaseTimingStats("sqlLatestFeedId", start, err)
		return feedid, err
	}

	defer rows.Close()
	for rows.Next() {
		//Only one row can be returned at most
		if err = rows.Scan(&feedid); err != nil {
			logDatabaseTimingStats("sqlLatestFeedId", start, err)
			return feedid, err
		}
	}

	if err = rows.Err(); err != nil {
		return feedid,err
	}

	logDatabaseTimingStats("sqlLatestFeedId", start, err)
	return feedid, nil
}

func writeEventToAtomEventTable(tx *sql.Tx, event *goes.Event) error {
	log.Debug("insert event into atom_event")
	start := time.Now()
	_, err := tx.Exec(sqlInsertEventIntoFeed,
		event.Source, event.Version, event.TypeCode, event.Payload)
	logDatabaseTimingStats("sqlInsertEventIntoFeed", start, err)
	return err
}

func getRecentFeedCount(tx *sql.Tx) (int, error) {
	log.Debug("get current count")
	var count int
	start := time.Now()
	err := tx.QueryRow(sqlRecentFeedCount).Scan(&count)
	logDatabaseTimingStats("sqlRecentFeedCount", start, err)

	return count, err
}

func createNewFeed(tx *sql.Tx, currentFeedId sql.NullString) error {
	log.Infof("Feed threshold of %d met", FeedThreshold)
	var prevFeedId sql.NullString
	uuidStr, err := uuid()
	if err != nil {
		return err
	}

	if currentFeedId.Valid {
		prevFeedId = currentFeedId

	}
	currentFeedId = sql.NullString{String: uuidStr, Valid: true}

	log.Info("Update feed ids")

	start := time.Now()
	_, err = tx.Exec(sqlUpdateFeedIds, currentFeedId)
	logDatabaseTimingStats("sqlUpdateFeedIds", start, err)

	if err != nil {
		return err
	}


	log.Infof("Insert into feed %v, %v", currentFeedId, prevFeedId)
	start = time.Now()
	_, err = tx.Exec(sqlInsertFeed,
		currentFeedId, prevFeedId)
	logDatabaseTimingStats("sqlInsertFeed", start, err)
	return err
}

func lockTable(tx *sql.Tx) error {
	start := time.Now()
	_, err := tx.Exec(sqlLockTable)
	logDatabaseTimingStats("sqlLockTable", start, err)
	return err
}

func doRollback(tx *sql.Tx) {
	err := tx.Rollback()
	if err != nil {
		log.Warnf("Error on transaction rollback: %s",err.Error())
	}
}

func processEvent(db *sql.DB, event *goes.Event) error {
	log.Debug("Processor invoked")

	//Need a transaction to group the work in this method
	log.Debug("create transaction")
	tx, err := db.Begin()
	if err != nil {
		return err
	}

	//Treat the processing as a critical section to avoid concurrency headaches.
	err = lockTable(tx)
	if err != nil {
		doRollback(tx)
		return err
	}

	//Get the current feed id
	feedid, err := selectLatestFeed(tx)
	if err != nil {
		doRollback(tx)
		return err
	}
	log.Debugf("previous feed id is %s", feedid.String)

	//Insert current row
	err = writeEventToAtomEventTable(tx, event)
	if err != nil {
		doRollback(tx)
		return err
	}

	//Get current count of records in the current feed
	count, err := getRecentFeedCount(tx)
	if err != nil {
		doRollback(tx)
		return err
	}
	log.Debugf("current count is %d", count)

	//Threshold met
	if count == FeedThreshold {
		err := createNewFeed(tx, feedid)
		if err != nil {
			doRollback(tx)
			return err
		}
	}

	log.Debug("commit txn")
	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}

func configureStatsD() {
	statsdEndpoint := os.Getenv("STATSD_ENDPOINT")
	log.Infof("STATSD_ENDPOINT: %s", statsdEndpoint)

	if statsdEndpoint != "" {

		log.Info("Using vanilla statsd client to send telemetry to ", statsdEndpoint)
		sink, err := metrics.NewStatsdSink(statsdEndpoint)
		if err != nil {
			log.Warn("Unable to configure statds sink", err.Error())
			return
		}
		metrics.NewGlobal(metrics.DefaultConfig(statsdEndpoint), sink)
	} else {
		log.Info("Using in memory metrics accumulator - dump via USR1 signal")
		inm := metrics.NewInmemSink(10*time.Second, 5*time.Minute)
		metrics.DefaultInmemSignal(inm)
		metrics.NewGlobal(metrics.DefaultConfig("xavi"), inm)
	}
}

func NewESAtomPubProcessor() orapub.EventProcessor {
	configureStatsD()
	return orapub.EventProcessor{
		Initialize: func(db *sql.DB) error {
			return nil
		},
		Processor: func(db *sql.DB, event *goes.Event) error {
			start := time.Now()
			err :=  processEvent(db, event)
			writeProcessEventStats(start, err)
			return err
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
