package esatompub

import (
	"database/sql"
	"github.com/xtracdev/goes"
	"time"
)

type TimestampedEvent struct {
	goes.Event
	Timestamp time.Time
}

const (
	sqlSelectRecent       = `select event_time, aggregate_id, version, typecode, payload from atom_event where feedid is null`
	sqlSelectForFeed      = `select event_time, aggregate_id, version, typecode, payload from atom_event where feedid = :1`
	sqlSelectPreviousFeed = `select previous from feed where feedid = :1`
	sqlSelectNextFeed     = `select feedid from feed where previous = :1`
	sqlSelectEvent        = `select event_time, typecode, payload from atom_event where aggregate_id = :1 and version = :2`
)

func RetrieveRecent(db *sql.DB) ([]TimestampedEvent, error) {
	return retrieveEvents(db, sqlSelectRecent, "")
}

func RetrieveArchive(db *sql.DB, feedid string) ([]TimestampedEvent, error) {
	return retrieveEvents(db, sqlSelectForFeed, feedid)
}

func retrieveEvents(db *sql.DB, query string, feedid string) ([]TimestampedEvent, error) {
	var events []TimestampedEvent

	var rows *sql.Rows
	var err error
	if feedid == "" {
		rows, err = db.Query(query)
	} else {
		rows, err = db.Query(query, feedid)
	}

	if err != nil {
		return events, err
	}

	defer rows.Close()

	var eventTime time.Time
	var aggregateId, typecode string
	var version int
	var payload []byte

	for rows.Next() {
		err := rows.Scan(&eventTime, &aggregateId, &version, &typecode, &payload)
		if err != nil {
			return events, err
		}

		event := TimestampedEvent{
			Event: goes.Event{
				Source:   aggregateId,
				Version:  version,
				Payload:  payload,
				TypeCode: typecode,
			},
			Timestamp: eventTime,
		}

		events = append(events, event)
	}

	if err = rows.Err(); err != nil {
		return events, err
	}

	return events, nil
}

func RetrieveLastFeed(db *sql.DB) (string, error) {
	var feedid string

	err := db.QueryRow(sqlLatestFeedId).Scan(&feedid)
	if err == sql.ErrNoRows {
		return "", nil
	} else if err != nil {
		return "", err
	}

	return feedid, nil
}

func RetrievePreviousFeed(db *sql.DB, id string) (sql.NullString, error) {
	var feedid sql.NullString

	err := db.QueryRow(sqlSelectPreviousFeed, id).Scan(&feedid)
	if err == sql.ErrNoRows {
		return feedid, nil
	} else if err != nil {
		return feedid, err
	}

	return feedid, nil
}

func RetrieveNextFeed(db *sql.DB, feedId string) (sql.NullString, error) {
	var previous sql.NullString

	err := db.QueryRow(sqlSelectNextFeed, feedId).Scan(&previous)
	if err == sql.ErrNoRows {
		return previous, nil
	} else if err != nil {
		return previous, err
	}

	return previous, nil
}

func RetrieveEvent(db *sql.DB, aggID string, version int) (TimestampedEvent, error) {
	var event TimestampedEvent

	var eventTime time.Time
	var typecode string
	var payload []byte

	rows, err := db.Query(sqlSelectEvent, aggID, version)
	if err != nil {
		return event, err
	}

	var scanCount int
	for rows.Next() {
		scanCount += 1
		err := rows.Scan(&eventTime, &typecode, &payload)
		if err != nil {
			return event, err
		}
	}

	if scanCount == 0 {
		return event, sql.ErrNoRows
	}

	if err = rows.Err(); err != nil {
		return event, err
	}

	event = TimestampedEvent{
		Event: goes.Event{
			Source:   aggID,
			Version:  version,
			Payload:  payload,
			TypeCode: typecode,
		},
		Timestamp: eventTime,
	}

	return event, nil
}
