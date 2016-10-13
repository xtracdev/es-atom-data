package esatompub

import (
	"github.com/xtracdev/goes"
	"time"
	"database/sql"
)

type TimestampedEvent struct {
	goes.Event
	Timestamp time.Time
}

func RetrieveRecent(db *sql.DB)([]TimestampedEvent,error) {
	var events []TimestampedEvent

	rows, err := db.Query(sqlSelectRecent)
	if err != nil {
		return events,err
	}

	defer rows.Close()

	var eventTime time.Time
	var aggregateId, typecode string
	var version int
	var payload []byte

	for rows.Next() {
		err := rows.Scan(&eventTime,&aggregateId, &version, &typecode, &payload)
		if err != nil {
			return events,err
		}

		event := TimestampedEvent{
			Event: goes.Event {
				Source: aggregateId,
				Version: version,
				Payload: payload,
				TypeCode: typecode,
			},
			Timestamp: eventTime,
		}

		events = append(events, event)
	}

	if err = rows.Err(); err != nil {
		return events, err
	}

	return events,nil
}

func RetrieveLastFeed(db *sql.DB) (string, error) {
	var feedid string

	err := db.QueryRow(sqlLatestFeedId).Scan(&feedid)
	if err == sql.ErrNoRows {
		return "",nil
	} else if err != nil {
		return "", err
	}

	return feedid,nil
}