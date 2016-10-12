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

	rows, err := db.Query("select event_time, aggregate_id, version, typecode, payload from atom_event where feedid is null")
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
