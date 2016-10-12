package esatompub

import (
	"testing"
	"gopkg.in/DATA-DOG/go-sqlmock.v1"
	"time"
	"github.com/stretchr/testify/assert"
)



func TestQueryForRecent(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	ts := time.Now()
	rows := sqlmock.NewRows([]string{"event_time", "aggregate_id",
		"version", "typecode", "payload"},
			).AddRow(ts, "1x2x333", 3, "foo", []byte("yeah ok"))
	mock.ExpectQuery("select").WillReturnRows(rows)

	events, err := RetrieveRecent(db)
	if assert.Nil(t,err) {
		err := mock.ExpectationsWereMet()
		assert.Nil(t, err, "mock expectations were not met")
		if assert.Equal(t, 1, len(events), "Expected an event back") {
			event := events[0]
			assert.Equal(t, event.Timestamp, ts)
			assert.Equal(t, event.Payload, []byte("yeah ok"))
			assert.Equal(t, event.TypeCode, "foo")
			assert.Equal(t, event.Source, "1x2x333")
			assert.Equal(t, event.Version, 3)
		}
	}
}


