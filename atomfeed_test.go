package esatompub

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"github.com/xtracdev/goes"
	"gopkg.in/DATA-DOG/go-sqlmock.v1"
	"os"
	"testing"
)

func TestSetThresholdFromEnv(t *testing.T) {
	assert.Equal(t, defaultFeedThreshold, FeedThreshold)
	os.Setenv("FEED_THRESHOLD", "2")
	ReadFeedThresholdFromEnv()
	assert.Equal(t, 2, FeedThreshold)
}

func TestSetThresholdToDefaultOnBadEnvSpec(t *testing.T) {
	os.Setenv("FEED_THRESHOLD", "two")
	ReadFeedThresholdFromEnv()
	assert.Equal(t, defaultFeedThreshold, FeedThreshold)
	os.Setenv("FEED_THRESHOLD", "2")
}

func TestReadPreviousFeedId(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	rows := sqlmock.NewRows([]string{"feedid"}).
		AddRow("foo")

	mock.ExpectBegin()
	mock.ExpectQuery(`select feedid from feed where id = \(select max\(id\) from feed\)`).WillReturnRows(rows)

	tx, _ := db.Begin()
	feedid, err := readPreviousFeedId(tx)
	if assert.Nil(t, err) {
		assert.Equal(t, "foo", feedid.String)
		err = mock.ExpectationsWereMet()
		assert.Nil(t, err)
	}
}

func TestReadPreviousFeedIdQueryError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	queryErr := errors.New("query error")
	mock.ExpectBegin()
	mock.ExpectQuery(`select feedid from feed where id = \(select max\(id\) from feed\)`).WillReturnError(queryErr)

	tx, _ := db.Begin()
	_, err = readPreviousFeedId(tx)
	if assert.NotNil(t, err) {
		assert.Equal(t, queryErr, err)
	}
}

func TestReadPreviousFeedIdScanError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	foo := struct {
		foo string
		bar string
	}{
		"foo", "bar",
	}
	rows := sqlmock.NewRows([]string{"feedid"}).AddRow(foo)

	mock.ExpectBegin()
	mock.ExpectQuery(`select feedid from feed where id = \(select max\(id\) from feed\)`).WillReturnRows(rows)

	tx, _ := db.Begin()
	_, err = readPreviousFeedId(tx)
	if assert.NotNil(t, err) {
		err = mock.ExpectationsWereMet()
		assert.Nil(t, err)
	}
}

func TestWriteEvent(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	eventPtr := &goes.Event{
		Source:   "agg1",
		Version:  1,
		TypeCode: "foo",
		Payload:  []byte("ok"),
	}

	mock.ExpectBegin()
	mock.ExpectExec("insert into atom_event")

	tx, _ := db.Begin()
	err = writeEventToAtomEventTable(tx, eventPtr)
	assert.Nil(t, nil)
	err = mock.ExpectationsWereMet()
	assert.Nil(t, err)
}

func TestGetRecentFeedCount(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	rows := sqlmock.NewRows([]string{"count(*)"}).AddRow(23)

	mock.ExpectBegin()
	mock.ExpectQuery(`select count\(\*\) from atom_event where feedid is null`).WillReturnRows(rows)
	tx, _ := db.Begin()
	count, err := getRecentFeedCount(tx)
	if assert.Nil(t, err) {
		assert.Equal(t, count, 23)
	}

}
