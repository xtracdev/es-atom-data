package esatompub

import (
	"errors"
	"github.com/stretchr/testify/assert"
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
