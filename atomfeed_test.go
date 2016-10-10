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

/*
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
*/

var trueVal = true
var falseVal = false

const errorExpected = true
const noErrorExpected = false

var processTests = []struct {
	beginOk           *bool
	tableLockOk       *bool
	feedIdSelectOk    *bool
	eventInsertOk     *bool
	thesholdCountOk   *bool
	atomEventUpdateOk *bool
	feedInsertOk      *bool
	expectCommit      *bool
	expectError       bool
}{
	{&trueVal, &trueVal, &trueVal, &trueVal, &trueVal, &trueVal, &trueVal, &trueVal, noErrorExpected},
	{&falseVal, nil, nil, nil, nil, nil, nil, nil, errorExpected},
	{&trueVal, &falseVal, nil, nil, nil, nil, nil, nil, errorExpected},
}

func testBeginSetup(mock sqlmock.Sqlmock, ok *bool) {
	if *ok {
		mock.ExpectBegin()
	} else {
		mock.ExpectBegin().WillReturnError(errors.New("sorry mate no txn for you"))
	}
}

func testTableLockSetup(mock sqlmock.Sqlmock, ok *bool) {
	if ok == nil {
		return
	}

	if *ok == true {
		execOkResult := sqlmock.NewResult(1, 1)
		mock.ExpectExec("lock table feed").WillReturnResult(execOkResult)
	} else {
		mock.ExpectExec("lock table feed").WillReturnError(errors.New("BAM!"))
	}
}

func TestProcessEvents(t *testing.T) {
	for _, tt := range processTests {

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

		testBeginSetup(mock, tt.beginOk)
		testTableLockSetup(mock, tt.tableLockOk)

		//feed id is selected
		if tt.feedIdSelectOk != nil {
			rows := sqlmock.NewRows([]string{"feedid"}).AddRow("XXX")
			mock.ExpectQuery("select feedid from feed").WillReturnRows(rows)
		}

		//event is inserted into the atom_event table
		if tt.eventInsertOk != nil {
			execOkResult := sqlmock.NewResult(1, 1)
			mock.ExpectExec("insert into atom_event").WithArgs(
				eventPtr.Source, eventPtr.Version, eventPtr.TypeCode, eventPtr.Payload,
			).WillReturnResult(execOkResult)
		}

		//return the count of threshold to trigger new feed creation
		if tt.thesholdCountOk != nil {
			rows := sqlmock.NewRows([]string{"feedid"}).AddRow("XXX")
			rows = sqlmock.NewRows([]string{"count(*)"}).
				AddRow(FeedThreshold)
			mock.ExpectQuery(`select count`).WillReturnRows(rows)
		}

		//atom_event is updated with the feed id
		if tt.atomEventUpdateOk != nil {
			execOkResult := sqlmock.NewResult(1, 1)
			mock.ExpectExec("update atom_event set feedid").WillReturnResult(execOkResult)
		}

		//insert the new feed into the feed table
		if tt.feedInsertOk != nil {
			execOkResult := sqlmock.NewResult(1, 1)
			mock.ExpectExec("insert into feed").WillReturnResult(execOkResult).WithArgs(sqlmock.AnyArg(), "XXX")
		}

		//expect a commit at the end
		if tt.expectCommit != nil {
			mock.ExpectCommit()
		}

		processor := NewESAtomPubProcessor()

		err = processor.Initialize(db)
		assert.Nil(t, err)

		err = processor.Processor(db, eventPtr)
		if tt.expectError {
			assert.NotNil(t, err)
		} else {
			assert.Nil(t, err)
		}

		err = mock.ExpectationsWereMet()
		assert.Nil(t, err)
	}
}

/*
func TestProcessorBeginTxnFailure(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	mock.ExpectBegin().WillReturnError(errors.New("sorry mate no txn for you"))

	err = processEvent(db, nil)
	assert.NotNil(t, err)
	err = mock.ExpectationsWereMet()
	assert.Nil(t, err)
}

func TestProcessorTableLockError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	//processEvents starts a transaction
	mock.ExpectBegin()

	//table lock is acquired in the happy path
	mock.ExpectExec("lock table feed").WillReturnError(errors.New("whhops"))

	err = processEvent(db, nil)
	assert.NotNil(t, err)
	err = mock.ExpectationsWereMet()
	assert.Nil(t, err)
}
*/
