package atom

import (
	"database/sql"
	log "github.com/Sirupsen/logrus"
	. "github.com/gucumber/gucumber"
	_ "github.com/mattn/go-oci8"
	"github.com/stretchr/testify/assert"
)

func init() {
	var env *envConfig
	var db *sql.DB

	var initializeEnvironment = func() error {
		var err error
		env, err = NewEnvConfig()
		if err != nil {
			return err
		}

		log.Infof("Connection for test: %s", env.MaskedConnectString())

		db, err = sql.Open("oci8", env.ConnectString())
		if err != nil {
			return err
		}

		err = db.Ping()
		if err != nil {
			return err
		}

		return nil
	}

	Given(`^a new feed environment$`, func() {
		err := initializeEnvironment()
		if assert.Nil(T, err) {
			_, err = db.Exec("delete from recent")
			assert.Nil(T, err)
			_, err = db.Exec("delete from archive")
			assert.Nil(T, err)
			_, err = db.Exec("delete from feeds")
			assert.Nil(T, err)
		}
	})
}
