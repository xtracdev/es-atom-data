package esatompub

import (
	"database/sql"
	"github.com/xtracdev/goes"
	"github.com/xtracdev/orapub"
)

func NewESAtomPubProcessor() orapub.EventProcessor {
	return orapub.EventProcessor{
		Initialize: func(db *sql.DB) error {
			return nil
		},
		Processor: func(db *sql.DB, event *goes.Event) error {
			_, err := db.Exec("insert into recent (aggregate_id, version,typecode, payload) values(:1,:2,:3,:4)",
				event.Source, event.Version, event.TypeCode, event.Payload)
			return err
		},
	}
}
