package mdb

import (
	"log"
	"os"

	"github.com/dgraph-io/badger"
	"github.com/pkg/errors"
)

type DB struct {
	*badger.DB
}

// New creates a new DB wrapper around LMDB
func New(folder string, cfg *Config) (*DB, error) {

	os.MkdirAll(folder, os.ModePerm)
	opts := badger.DefaultOptions(folder)
	if cfg.Readonly {
		log.Println("Setting readonly to true")
		opts.ReadOnly = true
	} else {
		log.Println("Setting readonly to false")
	}
	//https://stackoverflow.com/questions/28969455/golang-properly-instantiate-os-filemode
	db, err := badger.Open(opts)
	if err != nil {
		return nil, errors.Wrap(err, "DB.New")
	}
	return &DB{db}, nil

}

// Close the environment
func (db *DB) Close() error {

	err := db.DB.Close()
	if err != nil {
		return errors.Wrap(err, "DB.Close")
	}
	return nil
}
