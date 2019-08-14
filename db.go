package mdb

import (
	"github.com/bmatsuo/lmdb-go/lmdb"
	"github.com/dgraph-io/badger"
	"github.com/pkg/errors"
)

type TxOp func(tx *Tx) error

func (db *DB) CreateRead() (tx *Tx, err error) {
	return db.CreateTransaction(ReadOnly)
}

func (db *DB) Read(fn TxOp) error {
	return db.DB.View(func(t *badger.Txn) error {

		tx := &Tx{db.DB, t}
		if err := fn(tx); err != nil {
			return errors.Wrap(err, "db.Env.View")
		}
		return nil
	})
}

func (db *DB) Update(fn TxOp) error {
	return db.DB.Update(func(t *badger.Txn) error {

		tx := &Tx{db.DB, t}
		if err := fn(tx); err != nil {
			return errors.Wrap(err, "db.Env.View")
		}
		return nil
	})
}

func (db *DB) UpdateLocked(threadLocked bool, fn TxOp) error {
	if !threadLocked {
		return db.Update(fn)
	}

	panic("UpdateLocked not implemented = todo")
	/*
		return db.Env.UpdateLocked(func(t *lmdb.Txn) error {

			tx := &Tx{db.DBI, db.Env, t}
			if err := fn(tx); err != nil {
				return errors.Wrap(err, "db.Env.View")
			}
			return nil
		})
	*/
}

func (db *DB) CreateWrite() (tx *Tx, err error) {
	return db.CreateTransaction(0)
}

const (
	ReadOnly = lmdb.Readonly
)

func (db *DB) CreateTransaction(flags uint) (tx *Tx, err error) {

	// todo: read or update tx?
	txn := db.DB.NewTransaction(true)
	return &Tx{db.DB, txn}, nil
}
