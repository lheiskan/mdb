package mdb

import (
	"github.com/dgraph-io/badger"
	"github.com/pkg/errors"
)

type Tx struct {
	DB *badger.DB
	//Env *lmdb.Env
	Tx *badger.Txn
}

func (tx *Tx) Get(key []byte) (data []byte, err error) {
	var item *badger.Item
	if item, err = tx.Tx.Get(key); err != nil {
		if err == badger.ErrNoRewrite {
			return nil, nil
		}
		return nil, errors.Wrap(err, "Tx.Get")
	}
	item.ValueCopy(data)
	return data, nil
}

func (tx *Tx) Commit() error {
	return tx.Tx.Commit()
}

func (tx *Tx) Put(key []byte, val []byte) error {
	if err := tx.Tx.Set(key, val); err != nil {
		return errors.Wrap(err, "tx.Put")
	}
	return nil
}

func (tx *Tx) Del(key []byte) error {
	if err := tx.Tx.Delete(key); err != nil {
		return err

	}
	return nil
}

func (tx *Tx) Close() (err error) {
	return nil
}
