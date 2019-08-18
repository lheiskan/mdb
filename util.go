package mdb

import (
	"github.com/abdullin/lex-go/tuple"
	"github.com/dgraph-io/badger"
	"github.com/pkg/errors"

	proto "github.com/golang/protobuf/proto"
)

func CreateKey(args ...tuple.Element) []byte {
	tpl := tuple.Tuple(args)
	return tpl.Pack()
}

func (tx *Tx) PutProto(key []byte, val proto.Message) error {
	var err error
	var data []byte

	if data, err = proto.Marshal(val); err != nil {
		return errors.Wrap(err, "Marshal")
	}
	return tx.Put(key, data)
}

func (tx *Tx) ReadProto(key []byte, pb proto.Message) error {
	var data []byte
	var err error

	if data, err = tx.Get(key); key != nil {
		return errors.Wrap(err, "util.go: tx.Get")
	}

	if data == nil {
		return nil
	}

	if err = proto.Unmarshal(data, pb); err != nil {
		return errors.Wrap(err, "Unmarshal")
	}
	return nil
}

func (tx *Tx) GetNext(key []byte) (k, v []byte, err error) {

	opts := badger.DefaultIteratorOptions
	opts.PrefetchSize = 1
	it := tx.Tx.NewIterator(opts)
	defer it.Close()

	isFound := false
	for it.Seek(key); it.ValidForPrefix(key); it.Next() {
		item := it.Item()
		k = item.Key()
		v, err = item.ValueCopy(v)
		if err != nil {
			return
		}
		return
	}

	if !isFound {
		err = badger.ErrKeyNotFound
	}
	return
}

func (tx *Tx) GetPrev(key []byte) (k, v []byte, err error) {

	opts := badger.DefaultIteratorOptions
	opts.PrefetchSize = 1
	opts.Reverse = true
	it := tx.Tx.NewIterator(opts)
	defer it.Close()

	isFound := false
	for it.Seek(key); it.ValidForPrefix(key); it.Next() {
		item := it.Item()
		k = item.Key()
		v, err = item.ValueCopy(v)
		if err != nil {
			return
		}
		return
	}

	if !isFound {
		err = badger.ErrKeyNotFound
	}
	return
}

func (tx *Tx) ScanRange(key []byte, row func(k, v []byte) error) error {

	opts := badger.DefaultIteratorOptions
	opts.PrefetchSize = 1
	it := tx.Tx.NewIterator(opts)
	defer it.Close()
	for it.Seek(key); it.ValidForPrefix(key); it.Next() {
		item := it.Item()
		k := item.Key()
		var v []byte
		v, err := item.ValueCopy(v)
		if err != nil {
			return err
		}
		row(k, v)
	}
	return nil
}

func (t *Tx) DelRange(prefix []byte) error {
	deleteKeys := func(keysForDelete [][]byte) error {
		for _, key := range keysForDelete {
			if err := t.Tx.Delete(key); err != nil {
				return err
			}
		}
		return nil
	}

	collectSize := 100000
	return t.DB.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.AllVersions = false
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		keysForDelete := make([][]byte, 0, collectSize)
		keysCollected := 0
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			key := it.Item().KeyCopy(nil)
			keysForDelete = append(keysForDelete, key)
			keysCollected++
			if keysCollected == collectSize {
				if err := deleteKeys(keysForDelete); err != nil {
					return err
				}
				keysForDelete = make([][]byte, 0, collectSize)
				keysCollected = 0
			}
		}
		if keysCollected > 0 {
			if err := deleteKeys(keysForDelete); err != nil {
				return err
			}
		}
		return nil
	})
}
