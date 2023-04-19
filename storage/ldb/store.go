// @author ousing9
package ldb

import (
	"context"
	"errors"
	"github.com/dgraph-io/badger/v2"
	"github.com/spacemonkeygo/monkit/v3"
	"github.com/zeebo/errs"
	"go.uber.org/zap"
	"path/filepath"
	"storj.io/common/storj"
	"storj.io/storj/storage/filestore"
)

var (
	// Error is the default WiscKey error class
	Error = errs.Class("WiscKey error")

	mon = monkit.Package()
)

type PieceDataStore struct {
	log *zap.Logger
	dir *filestore.Dir
	db  *badger.DB
}

// 每个 Storage node 都应该只有一个 WiscKey 实例，避免不必要的冲突
func New(log *zap.Logger, dir *filestore.Dir) *PieceDataStore {
	db, _ := badger.Open(badger.DefaultOptions(filepath.Join(dir.Path(), "WiscKey")))

	return &PieceDataStore{
		log: log,
		dir: dir,
		db:  db,
	}
}

// 获取 WiscKey 实例
func (store *PieceDataStore) GetInstance(ctx context.Context) (_ *badger.DB, err error) {
	defer mon.Task()(&ctx)(&err)

	return store.db, nil
}

func (store *PieceDataStore) Get(ctx context.Context, id storj.PieceID) (value []byte, err error) {
	defer mon.Task()(&ctx)(&err)
	value, err = WiscKeyGet(store.db, id.Bytes())
	if err != nil {
		return nil, Error.Wrap(err)
	}
	return value, nil
}

func (store *PieceDataStore) Set(ctx context.Context, id storj.PieceID, data []byte) (err error) {
	defer mon.Task()(&ctx)(&err)
	err = WiscKeySet(store.db, id.Bytes(), data)
	if err != nil {
		return Error.Wrap(err)
	}
	return nil
}

func (store *PieceDataStore) Has(ctx context.Context, id storj.PieceID) (has bool, err error) {
	defer mon.Task()(&ctx)(&err)
	has, err = WiscKeyHas(store.db, id.Bytes())
	if err != nil {
		return false, Error.Wrap(err)
	}
	return has, nil
}

func (store *PieceDataStore) Delete(ctx context.Context, id storj.PieceID) (err error) {
	defer mon.Task()(&ctx)(&err)
	err = WiscKeyDel(store.db, id.Bytes())
	if err != nil {
		return Error.Wrap(err)
	}
	return nil
}

func WiscKeyGet(db *badger.DB, key []byte) (value []byte, err error) {
	err = db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}

		value = []byte{}
		return item.Value(func(val []byte) error {
			value = append(value, val...)
			return nil
		})
	})
	if err != nil {
		return nil, err
	}

	return value, nil
}

func WiscKeySet(db *badger.DB, key []byte, value []byte) (err error) {
	return db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, value)
	})
}

func WiscKeyHas(db *badger.DB, key []byte) (has bool, err error) {
	err = db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(key)
		if errors.Is(err, badger.ErrKeyNotFound) {
			has = false
			return nil
		}
		has = err == nil
		return err
	})
	return has, err
}

func WiscKeyDel(db *badger.DB, key []byte) (err error) {
	return db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})
}
