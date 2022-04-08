package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// BagerReader is am impl of 'StorageReader'
// When a standAloneStorage instance call Reader(), return a BagerReader
type BagerReader struct {
	txn *badger.Txn
}

func (sr BagerReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(sr.txn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return val, err
}

func (sr BagerReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, sr.txn)
}

func (sr BagerReader) Close() {
	sr.txn.Discard()
}

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	kvDb *badger.DB
	path string
	conf *config.Config
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	path := conf.DBPath
	kvDb := engine_util.CreateDB(path, false)

	return &StandAloneStorage{kvDb, path, conf}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	// nothing....
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	if err := s.kvDb.Close(); err != nil {
		return err
	}
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	txn := s.kvDb.NewTransaction(false)
	sr := BagerReader{
		txn: txn,
	}
	return sr, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	wb := &engine_util.WriteBatch{}
	wb.Reset()

	for _, m := range batch {
		switch m.Data.(type) {
		case storage.Put:
			wb.SetCF(m.Cf(), m.Key(), m.Value())
		case storage.Delete:
			wb.DeleteCF(m.Cf(), m.Key())
		}
	}
	wb.SetSafePoint()
	return wb.WriteToDB(s.kvDb)
}
