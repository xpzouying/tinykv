package standalone_storage

import (
	"errors"
	"path"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	engine *engine_util.Engines
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {

	kvDB := engine_util.CreateDB(conf.DBPath, false)
	kvPath := path.Join(conf.DBPath, "/kv")

	raftPath := path.Join(conf.DBPath, "/raft")

	engine := engine_util.NewEngines(kvDB, nil, kvPath, raftPath)

	return &StandAloneStorage{engine: engine}
}

func (s *StandAloneStorage) Start() error {
	return nil
}

func (s *StandAloneStorage) Stop() error {

	return s.engine.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {

	txn := s.engine.Kv.NewTransaction(false)

	return &standAloneStorageReader{
		txn: txn,
	}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {

	for _, one := range batch {

		switch v := one.Data.(type) {
		case storage.Put:
			return engine_util.PutCF(s.engine.Kv, v.Cf, v.Key, v.Value)

		case storage.Delete:
			return engine_util.DeleteCF(s.engine.Kv, v.Cf, v.Key)

		default:
			return errors.New("unknown Modify function")
		}
	}

	return nil
}

type standAloneStorageReader struct {
	txn *badger.Txn
}

func (r *standAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	data, err := engine_util.GetCFFromTxn(r.txn, cf, key)
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil, nil
		}
		return nil, err
	}

	return data, nil
}

func (r *standAloneStorageReader) IterCF(cf string) engine_util.DBIterator {

	return engine_util.NewCFIterator(cf, r.txn)
}

func (r *standAloneStorageReader) Close() {

	r.txn.Discard()
}
