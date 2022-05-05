package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4A/4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).
	resp := new(kvrpcpb.GetResponse)
	reader, err := server.storage.Reader(nil)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.Version)
	lock, err := txn.GetLock(req.Key)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	// lock block read: lock.Ts < txn.Ts
	if lock != nil {
		if lock.IsLockedFor(req.Key, txn.StartTS, resp) {
			return resp, nil
		}
	}

	val, err := txn.GetValue(req.Key)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	if val == nil {
		resp.NotFound = true
	} else {
		resp.Value = val
	}
	return resp, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	resp := new(kvrpcpb.PrewriteResponse)
	reader, err := server.storage.Reader(nil)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	for _, mut := range req.Mutations {
		// check conflict write
		write, commitTs, err := txn.MostRecentWrite(mut.Key)
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionErr.RequestErr
				return resp, nil
			}
			return nil, err
		}
		// 1. txn is committed: commitTs > txnTs
		// 2. txn is rollbacked: commitTs == txnTs
		if write != nil && commitTs >= txn.StartTS {
			resp.Errors = append(resp.Errors, &kvrpcpb.KeyError{
				Conflict: &kvrpcpb.WriteConflict{
					StartTs:    req.StartVersion,
					ConflictTs: commitTs, // another txn's startTs or commitTs?
					Key:        mut.Key,
					Primary:    req.PrimaryLock,
				},
			})
			continue
		}
		// check lock conflict
		lock, err := txn.GetLock(mut.Key)
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionErr.RequestErr
				return resp, nil
			}
			return nil, err
		}
		if lock != nil {
			// another now has lock, so tell client. if lock is expired, client will call ResolveLocks, else client will backoff some time and retry
			resp.Errors = append(resp.Errors, &kvrpcpb.KeyError{
				Locked: lock.Info(mut.Key),
			})
			continue
		}

		// put into txn
		switch mut.Op {
		case kvrpcpb.Op_Put:
			txn.PutValue(mut.Key, mut.Value)
			txn.PutLock(mut.Key, &mvcc.Lock{
				Primary: req.PrimaryLock,
				Ts:      req.StartVersion,
				Ttl:     req.LockTtl,
				Kind:    mvcc.WriteKindPut,
			})
		case kvrpcpb.Op_Del:
			txn.DeleteValue(mut.Key)
			txn.PutLock(mut.Key, &mvcc.Lock{
				Primary: req.PrimaryLock,
				Ts:      req.StartVersion,
				Ttl:     req.LockTtl,
				Kind:    mvcc.WriteKindDelete,
			})
		case kvrpcpb.Op_Rollback:
			//TODO:
		}
	}
	err = server.storage.Write(nil, txn.Writes())
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	return resp, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	resp := new(kvrpcpb.CommitResponse)
	reader, err := server.storage.Reader(nil)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()
	// acquire latches
	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	for _, key := range req.Keys {
		// check lock still exist
		lock, err := txn.GetLock(key)
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionErr.RequestErr
				return resp, nil
			}
			return nil, err
		}
		if lock != nil && lock.Ts == txn.StartTS {
			txn.DeleteLock(key)
			txn.PutWrite(key, req.CommitVersion, &mvcc.Write{
				StartTS: req.StartVersion,
				Kind:    lock.Kind,
			})
		} else if lock == nil {
			// commit twice?
			return resp, nil
		} else {
			// lock tacken by another txn
			resp.Error = &kvrpcpb.KeyError{
				Retryable: "retry",
			}
			return resp, nil
		}
	}
	err = server.storage.Write(nil, txn.Writes())
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	return resp, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	resp := new(kvrpcpb.ScanResponse)
	reader, err := server.storage.Reader(nil)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	txn := mvcc.NewMvccTxn(reader, req.Version)
	scanner := mvcc.NewScanner(req.StartKey, txn)
	defer scanner.Close()

	var i uint32 = 0
	for ; i < req.Limit; i++ {
		kv := &kvrpcpb.KvPair{}
		key, val, err := scanner.Next()
		if err != nil {
			kv.Key = key
			kv.Error = &kvrpcpb.KeyError{
				Retryable: "true", // I don't know return what error
			}
			resp.Pairs = append(resp.Pairs, kv)
		} else {
			if key == nil {
				// scanner has exhausted
				return resp, nil
			} else if key != nil && val == nil {
				// key is deleted, do nothing
			} else {
				kv.Key, kv.Value = key, val
				resp.Pairs = append(resp.Pairs, kv)
			}

		}
	}
	return resp, nil
}

// CheckTxnStatus reports on the status of a transaction and may take action to
// rollback expired locks.
// If the transaction has previously been rolled back or committed, return that information.
// If the TTL of the transaction is exhausted, abort that transaction and roll back the primary lock.
// Otherwise, returns the TTL information.
func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	resp := new(kvrpcpb.CheckTxnStatusResponse)
	reader, err := server.storage.Reader(nil)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.LockTs)
	// check if txn is committed
	write, ts, err := txn.CurrentWrite(req.PrimaryKey)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	if write != nil {
		if write.Kind != mvcc.WriteKindRollback {
			// txn is committed
			resp.CommitVersion = ts
		} else {
			// txn is rollbacked, do nothing
			resp.LockTtl = 0
			resp.CommitVersion = 0
		}
		resp.Action = kvrpcpb.Action_NoAction
		return resp, nil
	}

	// check lock
	lock, err := txn.GetLock(req.PrimaryKey)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	if lock != nil {
		// lock exist and check if expired
		if mvcc.PhysicalTime(lock.Ts)+lock.Ttl <= mvcc.PhysicalTime(req.CurrentTs) {
			txn.DeleteLock(req.PrimaryKey)
			txn.DeleteValue(req.PrimaryKey)
			txn.PutWrite(req.PrimaryKey, req.LockTs, &mvcc.Write{
				StartTS: txn.StartTS,
				Kind:    mvcc.WriteKindRollback,
			})
			resp.Action = kvrpcpb.Action_TTLExpireRollback
			resp.LockTtl = 0
			resp.CommitVersion = 0
		} else {
			// lock doesn't expire
			resp.Action = kvrpcpb.Action_NoAction
			resp.LockTtl = lock.Ttl
		}
	} else {
		// no data for the key, we also need to record
		txn.PutWrite(req.PrimaryKey, req.LockTs, &mvcc.Write{
			StartTS: req.LockTs,
			Kind:    mvcc.WriteKindRollback,
		})
		resp.Action = kvrpcpb.Action_LockNotExistRollback
	}
	err = server.storage.Write(nil, txn.Writes())
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	return resp, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	resp := new(kvrpcpb.BatchRollbackResponse)
	reader, err := server.storage.Reader(nil)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	for _, key := range req.Keys {
		// check if key is committed
		write, ts, err := txn.CurrentWrite(key)
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionErr.RequestErr
				return resp, nil
			}
			return nil, err
		}
		if write != nil {
			if ts == txn.StartTS {
				// maybe this key has been rollback, do nothing
				continue
			} else {
				// this key has been committed, fail to rollback.
				resp.Error = &kvrpcpb.KeyError{
					Abort: "abort",
				}
				return resp, nil
			}
		}
		// delete lock and write rollback
		lock, err := txn.GetLock(key)
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionErr.RequestErr
				return resp, nil
			}
			return nil, err
		}
		if lock != nil && lock.Ts == txn.StartTS {
			txn.DeleteLock(key)
			txn.DeleteValue(key)
		}
		txn.PutWrite(key, req.StartVersion, &mvcc.Write{
			StartTS: req.StartVersion,
			Kind:    mvcc.WriteKindRollback,
		})
	}
	err = server.storage.Write(nil, txn.Writes())
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	return resp, nil
}

// Resolve lock will find all locks belonging to the transaction with the given start timestamp.
// If commit_version is 0, TinyKV will rollback all locks. If commit_version is greater than
// 0 it will commit those locks with the given commit timestamp.
// The client will make a resolve lock request for all secondary keys once it has successfully
// committed or rolled back the primary key.
func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	resp := new(kvrpcpb.ResolveLockResponse)
	reader, err := server.storage.Reader(nil)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	kvs, err := mvcc.AllLocksForTxn(txn)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	// if we want to commit, we need acquire latch first
	keys := [][]byte{}
	for _, kv := range kvs {
		keys = append(keys, kv.Key)
	}
	server.Latches.WaitForLatches(keys)
	defer server.Latches.ReleaseLatches(keys)
	if req.CommitVersion > 0 {
		for _, kv := range kvs {
			txn.DeleteLock(kv.Key)
			txn.PutWrite(kv.Key, req.CommitVersion, &mvcc.Write{
				StartTS: req.StartVersion,
				Kind:    kv.Lock.Kind,
			})
		}
	} else {
		for _, kv := range kvs {
			txn.DeleteLock(kv.Key)
			txn.DeleteValue(kv.Key)
			txn.PutWrite(kv.Key, req.StartVersion, &mvcc.Write{
				StartTS: req.StartVersion,
				Kind:    mvcc.WriteKindRollback,
			})
		}
	}
	err = server.storage.Write(nil, txn.Writes())
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	return resp, nil
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
