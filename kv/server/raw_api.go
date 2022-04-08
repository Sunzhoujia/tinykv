package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	cf := req.Cf
	key := req.Key

	resp := &kvrpcpb.RawGetResponse{}
	reader, err := server.storage.Reader(nil)

	if err != nil {
		return nil, err
	}
	defer reader.Close()

	item, err := reader.GetCF(cf, key)

	// Fail to find the key
	if item == nil {
		resp.NotFound = true
		resp.Error = "error"
		return resp, nil
	}

	resp.Value = item
	return resp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	cf, kv, val := req.Cf, req.Key, req.Value

	// Construct Storage.Modify to store data
	m := []storage.Modify{
		{
			Data: storage.Put{
				Cf:    cf,
				Key:   kv,
				Value: val,
			},
		},
	}
	err := server.storage.Write(nil, m)
	resp := &kvrpcpb.RawPutResponse{}

	if err != nil {
		resp.Error = "error"
		return resp, nil
	}

	return resp, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	cf, kv := req.Cf, req.Key

	m := []storage.Modify{
		{
			Data: storage.Delete{
				Key: kv,
				Cf:  cf,
			},
		},
	}

	err := server.storage.Write(nil, m)
	resp := &kvrpcpb.RawDeleteResponse{}

	if err != nil {
		resp.Error = "error"
		return resp, nil
	}

	return resp, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	startKey, cf, limit := req.StartKey, req.Cf, req.Limit

	reader, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	iter := reader.IterCF(cf)
	resp := &kvrpcpb.RawScanResponse{}

	kvs := []*kvrpcpb.KvPair{}

	i := 0
	for iter.Seek(startKey); iter.Valid(); iter.Next() {
		if i >= int(limit) {
			break
		}
		item := iter.Item()
		key := item.Key()
		val, _ := item.Value()

		kvs = append(kvs, &kvrpcpb.KvPair{
			Key:   key,
			Value: val,
		})
		i++
	}
	resp.Kvs = kvs

	iter.Close()
	return resp, nil
}
