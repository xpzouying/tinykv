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

	r, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}

	data, err := r.GetCF(req.GetCf(), req.GetKey())
	if err != nil {
		return nil, err
	}

	if data == nil {
		return &kvrpcpb.RawGetResponse{
			NotFound: true,
		}, nil
	}

	return &kvrpcpb.RawGetResponse{
		Value: data,
	}, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified

	batch := []storage.Modify{
		{
			Data: storage.Put{
				Cf:    req.GetCf(),
				Key:   req.GetKey(),
				Value: req.GetValue(),
			},
		},
	}
	if err := server.storage.Write(nil, batch); err != nil {
		return nil, err
	}

	return &kvrpcpb.RawPutResponse{}, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	return nil, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	return nil, nil
}
