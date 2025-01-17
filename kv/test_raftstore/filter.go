package test_raftstore

import (
	"math/rand"

	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
)

type Filter interface {
	Before(msgs *rspb.RaftMessage) bool
	After()
}

type PartitionFilter struct {
	s1 []uint64
	s2 []uint64
}

// 如果msg的To和From都在同一分区，返回false，不在同一分区，返回true
func (f *PartitionFilter) Before(msg *rspb.RaftMessage) bool {
	inS1 := false
	inS2 := false
	for _, storeID := range f.s1 {
		if msg.FromPeer.StoreId == storeID || msg.ToPeer.StoreId == storeID {
			inS1 = true
			break
		}
	}
	for _, storeID := range f.s2 {
		if msg.FromPeer.StoreId == storeID || msg.ToPeer.StoreId == storeID {
			inS2 = true
			break
		}
	}
	return !(inS1 && inS2)
}

func (f *PartitionFilter) After() {}

type DropFilter struct{}

// 0.9 true， 0.1 false
func (f *DropFilter) Before(msg *rspb.RaftMessage) bool {
	return (rand.Int() % 1000) > 100
}

func (f *DropFilter) After() {}
