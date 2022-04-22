package raftstore

import "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"

type applyTask struct {
	peer  *peer
	entry *eraftpb.Entry
}

type applyWorker struct {
	closeCh <-chan struct{}
	applyCh chan applyTask
}
