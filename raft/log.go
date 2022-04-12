// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	if storage == nil {
		log.Panicf("storage must not be nil")
	}

	raftlog := &RaftLog{
		storage: storage,
	}

	fistIndex, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	lastIndex, err := storage.LastIndex()
	if err != nil {
		panic(err)
	}

	// lastIndex is the last persisted index
	raftlog.stabled = lastIndex

	raftlog.committed = fistIndex - 1
	raftlog.applied = fistIndex - 1

	return raftlog
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	return nil
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	return nil
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	// 先判断log.entries里面有没有entry，没有则去storage里去找
	if len(l.entries) != 0 {
		return l.entries[0].Index + uint64(len(l.entries)) - 1
	}

	index, err := l.storage.LastIndex()
	if err != nil {
		panic(err)
	}
	return index
}

// LastIndex return the first index of the log entries(entrirs[0]除外)
func (l *RaftLog) FirstIndex() uint64 {
	// Your Code Here (2A).
	index, err := l.storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	return index
}

// 返回最后一个索引的term
func (l *RaftLog) LastTerm() uint64 {
	t, err := l.Term(l.LastIndex())
	if err != nil {
		log.Panicf("unexpected error when getting the last term (%v)", err)
	}
	return t
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	// the valid term range is [index of dummy entry, last index]
	dummyIndex := l.FirstIndex() - 1
	if i < dummyIndex || i > l.LastIndex() {
		log.Panicf("unexpected error when getting the %d term", i)
	}

	// entry在l.entries中
	if len(l.entries) > 0 && i >= l.entries[0].Index {
		return l.entries[i-l.entries[0].Index].Term, nil
	}

	// entry在storage中
	t, err := l.storage.Term(i)
	if err == nil {
		return t, nil
	}

	if err == ErrCompacted || err == ErrUnavailable {
		return 0, err
	}
	panic(err)
}

func (l *RaftLog) isUpToDate(lastIndex, term uint64) bool {
	return term > l.LastTerm() || (term == l.LastTerm() && lastIndex >= l.LastIndex())
}

func (l *RaftLog) commitTo(tocommit uint64) {
	if l.committed < tocommit {
		if l.LastIndex() < tocommit {
			log.Panicf("tocommit(%d) is out of range [lastIndex(%d)]. Was the raft log corrupted, truncated, or lost?", tocommit, l.LastIndex())
		}
		l.committed = tocommit
		log.Infof("commit to %d", tocommit)
	}
}
