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
	// all the entries after last snapshot
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	// last snapshot index and all entries index > dummyIndex
	dummyIndex uint64
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

	firstIndex, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	lastIndex, err := storage.LastIndex()
	if err != nil {
		panic(err)
	}

	entries, err := storage.Entries(firstIndex, lastIndex+1)
	if err != nil {
		panic(err)
	}

	// snapshot index
	raftlog.dummyIndex = firstIndex - 1

	// 先存dummyEntry
	raftlog.entries = append([]pb.Entry{}, storage.DummyEntry())
	raftlog.entries = append(raftlog.entries, entries...)

	// lastIndex is the last persisted index
	raftlog.stabled = lastIndex

	// firstIndex前面的就是已经做了snapshot的log，一定applied了
	raftlog.committed = firstIndex - 1
	raftlog.applied = firstIndex - 1

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

// Append log entry, return l.lastIndex()
func (l *RaftLog) append(ents ...pb.Entry) uint64 {
	if len(ents) == 0 {
		return l.LastIndex()
	}

	// 如果索引小于committed，则说明该数据是非法的
	if after := ents[0].Index - 1; after < l.committed {
		log.Panicf("after(%d) is out of range [committed(%d)]", after, l.committed)
	}

	l.truncateAndAppend(ents)
	return l.LastIndex()
}

// appendEntries必定经过了日志匹配，所以只可能有以下三种场景：
// 1. ents恰好接在l.entries后面
// 2. ents被l.entries包含
// 3. ents和l.entries重叠了一段
func (l *RaftLog) truncateAndAppend(ents []pb.Entry) {
	start := ents[0].Index
	end := ents[uint64(len(ents)-1)].Index
	switch {
	case start == l.LastIndex()+1:
		log.Infof("truncateAndAppend case 1")
		l.entries = append(l.entries, ents...)
	case end < l.LastIndex():
		log.Infof("truncateAndAppend case 2")
		// [dummIndex, start) + [start, end] + [end+1, lastIndex]
		l.entries = append([]pb.Entry{}, l.slice(l.FirstIndex()-1, start)...)
		l.entries = append(l.entries, ents...)
		l.entries = append(l.entries, l.slice(end+1, l.LastIndex()+1)...)
	default:
		log.Infof("truncateAndAppend case 3")
		// [dummIndex, start) + [start, end]
		l.entries = append([]pb.Entry{}, l.slice(l.FirstIndex()-1, start)...)
		l.entries = append(l.entries, ents...)
	}
}

// entry index range [li, hi)
func (l *RaftLog) slice(lo uint64, hi uint64) []pb.Entry {
	if lo > hi {
		log.Panicf("invalid slice %d > %d", lo, hi)
	}
	if lo < l.FirstIndex()-1 || hi > l.LastIndex() {
		log.Panicf("raftLog.entries[%d,%d) out of bound [%d,%d]", lo, hi, l.FirstIndex()-1, l.LastIndex())
	}
	offset := l.FirstIndex() - 1

	return l.entries[lo-offset : hi-offset]
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	// 先判断log.entries里面有没有entry，没有则去storage里去找
	if len(l.entries) == 0 {
		log.Panicf("raftLog.entries is empty")
	}

	return l.entries[uint64(len(l.entries)-1)].Index
}

// FirstIndex return the first index of the log entries
// FirstIndex - 1 = dummyIndex
func (l *RaftLog) FirstIndex() uint64 {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		log.Panicf("raftLog.entries is empty")
	}

	return l.entries[0].Index + 1
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
	if i < l.FirstIndex()-1 {
		return 0, ErrCompacted
	}

	if i > l.LastIndex() {
		return 0, ErrUnavailable
	}

	offset := l.FirstIndex() - 1
	return l.entries[i-offset].Term, nil
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

func (l *RaftLog) appliedTo(i uint64) {
	if i == 0 {
		return
	}
	if l.committed < i || i < l.applied {
		log.Panicf("applied(%d) is out of range [prevApplied(%d), committed(%d)]", i, l.applied, l.committed)
	}
	l.applied = i
}
