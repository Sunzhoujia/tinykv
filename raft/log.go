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
	"fmt"

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
	// pendingsnapshot仅当接收从leader发送的snapshot时才存在
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

	raftlog.entries = append([]pb.Entry{}, entries...)

	// lastIndex is the last persisted index
	raftlog.stabled = lastIndex

	// firstIndex前面的就是已经做了snapshot的log，一定applied了
	raftlog.committed = firstIndex - 1
	raftlog.applied = firstIndex - 1
	log.Infof("raftLog: firstIndex %d, lastIndex %d, entry %v", raftlog.FirstIndex(), raftlog.LastIndex(), raftlog.entries)
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
	return l.slice(l.stabled+1, l.LastIndex()+1)
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	return l.slice(l.applied+1, l.committed+1)
}

// hasNextEnts returns if there is any available entries for execution. This
// is a fast check without heavy raftLog.slice() in raftLog.nextEnts().
func (l *RaftLog) hasNextEnts() bool {
	//off := max(l.applied+1, l.FirstIndex())
	off := l.applied + 1
	return l.committed+1 > off
}

// hasPendingSnapshot returns if there is pending snapshot waiting for applying.
func (l *RaftLog) hasPendingSnapshot() bool {
	return l.pendingSnapshot != nil && !IsEmptySnap(l.pendingSnapshot)
}

// return entries from given index
// l think compact often happen, so won't return many entries
func (l *RaftLog) Entries(i uint64) ([]pb.Entry, error) {
	if i > l.LastIndex() {
		return nil, nil
	}

	// i <= dummIndex，已经无法获取该log
	if i < l.FirstIndex() {
		return nil, ErrCompacted
	}

	//log.Infof("Entries %v, [%d, %d)", l.entries, i, l.LastIndex()+1)
	return l.slice(i, l.LastIndex()+1), nil
}

// because msg need send []*pb.Entry, so we need change to pointer slice
func (l *RaftLog) EntsToP(ents []pb.Entry) []*pb.Entry {
	pents := []*pb.Entry{}
	for i := 0; i < len(ents); i++ {
		pents = append(pents, &ents[i])
	}
	return pents
}

func (l *RaftLog) PToEnts(pents []*pb.Entry) []pb.Entry {
	ents := []pb.Entry{}
	for _, e := range pents {
		ents = append(ents, *e)
	}
	return ents
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
		log.Infof("truncateAndAppend case 2, start %d, end %d, lastIndex %d", start, end, l.LastIndex())
		firstEnts := l.slice(l.FirstIndex(), start)
		l.entries = append(firstEnts, ents...)
	default:
		log.Infof("truncateAndAppend case 3: start %d, end %d", start, end)
		l.entries = append([]pb.Entry{}, l.slice(l.FirstIndex(), start)...)
		l.entries = append(l.entries, ents...)
	}
	// update l.stabled here?
	// if log rollback，l.stable must rollback
	// because we need construct Ready's Entries for upper app to persist
	l.stabled = min(l.stabled, start-1)
}

// entry index range [li, hi)
func (l *RaftLog) slice(lo uint64, hi uint64) []pb.Entry {
	if lo > hi {
		log.Panicf("invalid slice %d > %d", lo, hi)
	}
	if lo < l.FirstIndex() || hi > l.LastIndex()+1 {
		log.Panicf("raftLog.entries[%d,%d) out of bound [%d,%d]", lo, hi, l.FirstIndex()-1, l.LastIndex())
	}
	offset := l.FirstIndex()
	//log.Infof("entries [%d, %d), %v", lo, hi, l.entries[lo-offset:hi-offset])
	return l.entries[lo-offset : hi-offset]
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	// 先判断log.entries里面有没有entry，没有则去storage里去找
	if len(l.entries) == 0 {
		return l.dummyIndex
	}

	return l.entries[uint64(len(l.entries)-1)].Index
}

// FirstIndex return the first index of the log entries
// FirstIndex - 1 = dummyIndex
func (l *RaftLog) FirstIndex() uint64 {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		return l.dummyIndex + 1
	}

	return l.entries[0].Index
}

// 返回最后一个索引的term
func (l *RaftLog) LastTerm() uint64 {
	t, _ := l.Term(l.LastIndex())
	// if err != nil {
	// 	log.Panicf("unexpected error when getting the last term (%v)", err)
	// }
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

	if i == l.dummyIndex {
		if l.pendingSnapshot != nil {
			return l.pendingSnapshot.Metadata.Term, nil
		} else {
			return l.storage.Term(i)
		}
	}

	offset := l.FirstIndex()
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

func (l *RaftLog) stableSnapTo(i uint64) {
	if l.pendingSnapshot != nil && l.pendingSnapshot.Metadata.Index == i {
		l.pendingSnapshot = nil
	}
}

func (l *RaftLog) stableTo(i, t uint64) {
	// 1. entry为snapshot 的最后一个entry
	// 2. entry在raftLog.entries中
	gt, err := l.Term(i)
	if err != nil {
		return
	}

	if gt == t && i >= l.stabled {
		l.stabled = i
	}
}

// check if matchIndex.Term == term
func (l *RaftLog) maybeCommit(maxIndex, term uint64) bool {
	if maxIndex > l.committed && l.zeroTermOnErrCompacted(l.Term(maxIndex)) == term {
		l.commitTo(maxIndex)
		return true
	}
	return false
}

// 1. log match
// 2. append log entries
// 3. update commitIndex
func (l *RaftLog) maybeAppend(index, logTerm, committed uint64, ents ...pb.Entry) (lastnewi uint64, ok bool) {
	if l.matchTerm(index, logTerm) {
		lastnewi = index + uint64(len(ents))
		ci := l.findConflict(ents)
		log.Infof("match firstIndex %d", ci)
		switch {
		case ci == 0:
			// 已拥有全部新日志，不需要append了
		case ci <= l.committed:
			// 如果返回值小于当前的committed索引，说明committed前的日志发生了冲突，这违背了Raft算法保证的Log Matching性质，因此会引起panic。
			log.Panicf("entry %d conflict with committed entry [committed(%d)]", ci, l.committed)
		default:
			// 如果返回值大于committed，既可能是冲突发生在committed之后，也可能是有新日志，
			// 但二者的处理方式都是相同的，即从冲突处或新日志处开始的日志覆盖或追加到当前日志中即可。
			offset := index + 1
			if ci-offset > uint64(len(ents)) {
				log.Panicf("index, %d, is out of range [%d]", ci-offset, len(ents))
			}
			l.append(ents[ci-offset:]...)
		}
		// 更新当前的committed索引为给定的新日志中最后一条日志的index（lastnewi）和传入的新的committed中较小的一个
		// commitTo方法保证了committed索引只会前进而不会回退，而使用lastnewi和传入的committed中的最小值则是因为传入的数据可能有如下两种情况：
		// 1. leader给follower复制日志时，如果复制的日志条目超过了单个消息的上限，
		//    则可能出现leader传给follower的committed值大于该follower复制完这条消息中的日志后的最大index。此时，该follower的新committed值为lastnewi。
		// 2. follower能够跟上leader，leader传给follower的日志中有未确认被法定数量节点稳定存储的日志，
		//    此时传入的committed比lastnewi小，该follower的新committed值为传入的committed值。
		l.commitTo(min(committed, lastnewi))
		return lastnewi, true
	}

	return 0, false
}

func (l *RaftLog) zeroTermOnErrCompacted(t uint64, err error) uint64 {
	if err == nil || err == ErrUnavailable {
		return t
	}
	if err == ErrCompacted {
		return 0
	}

	log.Panicf("unexpected error (%v)", err)
	return 0
}

func (l *RaftLog) matchTerm(index, term uint64) bool {
	t, err := l.Term(index)
	if err != nil {
		return false
	}
	return t == term
}

// 1. 给定的日志与已有的日志的index和term冲突，返回第一条冲突的日志条目的index。
// 2. 没有冲突，且给定的日志的所有条目均已在已有日志中，返回0.
// 3. 给定的日志中包含已有日志中没有的新日志，返回第一条新日志的index。
func (l *RaftLog) findConflict(ents []pb.Entry) uint64 {
	for _, ne := range ents {
		if !l.matchTerm(ne.Index, ne.Term) {
			if ne.Index <= l.LastIndex() {
				//log.Infof("ne.Index %d, l.lastIndex %d", ne.Index, l.LastIndex())
				log.Infof("found conflict at index %d [existing term: %d, conflicting term: %d]",
					ne.Index, l.zeroTermOnErrCompacted(l.Term(ne.Index)), ne.Term)
			}
			return ne.Index
		}
	}
	return 0
}

// 先从pendingSnapshot里拿，拿不到再去storage.Snapshot()里拿
func (l *RaftLog) snapshot() (pb.Snapshot, error) {
	// Follower应用完snapshot后应该直接清除pendingSnapshot了
	if l.pendingSnapshot != nil {
		log.Infof("Get snapshot from pendingSnapshot?????")
		return *l.pendingSnapshot, nil
	}
	return l.storage.Snapshot()
}

func (l *RaftLog) restore(s *pb.Snapshot) {
	log.Infof("log [%s] starts to restore snapshot [index: %d, term: %d]", l, s.Metadata.Index, s.Metadata.Term)
	l.committed = s.Metadata.Index
	l.stabled = s.Metadata.Index
	l.dummyIndex = s.Metadata.Index
	l.pendingSnapshot = s
	// 都把entries情况了，所以直接将l.applie置为snapIndex，否则上层在获取ready，调用nextentries会出错
	l.applied = s.Metadata.Index
	// 直接全部compact掉就行？
	// 走到这一步说明snap的最后一条entry是和raftlog不匹配的，所以即使raftlog有在snapIdnex后面还有log，也会被leader给覆盖掉，所以直接丢弃就行
	// lastIndex会返回dummyIndex = snapIndex
	l.entries = []pb.Entry{}
}

func (l *RaftLog) String() string {
	return fmt.Sprintf("committed=%d, applied=%d, stabled=%d, lastIndex=%d", l.committed, l.applied, l.stabled, l.LastIndex())
}
