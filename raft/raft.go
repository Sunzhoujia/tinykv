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
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strings"

	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64

	// add field
	randElectionTimeout int
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	// storage存的是已经持久化的hardstate，snapshot，entries
	raftlog := newLog(c.Storage)
	hs, cs, err := c.Storage.InitialState()
	log.Infof("hardstate: %v", hs)
	if err != nil {
		panic(err)
	}

	r := &Raft{
		id:               c.ID,
		Lead:             None,
		RaftLog:          raftlog,
		Term:             0,
		Vote:             None,
		Prs:              make(map[uint64]*Progress),
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
	}

	peers := cs.Nodes

	// 初始化对每个peer的nextIndex和matchIndex
	for _, p := range peers {
		r.Prs[p] = &Progress{Next: 1, Match: 0}
	}

	// 判断是否第一次启动, 不是的话从hardState加载Term，Vote
	if !IsEmptyHardState(hs) {
		r.loadState(hs)
	}

	if c.Applied > 0 {
		raftlog.appliedTo(c.Applied)
	}

	r.becomeFollower(r.Term, None)

	var nodesStrs []string
	for k, _ := range r.votes {
		nodesStrs = append(nodesStrs, fmt.Sprintf("%x", k))
	}
	log.Infof("newRaft %x [peers: [%s], term: %d ]",
		r.id, strings.Join(nodesStrs, ","), r.Term)

	return r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	pr := r.Prs[to]

	m := pb.Message{}
	m.To = to

	term, errt := r.RaftLog.Term(pr.Next - 1)
	// 获取Next之后的log
	ents, erre := r.RaftLog.Entries(pr.Next)

	if errt != nil || erre != nil {
		// TO-DO: send snapshot
		panic("need snapshot")
	} else {
		m.MsgType = pb.MessageType_MsgAppend
		m.Index = pr.Next - 1
		m.LogTerm = term
		m.Entries = r.RaftLog.EntsToP(ents)
		m.Commit = r.RaftLog.committed
		log.Infof("%x [term: %d, commit: %d, LastIndex: %d] send replication messages[PreIndex: %d, LogTerm: %d, entries %v] to %x",
			r.id, r.Term, r.RaftLog.committed, r.RaftLog.LastIndex(), m.Index, m.LogTerm, m.Entries, m.To)
	}

	r.send(m)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	commit := min(r.Prs[to].Match, r.RaftLog.committed)
	m := pb.Message{
		Term:    r.Term,
		To:      to,
		MsgType: pb.MessageType_MsgHeartbeat,
		Commit:  commit,
	}
	log.Infof("%x [term: %d, commit: %d, LastIndex: %d] send hearbeat to %x [matchIndex: %d, nextIndex: %d] ",
		r.id, r.Term, r.RaftLog.committed, r.RaftLog.LastIndex(), r.Prs[m.To].Match, r.Prs[m.To].Next, m.To)
	r.send(m)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	if r.State == StateLeader {
		r.tickHeartbeat()
	} else {
		r.tickElection()
	}
}

func (r *Raft) tickElection() {
	r.electionElapsed++
	// 如果触发electionTimeout，发送MsgHup
	if r.pastElectionTimeout() {
		r.electionElapsed = 0
		if err := r.Step(pb.Message{From: r.id, MsgType: pb.MessageType_MsgHup}); err != nil {
			log.Debugf("error occurred during election: %v", err)
		}
	}
}

func (r *Raft) tickHeartbeat() {
	r.heartbeatElapsed++

	// TO-DO: checkQuorum
	if r.State != StateLeader {
		return
	}
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0
		if err := r.Step(pb.Message{From: r.id, MsgType: pb.MessageType_MsgBeat}); err != nil {
			log.Debugf("error occurred during checking sending heartbeat: %v", err)
		}
	}
}

func (r *Raft) appendEntry(es ...*pb.Entry) (accepted bool) {
	li := r.RaftLog.LastIndex()
	ents := []pb.Entry{}
	for i := range es {
		es[i].Index = li + 1 + uint64(i)
		es[i].Term = r.Term
		ents = append(ents, *es[i])
	}

	r.RaftLog.append(ents...)
	r.Prs[r.id].maybeUpdate(r.RaftLog.LastIndex())

	// append之后，尝试一下是否可以进行commit。比如单机场景，append完就可以commit
	r.maybecommit()
	return true
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.reset(term)
	r.Lead = lead
	r.State = StateFollower

	log.Infof("%x became follower at term %d", r.id, r.Term)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	if r.State == StateLeader {
		panic("invalid transistion [leader --> candidate]")
	}

	r.reset(r.Term + 1)
	r.State = StateCandidate
	r.Vote = r.id
	log.Infof("%x became candidate at term %d", r.id, r.Term)
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	if r.State == StateFollower {
		panic("invalid transition [follower -> leader]")
	}

	r.reset(r.Term)
	r.Lead = r.id
	r.State = StateLeader

	r.appendEntry(&pb.Entry{Data: nil})
	log.Infof("%x became leader at term %d, l.entries %v", r.id, r.Term, r.RaftLog.entries)
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	//log.Infof("m.term %d", m.Term)
	// Handle the message term, which may result in out stepping down to a follower
	switch {
	case m.Term == 0:
		// local message
	case m.Term > r.Term:
		// 收到高Term的msg，直接变成Follower
		log.Infof("%x [term: %d] received a %s message with higher term from %x [term: %d]",
			r.id, r.Term, m.MsgType, m.From, m.Term)
		if m.MsgType == pb.MessageType_MsgHeartbeat || m.MsgType == pb.MessageType_MsgAppend || m.MsgType == pb.MessageType_MsgSnapshot {
			r.becomeFollower(m.Term, m.From)
		} else {
			r.becomeFollower(m.Term, None)
		}
	case m.Term < r.Term:
		// just ignore
		return nil
	}

	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.campaign()
	case pb.MessageType_MsgRequestVote: // r.Term == m.Term
		voteRespMsg := pb.MessageType_MsgRequestVoteResponse
		if (r.Vote == m.From || r.Vote == None) && r.RaftLog.isUpToDate(m.Index, m.LogTerm) {
			// 当前节点没有给其他节点投过票（r.Vote == None），或者是之前投过票的节点（r.Vote == m.From）
			log.Infof("%x [logterm: %d, index: %d, vote: %x] cast %s for %x [logterm: %d, index: %d] at term %d",
				r.id, r.RaftLog.LastTerm(), r.RaftLog.LastIndex(), r.Vote, m.MsgType, m.From, m.LogTerm, m.Index, r.Term)

			r.send(pb.Message{Term: r.Term, To: m.From, MsgType: voteRespMsg})
			// 重置选举超时，保存给哪个节点投票
			r.electionElapsed = 0
			r.Vote = m.From
		} else {
			log.Infof("%x [logterm: %d, index: %d, vote: %x] rejected %s for %x [logterm: %d, index: %d] at term %d",
				r.id, r.RaftLog.LastTerm(), r.RaftLog.LastIndex(), r.Vote, m.MsgType, m.From, m.LogTerm, m.Index, r.Term)
			r.send(pb.Message{Term: r.Term, To: m.From, MsgType: voteRespMsg, Reject: true})
		}
	}

	switch r.State {
	case StateFollower:
		return stepFollower(r, m)
	case StateCandidate:
		return stepCandidate(r, m)
	case StateLeader:
		return stepLeader(r, m)
	}
	return nil
}

// leader状态机
func stepLeader(r *Raft, m pb.Message) error {
	// leader不需要处理MsgRequestVoteResponse，直接丢弃
	switch m.MsgType {
	case pb.MessageType_MsgPropose:
		if len(m.Entries) == 0 {
			log.Panicf("%x stepped empty MsgProp", r.id)
		}

		// TO-DO: add check for ConfChange and leadTransferee

		if !r.appendEntry(m.Entries...) { // etcd中会进行流控，遵循了etcd中的设计先判断能否添加，实则未实现流控
			return ErrProposalDropped
		}
		r.bcastAppend()
		return nil
	case pb.MessageType_MsgBeat:
		r.bcastHeartbeat()
		return nil
	}

	pr := r.Prs[m.From]
	if pr == nil {
		log.Infof("%x no progress available for %x", r.id, m.From)
		return nil
	}

	switch m.MsgType {
	case pb.MessageType_MsgHeartbeatResponse:
		if pr.Match < r.RaftLog.LastIndex() {
			r.sendAppend(m.From)
		}
		return nil
	case pb.MessageType_MsgAppendResponse:
		preIndex := pr.Next - 1
		if m.Reject {

			log.Infof("%x received msgApp rejection(lastindex: %d) from %x for index %d",
				r.id, m.Index, m.From, preIndex)
			// index: 拒绝该AppendMsg节点的最大日志的索引
			if pr.maybeDecrTo(preIndex, m.Index) { // 尝试回退关于该节点的Match、Next索引
				log.Infof("%x decreased progress of %x to [%s]", r.id, m.From, pr)
				r.sendAppend(m.From)
			}
		} else { // 更新Follower的matchIndex，并尝试更新commitIndex
			oricommitted := r.RaftLog.committed
			if pr.maybeUpdate(m.Index) {
				if r.maybecommit() {
					r.bcastAppend() // 通知Follower更新了commitIndex
				}
			}
			log.Infof("%x received msgApp agreement(matchIndex: %d) from %x for index %d ,and update committed from %d to %d",
				r.id, m.Index, m.From, preIndex, oricommitted, r.RaftLog.committed)
		}
	}
	return nil
}

// candidate状态机
func stepCandidate(r *Raft, m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgPropose:
		log.Infof("%x no leader at term %d; dropping proposal", r.id, r.Term)
		return ErrProposalDropped
	case pb.MessageType_MsgRequestVoteResponse:
		gr := r.poll(m.From, m.MsgType, !m.Reject)
		log.Infof("%x [quorum:%d] has reveived %d votes and %d vote rejections", r.id, r.quorum(), gr, len(r.votes)-gr)

		switch r.quorum() {
		case gr:
			// 收到超过半数的赞成票，变成leader，发起一轮心跳消息
			r.becomeLeader()
			r.bcastAppend()
		case len(r.votes) - gr:
			// 收到超过半数反对票，变成Follower
			r.becomeFollower(r.Term, None)
		}
	case pb.MessageType_MsgHeartbeat:
		// 收到心跳消息，说明集群已经有leader，转换为follower
		r.becomeFollower(r.Term, m.From)
		r.handleHeartbeat(m)
	case pb.MessageType_MsgAppend:
		r.becomeFollower(m.Term, m.From)
		r.handleAppendEntries(m)
	}
	return nil

}

// follower状态机
func stepFollower(r *Raft, m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgPropose:
		// just drop
		// TO-DO : resend to leader if follower has a lead
		log.Infof("%x no leader at term %d; dropping proposal", r.id, r.Term)
		return ErrProposalDropped
	case pb.MessageType_MsgHeartbeat:
		r.electionElapsed = 0
		r.Lead = m.From
		r.handleHeartbeat(m)
	case pb.MessageType_MsgAppend:
		log.Infof("follower append entries %+v at term %d", m.Entries, r.Term)
		r.electionElapsed = 0
		r.Lead = m.From
		r.handleAppendEntries(m)
	}
	return nil
}

func (r *Raft) campaign() {
	if r.State == StateLeader {
		log.Infof("%x ignoring MsgHup because already leader", r.id)
		return
	}
	voteMsg := pb.MessageType_MsgRequestVote

	r.becomeCandidate()
	term := r.Term
	//log.Infof("quorum %d", r.quorum())
	if r.quorum() == r.poll(r.id, voteMsg, true) {
		r.becomeLeader()
		return
	}

	// 向集群中其他节点发投票信息
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		log.Infof("%x [logterm: %d, index %d] sent %s request to %x at Term %d",
			r.id, r.RaftLog.LastTerm(), r.RaftLog.LastIndex(), voteMsg, id, r.Term)

		r.send(pb.Message{Term: term, To: id, MsgType: voteMsg, Index: r.RaftLog.LastIndex(), LogTerm: r.RaftLog.LastTerm()})
	}
}

// send msg to its mailBox
func (r *Raft) send(m pb.Message) {
	if m.From == None {
		m.From = r.id
	}

	if m.MsgType == pb.MessageType_MsgRequestVote {
		if m.Term == 0 {
			// 投票时Term不能为0，涉及到preVote和Vote
			panic(fmt.Sprintf("term should be set when sending %s", m.MsgType))
		}
	} else {
		// 其他的消息类型，term必须为空, 在这里才去填充
		m.Term = r.Term
	}
	r.msgs = append(r.msgs, m)
}

func (r *Raft) bcastHeartbeat() {
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		r.sendHeartbeat(id)
	}
}

func (r *Raft) bcastAppend() {
	// send appendMsg for all peers
	for id, _ := range r.Prs {
		if id == r.id {
			continue
		}
		r.sendAppend(id)
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	// 用于日志匹配的条目在committed之前，说明这是一条过期的消息，因此直接返回MsgAppResp消息，并将消息的Index字段置为committed的值，以让leader快速更新该follower的next index。
	if m.Index < r.RaftLog.committed {
		r.send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgAppendResponse, Index: r.RaftLog.committed})
		return
	}

	// 验证用于日志匹配的字段Term与Index是否与本地的日志匹配。如果匹配并保存了日志，则返回MsgAppResp消息，并将消息的Index字段置为本地最后一条日志的index，以让leader发送后续的日志。
	//log.Infof("handle append entries: %v", m.Entries)
	if mlastIndex, ok := r.RaftLog.maybeAppend(m.Index, m.LogTerm, m.Commit, r.RaftLog.PToEnts(m.Entries)...); ok {
		log.Infof("%x [Term: %d, matchIndex: %d] accepted msgApp [logterm: %d, index: %d] from %x",
			r.id, r.Term, r.RaftLog.LastIndex(), m.LogTerm, m.Index, m.From)
		r.send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgAppendResponse, Index: mlastIndex})
	} else {
		// logterm = 0 表示这条log已经compacted或unavailable
		log.Infof("%x [logterm: %d, index: %d] rejected msgApp [logterm: %d, index: %d] from %x",
			r.id, r.RaftLog.zeroTermOnErrCompacted(r.RaftLog.Term(m.Index)), m.Index, m.LogTerm, m.Index, m.From)
		// reject = true, m.Index设置为Follower的lastIndex，加速恢复
		r.send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgAppendResponse, Index: r.RaftLog.LastIndex(), Reject: true})
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	r.RaftLog.commitTo(m.Commit)
	r.send(pb.Message{Term: r.Term, To: m.From, MsgType: pb.MessageType_MsgHeartbeatResponse})
	log.Infof("%x [Term %d, commit: %d] send heartbeatResp to %x", r.id, r.Term, r.RaftLog.committed, m.To)
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

// maybeCommit attempts to advance the commit index. Returns true if
// the commit index changed
func (r *Raft) maybecommit() bool {
	mci := r.committed() // calculate max matchIndex according Prs
	return r.RaftLog.maybeCommit(mci, r.Term)
}

func (r *Raft) reset(term uint64) {
	if r.Term != term {
		r.Term = term
		r.Vote = None
	}

	r.Lead = None

	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.resetRandomizedElectionTimeout()

	r.votes = map[uint64]bool{}

	// reset prs
	for id := range r.Prs {
		r.Prs[id] = &Progress{Next: r.RaftLog.LastIndex() + 1, Match: 0}
		if id == r.id {
			r.Prs[id].Match = r.RaftLog.LastIndex()
		}
	}

}

func (r *Raft) resetRandomizedElectionTimeout() {
	r.randElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
}

func (r *Raft) pastElectionTimeout() bool {
	return r.electionElapsed >= r.randElectionTimeout
}

func (r *Raft) loadState(state pb.HardState) {
	if state.Commit < r.RaftLog.committed || state.Commit > r.RaftLog.LastIndex() {
		log.Panicf("%x state.commit %d is out of range [%d, %d]", r.id, state.Commit, r.RaftLog.committed, r.RaftLog.LastIndex())
	}
	r.RaftLog.committed = state.Commit
	r.Term = state.Term
	r.Vote = state.Vote
}

// 计算有多少节点给candidate投票
func (r *Raft) poll(id uint64, t pb.MessageType, v bool) (granted int) {
	if v {
		log.Infof("%x received %s from %x at term %d", r.id, t, id, r.Term)
	} else {
		log.Infof("%x received %s rejection from %x at term %d", r.id, t, id, r.Term)
	}
	// 如果id没有投票过，那么更新id的投票情况
	if _, ok := r.votes[id]; !ok {
		r.votes[id] = v
	}
	// 计算下都有多少节点已经投票给自己了
	for _, vv := range r.votes {
		if vv {
			granted++
		}
	}
	return granted
}

func (r *Raft) quorum() int {
	return len(r.Prs)/2 + 1
}

func (r *Raft) committed() uint64 {
	n := len(r.Prs)
	if n == 0 {
		return math.MaxUint64
	}

	srt := make([]uint64, n)

	i := n - 1
	for _, pr := range r.Prs {
		srt[i] = pr.Match
		i--
	}

	// 升序排序
	sort.Slice(srt, func(i, j int) bool {
		return srt[i] < srt[j]
	})

	pos := n - (n/2 + 1)
	return srt[pos]
}

func (r *Raft) softState() *SoftState { return &SoftState{Lead: r.Lead, RaftState: r.State} }

func (r *Raft) hardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
}
