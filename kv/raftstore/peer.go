package raftstore

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"time"

	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/kv/util/worker"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/raft"
	"github.com/pingcap/errors"
)

func NotifyStaleReq(term uint64, cb *message.Callback) {
	cb.Done(ErrRespStaleCommand(term))
}

func NotifyReqRegionRemoved(regionId uint64, cb *message.Callback) {
	regionNotFound := &util.ErrRegionNotFound{RegionId: regionId}
	resp := ErrResp(regionNotFound)
	cb.Done(resp)
}

// If we create the peer actively, like bootstrap/split/merge region, we should
// use this function to create the peer. The region must contain the peer info
// for this store.
func createPeer(storeID uint64, cfg *config.Config, sched chan<- worker.Task,
	engines *engine_util.Engines, region *metapb.Region) (*peer, error) {
	// 一个region对应多个peer，在这里找到这个store对应的peer
	metaPeer := util.FindPeer(region, storeID)
	if metaPeer == nil {
		return nil, errors.Errorf("find no peer for store %d in region %v", storeID, region)
	}
	log.Infof("region %v create peer with ID %d", region, metaPeer.Id)
	return NewPeer(storeID, cfg, engines, region, sched, metaPeer)
}

// The peer can be created from another node with raft membership changes, and we only
// know the region_id and peer_id when creating this replicated peer, the region info
// will be retrieved later after applying snapshot.
func replicatePeer(storeID uint64, cfg *config.Config, sched chan<- worker.Task,
	engines *engine_util.Engines, regionID uint64, metaPeer *metapb.Peer) (*peer, error) {
	// We will remove tombstone key when apply snapshot
	log.Infof("[region %v] replicates peer with ID %d", regionID, metaPeer.GetId())
	region := &metapb.Region{
		Id:          regionID,
		RegionEpoch: &metapb.RegionEpoch{},
	}
	return NewPeer(storeID, cfg, engines, region, sched, metaPeer)
}

type proposal struct {
	// index + term for unique identification
	index uint64
	term  uint64
	cb    *message.Callback
}

type peer struct {
	// The ticker of the peer, used to trigger
	// * raft tick
	// * raft log gc
	// * region heartbeat
	// * split check
	ticker *ticker
	// Instance of the Raft module
	RaftGroup *raft.RawNode
	// The peer storage for the Raft module
	peerStorage *PeerStorage

	// Record the meta information of the peer
	Meta     *metapb.Peer
	regionId uint64
	// Tag which is useful for printing log
	Tag string

	// Record the callback of the proposals
	// (Used in 2B)
	proposals []*proposal

	// Index of last scheduled compacted raft log.
	// (Used in 2C)
	LastCompactedIdx uint64

	// Cache the peers information from other stores
	// when sending raft messages to other peers, it's used to get the store id of target peer
	// (Used in 3B conf change)
	peerCache map[uint64]*metapb.Peer
	// Record the instants of peers being added into the configuration.
	// Remove them after they are not pending any more.
	// (Used in 3B conf change)
	PeersStartPendingTime map[uint64]time.Time
	// Mark the peer as stopped, set when peer is destroyed
	// (Used in 3B conf change)
	stopped bool

	// An inaccurate difference in region size since last reset.
	// split checker is triggered when it exceeds the threshold, it makes split checker not scan the data very often
	// (Used in 3B split)
	SizeDiffHint uint64
	// Approximate size of the region.
	// It's updated everytime the split checker scan the data
	// (Used in 3B split)
	ApproximateSize *uint64
}

func NewPeer(storeId uint64, cfg *config.Config, engines *engine_util.Engines, region *metapb.Region, regionSched chan<- worker.Task,
	meta *metapb.Peer) (*peer, error) {
	if meta.GetId() == util.InvalidID {
		return nil, fmt.Errorf("invalid peer id")
	}
	tag := fmt.Sprintf("[region %v] %v", region.GetId(), meta.GetId())

	ps, err := NewPeerStorage(engines, region, regionSched, tag)
	if err != nil {
		return nil, err
	}

	appliedIndex := ps.AppliedIndex()

	raftCfg := &raft.Config{
		ID:            meta.GetId(),
		ElectionTick:  cfg.RaftElectionTimeoutTicks,
		HeartbeatTick: cfg.RaftHeartbeatTicks,
		Applied:       appliedIndex,
		Storage:       ps,
	}

	raftGroup, err := raft.NewRawNode(raftCfg)
	if err != nil {
		return nil, err
	}
	p := &peer{
		Meta:                  meta,
		regionId:              region.GetId(),
		RaftGroup:             raftGroup,
		peerStorage:           ps,
		peerCache:             make(map[uint64]*metapb.Peer),
		PeersStartPendingTime: make(map[uint64]time.Time),
		Tag:                   tag,
		ticker:                newTicker(region.GetId(), cfg),
	}

	// If this region has only one peer and I am the one, campaign directly.
	if len(region.GetPeers()) == 1 && region.GetPeers()[0].GetStoreId() == storeId {
		err = p.RaftGroup.Campaign()
		if err != nil {
			return nil, err
		}
	}

	return p, nil
}

func (p *peer) insertPeerCache(peer *metapb.Peer) {
	p.peerCache[peer.GetId()] = peer
}

func (p *peer) removePeerCache(peerID uint64) {
	delete(p.peerCache, peerID)
}

func (p *peer) getPeerFromCache(peerID uint64) *metapb.Peer {
	if peer, ok := p.peerCache[peerID]; ok {
		return peer
	}
	for _, peer := range p.peerStorage.Region().GetPeers() {
		if peer.GetId() == peerID {
			p.insertPeerCache(peer)
			return peer
		}
	}
	return nil
}

func (p *peer) nextProposalIndex() uint64 {
	return p.RaftGroup.Raft.RaftLog.LastIndex() + 1
}

/// Tries to destroy itself. Returns a job (if needed) to do more cleaning tasks.
func (p *peer) MaybeDestroy() bool {
	if p.stopped {
		log.Infof("%v is being destroyed, skip", p.Tag)
		return false
	}
	return true
}

/// Does the real destroy worker.Task which includes:
/// 1. Set the region to tombstone;
/// 2. Clear data;
/// 3. Notify all pending requests.
func (p *peer) Destroy(engine *engine_util.Engines, keepData bool) error {
	start := time.Now()
	region := p.Region()
	log.Infof("%v begin to destroy", p.Tag)

	// Set Tombstone state explicitly
	kvWB := new(engine_util.WriteBatch)
	raftWB := new(engine_util.WriteBatch)
	if err := p.peerStorage.clearMeta(kvWB, raftWB); err != nil {
		return err
	}
	meta.WriteRegionState(kvWB, region, rspb.PeerState_Tombstone)
	// write kv rocksdb first in case of restart happen between two write
	if err := kvWB.WriteToDB(engine.Kv); err != nil {
		return err
	}
	if err := raftWB.WriteToDB(engine.Raft); err != nil {
		return err
	}

	if p.peerStorage.isInitialized() && !keepData {
		// If we meet panic when deleting data and raft log, the dirty data
		// will be cleared by a newer snapshot applying or restart.
		p.peerStorage.ClearData()
	}

	for _, proposal := range p.proposals {
		NotifyReqRegionRemoved(region.Id, proposal.cb)
	}
	p.proposals = nil

	log.Infof("%v destroy itself, takes %v", p.Tag, time.Now().Sub(start))
	return nil
}

func (p *peer) isInitialized() bool {
	return p.peerStorage.isInitialized()
}

func (p *peer) storeID() uint64 {
	return p.Meta.StoreId
}

func (p *peer) Region() *metapb.Region {
	return p.peerStorage.Region()
}

/// Set the region of a peer.
///
/// This will update the region of the peer, caller must ensure the region
/// has been preserved in a durable device.
func (p *peer) SetRegion(region *metapb.Region) {
	p.peerStorage.SetRegion(region)
}

func (p *peer) PeerId() uint64 {
	return p.Meta.GetId()
}

func (p *peer) LeaderId() uint64 {
	return p.RaftGroup.Raft.Lead
}

func (p *peer) IsLeader() bool {
	return p.RaftGroup.Raft.State == raft.StateLeader
}

func (p *peer) Send(trans Transport, msgs []eraftpb.Message) {
	for _, msg := range msgs {
		err := p.sendRaftMessage(msg, trans)
		if err != nil {
			log.Debugf("%v send message err: %v", p.Tag, err)
		}
	}
}

/// Collects all pending peers and update `peers_start_pending_time`.
func (p *peer) CollectPendingPeers() []*metapb.Peer {
	pendingPeers := make([]*metapb.Peer, 0, len(p.Region().GetPeers()))
	truncatedIdx := p.peerStorage.truncatedIndex()
	for id, progress := range p.RaftGroup.GetProgress() {
		if id == p.Meta.GetId() {
			continue
		}
		if progress.Match < truncatedIdx {
			if peer := p.getPeerFromCache(id); peer != nil {
				pendingPeers = append(pendingPeers, peer)
				if _, ok := p.PeersStartPendingTime[id]; !ok {
					now := time.Now()
					p.PeersStartPendingTime[id] = now
					log.Debugf("%v peer %v start pending at %v", p.Tag, id, now)
				}
			}
		}
	}
	return pendingPeers
}

func (p *peer) clearPeersStartPendingTime() {
	for id := range p.PeersStartPendingTime {
		delete(p.PeersStartPendingTime, id)
	}
}

/// Returns `true` if any new peer catches up with the leader in replicating logs.
/// And updates `PeersStartPendingTime` if needed.
func (p *peer) AnyNewPeerCatchUp(peerId uint64) bool {
	if len(p.PeersStartPendingTime) == 0 {
		return false
	}
	if !p.IsLeader() {
		p.clearPeersStartPendingTime()
		return false
	}
	if startPendingTime, ok := p.PeersStartPendingTime[peerId]; ok {
		truncatedIdx := p.peerStorage.truncatedIndex()
		progress, ok := p.RaftGroup.Raft.Prs[peerId]
		if ok {
			if progress.Match >= truncatedIdx {
				delete(p.PeersStartPendingTime, peerId)
				elapsed := time.Since(startPendingTime)
				log.Debugf("%v peer %v has caught up logs, elapsed: %v", p.Tag, peerId, elapsed)
				return true
			}
		}
	}
	return false
}

func (p *peer) MaybeCampaign(parentIsLeader bool) bool {
	// The peer campaigned when it was created, no need to do it again.
	if len(p.Region().GetPeers()) <= 1 || !parentIsLeader {
		return false
	}

	// If last peer is the leader of the region before split, it's intuitional for
	// it to become the leader of new split region.
	p.RaftGroup.Campaign()
	return true
}

func (p *peer) Term() uint64 {
	return p.RaftGroup.Raft.Term
}

func (p *peer) HeartbeatScheduler(ch chan<- worker.Task) {
	clonedRegion := new(metapb.Region)
	err := util.CloneMsg(p.Region(), clonedRegion)
	if err != nil {
		return
	}
	ch <- &runner.SchedulerRegionHeartbeatTask{
		Region:          clonedRegion,
		Peer:            p.Meta,
		PendingPeers:    p.CollectPendingPeers(),
		ApproximateSize: p.ApproximateSize,
	}
}

func (p *peer) sendRaftMessage(msg eraftpb.Message, trans Transport) error {
	sendMsg := new(rspb.RaftMessage)
	sendMsg.RegionId = p.regionId
	// set current epoch
	sendMsg.RegionEpoch = &metapb.RegionEpoch{
		ConfVer: p.Region().RegionEpoch.ConfVer,
		Version: p.Region().RegionEpoch.Version,
	}

	fromPeer := *p.Meta
	toPeer := p.getPeerFromCache(msg.To)
	if toPeer == nil {
		return fmt.Errorf("failed to lookup recipient peer %v in region %v", msg.To, p.regionId)
	}
	log.Debugf("%v, send raft msg %v from %v to %v", p.Tag, msg.MsgType, fromPeer, toPeer)

	sendMsg.FromPeer = &fromPeer
	sendMsg.ToPeer = toPeer

	// There could be two cases:
	// 1. Target peer already exists but has not established communication with leader yet
	// 2. Target peer is added newly due to member change or region split, but it's not
	//    created yet
	// For both cases the region start key and end key are attached in RequestVote and
	// Heartbeat message for the store of that peer to check whether to create a new peer
	// when receiving these messages, or just to wait for a pending region split to perform
	// later.
	if p.peerStorage.isInitialized() && util.IsInitialMsg(&msg) {
		sendMsg.StartKey = append([]byte{}, p.Region().StartKey...)
		sendMsg.EndKey = append([]byte{}, p.Region().EndKey...)
	}
	sendMsg.Message = &msg
	return trans.Send(sendMsg)
}

func (p *peer) applyEntries(entries []eraftpb.Entry) {
	for _, ent := range entries {

		if ent.Data == nil {
			continue
		}
		reqs := []raft_cmdpb.Request{}
		dec := gob.NewDecoder(bytes.NewBuffer(ent.Data))
		for {
			var req raft_cmdpb.Request
			if err := dec.Decode(&req); err != nil {
				break
			}
			reqs = append(reqs, req)
		}
		p.processNormalRequest(reqs, ent.Index, ent.Term)
	}
}

func (p *peer) processNormalRequest(reqs []raft_cmdpb.Request, i, t uint64) {
	header := &raft_cmdpb.RaftResponseHeader{
		Error:       nil,
		Uuid:        nil,
		CurrentTerm: p.Term(),
	}

	resp := &raft_cmdpb.RaftCmdResponse{
		Header:    header,
		Responses: make([]*raft_cmdpb.Response, 0),
	}

	kvWB := &engine_util.WriteBatch{}

	for _, req := range reqs {
		switch req.CmdType {
		case raft_cmdpb.CmdType_Get:
			resp.Responses = append(resp.Responses, p.ProcessGetReq(req.Get))
		case raft_cmdpb.CmdType_Delete:
			resp.Responses = append(resp.Responses, p.ProcessDeleteReq(req.Delete, kvWB))
		case raft_cmdpb.CmdType_Put:
			resp.Responses = append(resp.Responses, p.ProcessPutReq(req.Put, kvWB))
		case raft_cmdpb.CmdType_Snap:
			resp.Responses = append(resp.Responses, p.ProcessSnapReq(req.Snap))
		}
	}

	if kvWB.Len() > 0 {
		if err := p.peerStorage.Engines.WriteKV(kvWB); err != nil {
			log.Panicf("write error %v", err)
		}
	}

	// notify server
	p.NotifyServer(resp, i, t)

}

func (p *peer) ProcessSnapReq(req *raft_cmdpb.SnapRequest) *raft_cmdpb.Response {
	// 返回region和txn
	regionLocalState := new(rspb.RegionLocalState)
	if err := engine_util.GetMeta(p.peerStorage.Engines.Kv, meta.RegionStateKey(p.regionId), regionLocalState); err != nil {
		log.Panicf("fail to find regionLocalState")
	}

	snapResp := &raft_cmdpb.SnapResponse{
		Region: regionLocalState.Region,
	}
	return &raft_cmdpb.Response{
		CmdType: raft_cmdpb.CmdType_Snap,
		Snap:    snapResp,
	}

}

func (p *peer) ProcessPutReq(req *raft_cmdpb.PutRequest, kvWB *engine_util.WriteBatch) *raft_cmdpb.Response {
	cf, key, val := req.Cf, req.Key, req.Value
	kvWB.SetCF(cf, key, val)
	return &raft_cmdpb.Response{
		CmdType: raft_cmdpb.CmdType_Put,
	}
}

func (p *peer) ProcessDeleteReq(req *raft_cmdpb.DeleteRequest, kvWB *engine_util.WriteBatch) *raft_cmdpb.Response {
	cf, key := req.Cf, req.Key
	kvWB.DeleteCF(cf, key)
	return &raft_cmdpb.Response{
		CmdType: raft_cmdpb.CmdType_Delete,
	}
}

func (p *peer) ProcessGetReq(req *raft_cmdpb.GetRequest) *raft_cmdpb.Response {
	resp := &raft_cmdpb.Response{
		CmdType: raft_cmdpb.CmdType_Get,
		Get:     &raft_cmdpb.GetResponse{},
	}

	cf, key := req.Cf, req.Key
	val, err := engine_util.GetCF(p.peerStorage.Engines.Kv, cf, key)
	if err != nil {
		resp.Get.Value = nil
	}
	resp.Get.Value = val
	return resp
}

func (p *peer) NotifyServer(resp *raft_cmdpb.RaftCmdResponse, i, t uint64) {
	for len(p.proposals) > 0 {
		prop := p.proposals[0]
		if t < prop.term {
			return
		}

		if t > prop.term {
			prop.cb.Done(ErrRespStaleCommand(prop.term))
			p.proposals = p.proposals[1:]
			continue
		}

		if t == prop.term && i < prop.index {
			return
		}

		if i == prop.index && t == prop.term {

			// if resp.Responses[0].CmdType == raft_cmdpb.CmdType_Snap {
			// 	prop.cb.Txn = p.peerStorage.Engines.Kv.NewTransaction(false)
			// }
			prop.cb.Txn = p.peerStorage.Engines.Kv.NewTransaction(false)
			prop.cb.Done(resp)
			p.proposals = p.proposals[1:]
			return
		}
		log.Panicf("This should not happen.")
	}

	// if !p.IsLeader() {
	// 	return
	// }

	// log.Infof("peer %x notify server index %d, term %d", p.PeerId(), i, t)
	// if len(p.proposals) == 0 {
	// 	log.Panicf("peer %x Fail to find callback for len %d", p.PeerId(), len(p.proposals))
	// }

	// offset := p.proposals[0].index
	// if offset+uint64(len(p.proposals)) <= i {
	// 	log.Panicf("Fail to find callback for %v", p.proposals[0])
	// }

	// prop := p.proposals[i-offset]

	// // maybe no need to check
	// if prop.term != t {
	// 	log.Panicf("Mismatch term when callback , proposal term %d, response term %d", prop.term, t)
	// }

	// // 对snap请求需要携带txn
	// if resp.Responses[0].CmdType == raft_cmdpb.CmdType_Snap {
	// 	prop.cb.Txn = p.peerStorage.Engines.Kv.NewTransaction(false)
	// }

	// prop.cb.Done(resp)
	// // compact
	// log.Infof("peer %x compact propose [%d, %d]", p.PeerId(), p.proposals[0].index, prop.index)
	// p.proposals = append([]*proposal{}, p.proposals[i-offset+1:]...)
}
