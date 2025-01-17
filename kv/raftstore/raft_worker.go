package raftstore

import (
	"sync"

	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
)

// raftWorker is responsible for run raft commands and apply raft logs.
// 1. 监听raftCh信道，接收raftCmd和raftMsg，然后进行封装，路由到对应peer的raft状态机
// 2. 将收到的Msg封装成entry，走完raft共识后，获取raft的ready，进行entries持久化，apply log，发送Msg等操作
type raftWorker struct {
	pr *router

	// receiver of messages should sent to raft, including:
	// * raft command from `raftStorage`
	// * raft inner messages from other peers sent by network
	raftCh chan message.Msg
	ctx    *GlobalContext

	closeCh <-chan struct{}
}

func newRaftWorker(ctx *GlobalContext, pm *router) *raftWorker {
	return &raftWorker{
		raftCh: pm.peerSender,
		ctx:    ctx,
		pr:     pm,
	}
}

// run runs raft commands.
// On each loop, raft commands are batched by channel buffer.
// After commands are handled, we collect apply messages by peers, make a applyBatch, send it to apply channel.
//从raftCh里拿Msg，然后调用HandleMsg()处理，并且是一次拿一批Msg
func (rw *raftWorker) run(closeCh <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	var msgs []message.Msg
	for {
		msgs = msgs[:0]
		select {
		case <-closeCh:
			return
		case msg := <-rw.raftCh:
			msgs = append(msgs, msg)
		}
		pending := len(rw.raftCh)
		for i := 0; i < pending; i++ {
			msgs = append(msgs, <-rw.raftCh)
		}
		peerStateMap := make(map[uint64]*peerState)
		for _, msg := range msgs {
			// 将需要处理的Msg的peer放进Map，后续处理完后遍历Map，对每个peer的raft进行状态更新。
			peerState := rw.getPeerState(peerStateMap, msg.RegionID)
			if peerState == nil {
				continue
			}
			// 用peer包装了一个临时的handler，去处理这条Msg
			newPeerMsgHandler(peerState.peer, rw.ctx).HandleMsg(msg)
		}
		//log.Infof("store %d start handle ready", rw.ctx.store.Id)
		for _, peerState := range peerStateMap {
			newPeerMsgHandler(peerState.peer, rw.ctx).HandleRaftReady()
		}
	}
}

// 去router的map里拿对应region的peer
func (rw *raftWorker) getPeerState(peersMap map[uint64]*peerState, regionID uint64) *peerState {
	peer, ok := peersMap[regionID]
	if !ok {
		peer = rw.pr.get(regionID)
		if peer == nil {
			return nil
		}
		peersMap[regionID] = peer
	}
	return peer
}
