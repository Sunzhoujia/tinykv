package raftstore

import (
	"bytes"
	"sync"
	"time"

	"github.com/Connor1996/badger"
	"github.com/Connor1996/badger/y"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/scheduler_client"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/kv/util/worker"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/btree"
	"github.com/pingcap/errors"
)

var _ btree.Item = &regionItem{}

type regionItem struct {
	region *metapb.Region
}

// Less returns true if the region start key is less than the other.
func (r *regionItem) Less(other btree.Item) bool {
	left := r.region.GetStartKey()
	right := other.(*regionItem).region.GetStartKey()
	return bytes.Compare(left, right) < 0
}

// key --> regionID --> region
type storeMeta struct {
	sync.RWMutex
	/// region end key -> region ID
	regionRanges *btree.BTree
	/// region_id -> region
	regions map[uint64]*metapb.Region
	/// `MsgRequestVote` messages from newly split Regions shouldn't be dropped if there is no
	/// such Region in this store now. So the messages are recorded temporarily and will be handled later.
	pendingVotes []*rspb.RaftMessage
}

func newStoreMeta() *storeMeta {
	return &storeMeta{
		regionRanges: btree.New(2),
		regions:      map[uint64]*metapb.Region{},
	}
}

func (m *storeMeta) setRegion(region *metapb.Region, peer *peer) {
	m.regions[region.Id] = region
	peer.SetRegion(region)
}

// getOverlaps gets the regions which are overlapped with the specified region range.
func (m *storeMeta) getOverlapRegions(region *metapb.Region) []*metapb.Region {
	item := &regionItem{region: region}
	var result *regionItem
	// find is a helper function to find an item that contains the regions start key.
	m.regionRanges.DescendLessOrEqual(item, func(i btree.Item) bool {
		result = i.(*regionItem)
		return false
	})

	if result == nil || engine_util.ExceedEndKey(region.GetStartKey(), result.region.GetEndKey()) {
		result = item
	}

	var overlaps []*metapb.Region
	m.regionRanges.AscendGreaterOrEqual(result, func(i btree.Item) bool {
		over := i.(*regionItem)
		if engine_util.ExceedEndKey(over.region.GetStartKey(), region.GetEndKey()) {
			return false
		}
		overlaps = append(overlaps, over.region)
		return true
	})
	return overlaps
}

type GlobalContext struct {
	cfg                  *config.Config
	engine               *engine_util.Engines
	store                *metapb.Store
	storeMeta            *storeMeta
	snapMgr              *snap.SnapManager
	router               *router
	trans                Transport
	schedulerTaskSender  chan<- worker.Task
	regionTaskSender     chan<- worker.Task
	raftLogGCTaskSender  chan<- worker.Task
	splitCheckTaskSender chan<- worker.Task
	schedulerClient      scheduler_client.Client
	tickDriverSender     chan uint64
}

type Transport interface {
	Send(msg *rspb.RaftMessage) error
}

/// loadPeers loads peers in this store. It scans the db engine, loads all regions and their peers from it
/// WARN: This store should not be used before initialized.
// 加载KVDB中所有的RegionLocalState，根据存储的region信息构建peers。
// 每个region里面存了这个region对应的所有peers(peerID, storeID)
// 如果读出来的RegionLocalState的peer状态是tombStone，则不创建peer
func (bs *Raftstore) loadPeers() ([]*peer, error) {
	// Scan region meta to get saved regions.
	startKey := meta.RegionMetaMinKey
	endKey := meta.RegionMetaMaxKey
	ctx := bs.ctx
	kvEngine := ctx.engine.Kv
	storeID := ctx.store.Id

	var totalCount, tombStoneCount int
	var regionPeers []*peer

	t := time.Now()
	kvWB := new(engine_util.WriteBatch)
	raftWB := new(engine_util.WriteBatch)
	// 去kv engine遍历RegionLocalState（region信息和peer state），key format：0x01 0x03 region_id 0x01
	err := kvEngine.View(func(txn *badger.Txn) error {
		// get all regions from RegionLocalState
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(startKey); it.Valid(); it.Next() {
			item := it.Item()
			if bytes.Compare(item.Key(), endKey) >= 0 {
				break
			}
			regionID, suffix, err := meta.DecodeRegionMetaKey(item.Key())
			if err != nil {
				return err
			}
			if suffix != meta.RegionStateSuffix {
				continue
			}
			val, err := item.Value()
			if err != nil {
				return errors.WithStack(err)
			}
			totalCount++
			localState := new(rspb.RegionLocalState)
			err = localState.Unmarshal(val)
			if err != nil {
				return errors.WithStack(err)
			}
			region := localState.Region
			if localState.State == rspb.PeerState_Tombstone {
				tombStoneCount++
				bs.clearStaleMeta(kvWB, raftWB, localState)
				continue
			}
			// 新建peer
			peer, err := createPeer(storeID, ctx.cfg, ctx.regionTaskSender, ctx.engine, region)
			if err != nil {
				return err
			}
			ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: region})
			ctx.storeMeta.regions[regionID] = region
			// No need to check duplicated here, because we use region id as the key
			// in DB.
			regionPeers = append(regionPeers, peer)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	// 清理Tombstone状态的peer的数据
	kvWB.MustWriteToDB(ctx.engine.Kv)
	raftWB.MustWriteToDB(ctx.engine.Raft)

	log.Infof("start store %d, region_count %d, tombstone_count %d, takes %v",
		storeID, totalCount, tombStoneCount, time.Since(t))
	return regionPeers, nil
}

func (bs *Raftstore) clearStaleMeta(kvWB, raftWB *engine_util.WriteBatch, originState *rspb.RegionLocalState) {
	region := originState.Region
	raftState, err := meta.GetRaftLocalState(bs.ctx.engine.Raft, region.Id)
	if err != nil {
		// it has been cleaned up.
		return
	}
	err = ClearMeta(bs.ctx.engine, kvWB, raftWB, region.Id, raftState.LastIndex)
	if err != nil {
		panic(err)
	}
	if err := kvWB.SetMeta(meta.RegionStateKey(region.Id), originState); err != nil {
		panic(err)
	}
}

type workers struct {
	raftLogGCWorker  *worker.Worker
	schedulerWorker  *worker.Worker
	splitCheckWorker *worker.Worker
	regionWorker     *worker.Worker
	wg               *sync.WaitGroup
}

type Raftstore struct {
	ctx        *GlobalContext
	storeState *storeState
	router     *router
	workers    *workers
	tickDriver *tickDriver
	closeCh    chan struct{}
	wg         *sync.WaitGroup
}

// 为store上的所有region都创建一个peer，然后在router里注册这些不同region的peers。最后为每个peer都启动一堆worker线程，重点关注raftworker
func (bs *Raftstore) start(
	meta *metapb.Store,
	cfg *config.Config,
	engines *engine_util.Engines,
	trans Transport,
	schedulerClient scheduler_client.Client,
	snapMgr *snap.SnapManager) error {
	y.Assert(bs.workers == nil)
	// TODO: we can get cluster meta regularly too later.
	if err := cfg.Validate(); err != nil {
		return err
	}
	err := snapMgr.Init()
	if err != nil {
		return err
	}
	wg := new(sync.WaitGroup)
	bs.workers = &workers{
		splitCheckWorker: worker.NewWorker("split-check", wg),
		regionWorker:     worker.NewWorker("snapshot-worker", wg),
		raftLogGCWorker:  worker.NewWorker("raft-gc-worker", wg),
		schedulerWorker:  worker.NewWorker("scheduler-worker", wg),
		wg:               wg,
	}
	bs.ctx = &GlobalContext{
		cfg:                  cfg,
		engine:               engines,
		store:                meta,
		storeMeta:            newStoreMeta(),
		snapMgr:              snapMgr,
		router:               bs.router,
		trans:                trans,
		schedulerTaskSender:  bs.workers.schedulerWorker.Sender(),
		regionTaskSender:     bs.workers.regionWorker.Sender(),
		splitCheckTaskSender: bs.workers.splitCheckWorker.Sender(),
		raftLogGCTaskSender:  bs.workers.raftLogGCWorker.Sender(),
		schedulerClient:      schedulerClient,
		tickDriverSender:     bs.tickDriver.newRegionCh,
	}
	// 这里的region peer是这个store上的所有region的peer（一个region对应一个peer）
	regionPeers, err := bs.loadPeers()
	log.Infof("all load peers %v", regionPeers)
	if err != nil {
		return err
	}

	// 在router上注册peer
	for _, peer := range regionPeers {
		bs.router.register(peer)
	}
	// 开启工作线程，然后把tickDriver的线程开起来，这个线程主要做两件事：
	// 1. 驱动store_worker，store_worker定期给schedule client发heartbeat的Task，让schedule client和schedule通信
	// 2. 驱动store中各个region的peer的 raft 状态机
	// 至此，集群运行起来了。
	bs.startWorkers(regionPeers)
	return nil
}

func (bs *Raftstore) startWorkers(peers []*peer) {
	ctx := bs.ctx
	workers := bs.workers
	router := bs.router
	bs.wg.Add(2)
	// raftWorker, storeWorker
	rw := newRaftWorker(ctx, router)
	go rw.run(bs.closeCh, bs.wg)

	// storeWorker的工作：持有storeSender，接收与store相关的信息。定期向调度器发心跳，会报store的消息，自身状态
	sw := newStoreWorker(ctx, bs.storeState)
	go sw.run(bs.closeCh, bs.wg)

	// 分别给raftWorker和storeWorker发送Msg，启动Tick（设置下次超时时间）
	// 因为tickDriver启动后，定时会给这两个worker发TickMsg，然后worker处理TickMsg时就是检查各自Tick上注册的schedule事件是否有超时的
	router.sendStore(message.Msg{Type: message.MsgTypeStoreStart, Data: ctx.store})
	for i := 0; i < len(peers); i++ {
		regionID := peers[i].regionId
		_ = router.send(regionID, message.Msg{RegionID: regionID, Type: message.MsgTypeStart})
	}

	engines := ctx.engine
	cfg := ctx.cfg
	workers.splitCheckWorker.Start(runner.NewSplitCheckHandler(engines.Kv, NewRaftstoreRouter(router), cfg))
	workers.regionWorker.Start(runner.NewRegionTaskHandler(engines, ctx.snapMgr))
	workers.raftLogGCWorker.Start(runner.NewRaftLogGCTaskHandler())
	workers.schedulerWorker.Start(runner.NewSchedulerTaskHandler(ctx.store.Id, ctx.schedulerClient, NewRaftstoreRouter(router)))
	// 开启tickDriver，开始计时
	go bs.tickDriver.run()
}

func (bs *Raftstore) shutDown() {
	close(bs.closeCh) // 关闭raftWorker和storeWorker
	bs.wg.Wait()
	bs.tickDriver.stop()
	if bs.workers == nil {
		return
	}
	workers := bs.workers
	bs.workers = nil
	workers.splitCheckWorker.Stop()
	workers.regionWorker.Stop()
	workers.raftLogGCWorker.Stop()
	workers.schedulerWorker.Stop()
	workers.wg.Wait()
	log.Infof("store %x stop all workers", bs.storeState.id)
}

// 创建router，raftstore，tickdriver
func CreateRaftstore(cfg *config.Config) (*RaftstoreRouter, *Raftstore) {
	storeSender, storeState := newStoreState(cfg) // chan message.Msg, 发送端给storeSender，接收端给 storeState
	router := newRouter(storeSender)              // router包括两个ch，peerSender 和 storeSender
	raftstore := &Raftstore{
		router:     router,
		storeState: storeState,
		tickDriver: newTickDriver(cfg.RaftBaseTickInterval, router, storeState.ticker),
		closeCh:    make(chan struct{}),
		wg:         new(sync.WaitGroup),
	}
	return NewRaftstoreRouter(router), raftstore
}
