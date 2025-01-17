// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"fmt"
	"sort"

	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("balance-region", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler("balance-region", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newBalanceRegionScheduler(opController), nil
	})
}

const (
	// balanceRegionRetryLimit is the limit to retry schedule for selected store.
	balanceRegionRetryLimit = 10
	balanceRegionName       = "balance-region-scheduler"
)

type balanceRegionScheduler struct {
	*baseScheduler
	name         string
	opController *schedule.OperatorController
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *schedule.OperatorController, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	base := newBaseScheduler(opController)
	s := &balanceRegionScheduler{
		baseScheduler: base,
		opController:  opController,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// BalanceRegionCreateOption is used to create a scheduler with an option.
type BalanceRegionCreateOption func(s *balanceRegionScheduler)

func (s *balanceRegionScheduler) GetName() string {
	if s.name != "" {
		return s.name
	}
	return balanceRegionName
}

func (s *balanceRegionScheduler) GetType() string {
	return "balance-region"
}

func (s *balanceRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	// Your Code Here (3C).
	stores := cluster.GetStores()
	sources := []*core.StoreInfo{}
	for _, store := range stores {
		if store.IsUp() && store.DownTime() < cluster.GetMaxStoreDownTime() {
			sources = append(sources, store)
		}
	}
	if len(sources) < 2 {
		return nil
	}
	sort.Slice(sources, func(i, j int) bool {
		return sources[i].GetRegionSize() < sources[j].GetRegionSize()
	})

	// choose target region to move
	var region *core.RegionInfo
	var srcStore, dstStore *core.StoreInfo
	i := len(sources) - 1
	for ; i > 0; i-- {
		var regions core.RegionsContainer
		cluster.GetPendingRegionsWithLock(sources[i].GetID(), func(rc core.RegionsContainer) { regions = rc })
		region = regions.RandomRegion(nil, nil)
		if region != nil {
			break
		}
		cluster.GetFollowersWithLock(sources[i].GetID(), func(rc core.RegionsContainer) { regions = rc })
		region = regions.RandomRegion(nil, nil)
		if region != nil {
			break
		}
		cluster.GetLeadersWithLock(sources[i].GetID(), func(rc core.RegionsContainer) { regions = rc })
		region = regions.RandomRegion(nil, nil)
		if region != nil {
			break
		}
	}
	if region == nil {
		return nil
	}
	srcStore = sources[i]
	// If region's replicas < maxReplicas, fail to schedule
	storeIDs := region.GetStoreIds()
	if len(storeIDs) < cluster.GetMaxReplicas() {
		return nil
	}
	// dstStore mustn't own target region
	for j := 0; j < i; j++ {
		if _, ok := storeIDs[sources[j].GetID()]; !ok {
			dstStore = sources[j]
			break
		}
	}
	if dstStore == nil {
		return nil
	}
	// the difference of region size >= 2*approximateSize
	if srcStore.GetRegionSize()-dstStore.GetRegionSize() < 2*region.GetApproximateSize() {
		return nil
	}
	newPeer, err := cluster.AllocPeer(dstStore.GetID())
	if err != nil {
		return nil
	}
	desc := fmt.Sprintf("move-from-%d-to-%d", srcStore.GetID(), dstStore.GetID())
	op, err := operator.CreateMovePeerOperator(desc, cluster, region, operator.OpBalance, srcStore.GetID(), dstStore.GetID(), newPeer.GetId())
	if err != nil {
		return nil
	}
	return op
}
