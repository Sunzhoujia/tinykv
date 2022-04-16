package raft

import (
	"fmt"
	"strings"
)

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

// 收到appResp的成功应答之后，leader更新节点的索引数据
// 如果传入的n小于等于当前的match索引，则索引就不会更新，返回false；否则更新索引返回true
func (pr *Progress) maybeUpdate(n uint64) bool {
	var updated bool
	if pr.Match < n {
		pr.Match = n
		updated = true
	}
	pr.Next = max(pr.Next, n+1)
	return updated
}

// 判断是否是过期消息，对过期消息直接返回false，不处理
// rejected: MsgApp携带的preLogIndex
// matchHint: follower最后一条日志索引
func (pr *Progress) maybeDecrTo(rejectIndex, matchHint uint64) bool {
	if rejectIndex <= pr.Match {
		return false
	}

	pr.Next = max(min(rejectIndex, matchHint+1), 1)
	return true
}

func (pr *Progress) String() string {
	var buf strings.Builder
	fmt.Fprintf(&buf, " match=%d next=%d", pr.Match, pr.Next)

	return buf.String()
}
