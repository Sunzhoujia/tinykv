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

// leader update follower's nextIndex and matchIndex
func (pr *Progress) maybeUpdate(n uint64) bool {
	var updated bool
	if pr.Match < n {
		pr.Match = n
		updated = true
	}
	pr.Next = max(pr.Next, n+1)
	return updated
}

// 没有实现按term的快速回退，etcd的实现中说明这是没有必要的
// rejected: MsgApp携带的preLogIndex
// matchHint: follower最后一条日志索引
func (pr *Progress) maybeDecrTo(rejectIndex, matchHint uint64) bool {
	// 过期消息，返回false，不处理
	if rejectIndex <= pr.Match {
		return false
	}
	// 1. next = next-1
	// 2. next = matchHint+1
	pr.Next = max(min(rejectIndex, matchHint+1), 1)
	return true
}

func (pr *Progress) String() string {
	var buf strings.Builder
	fmt.Fprintf(&buf, " match=%d next=%d", pr.Match, pr.Next)

	return buf.String()
}
