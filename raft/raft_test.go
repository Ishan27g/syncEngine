package raft

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/Ishan27g/syncEngine/peer"
)

var envDir = func() string {
	c, _ := os.Getwd()
	if strings.Contains(c, "/home/runner") { // github ci
		return c
	}
	return filepath.Join(c, "../.envfiles/")
}
var fanout = func(val peer.Peer, to ...chan peer.Peer) {
	for _, t := range to {
		go func(t chan peer.Peer) {
			t <- val
			fmt.Println("sent to follower")
		}(t)
	}
}
var mockHbAtFollower = func(count int) []chan peer.Peer {
	var followerHbChan []chan peer.Peer
	for i := 0; i < count; i++ {
		followerHbChan = append(followerHbChan, make(chan peer.Peer))
	}
	return followerHbChan
}

func mockHbToFollower(self peer.Peer, to ...chan peer.Peer) func() {
	var sent = 0

	mockHbsFromLeader := func() {
		if sent == 0 {
			go fanout(self, to...)
		}
		sent++
	}
	return mockHbsFromLeader
}

func runLeader(self peer.Peer, mockHbsFromLeader func()) Raft {
	lraft := InitRaft(0, nil, nil, self, &self, mockHbsFromLeader)
	lraft.Start()
	return lraft
}
func runFollowers(leader peer.Peer, followers []peer.Peer, fHbFromLeader ...chan peer.Peer) []Raft {
	var f []Raft
	for i, follower := range followers {
		fBvoted := make(chan peer.Peer)
		fAraft := InitRaft(0, fBvoted, fHbFromLeader[i], follower, &leader, nil)
		f = append(f, fAraft)
		fAraft.Start()
	}
	return f
}

func generate(leaderEnvFile string, followerEnvFile ...string) (leaderEnv peer.Peer, followerState []peer.Peer) {
	l := peer.InitEnv(envDir() + leaderEnvFile)
	l.Self.RaftTerm = 1
	var fs []peer.Peer
	for _, s := range followerEnvFile {
		fEnv := peer.InitEnv(envDir() + s)
		fs = append(fs, fEnv.Self)
	}
	return l.Self, fs
}
