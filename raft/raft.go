package raft

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/Ishan27g/go-utils/mLogger"
	"github.com/Ishan27g/syncEngine/peer"
	"github.com/Ishan27g/syncEngine/proto"
	"github.com/Ishan27g/syncEngine/transport"
	"github.com/hashicorp/go-hclog"
)

const Monitor_Timeout = 5 * time.Second
const Hb_Timeout = 3 * time.Second

type Raft interface {
	Start()
	Details() string
	GetState() string
	GetTerm() int
	GetLeader() peer.Peer
	IsLeader() bool
}
type raft struct {
	hbFromLeader   chan peer.Peer
	votedForLeader chan peer.Peer

	currentLeader     *peer.Peer
	self              peer.Peer
	rmode             int
	sendHbToFollowers func()
	hclog.Logger
}

func (r *raft) Details() string {
	return r.details()
}

func (r *raft) GetState() string {
	return r.self.Mode
}
func (r *raft) getTerm() int {
	if r.rmode == 0 {
		return r.self.RaftTerm
	}
	return r.self.SyncTerm
}
func (r *raft) setTerm(term int) {
	if r.rmode == 0 {
		r.self.RaftTerm = term
	} else {
		r.self.SyncTerm = term
	}
}

func (r *raft) GetTerm() int {
	return r.getTerm()
}

func (r *raft) GetLeader() peer.Peer {
	return *r.currentLeader
}

func (r *raft) IsLeader() bool {
	return r.self == *r.currentLeader
}

// InitRaft starts raft according to self.Mode
// if follower ->  monitors votedForLeader & hbFromLeader channels, starts election on timeout
// if leader -> actionWhenLeader is called repeatedly after timeout
func InitRaft(mode int, votedForLeader chan peer.Peer, hbFromLeader chan peer.Peer, self peer.Peer, leader *peer.Peer, actionWhenLeader func()) Raft {

	r := raft{
		currentLeader:     leader,
		self:              self,
		hbFromLeader:      hbFromLeader,
		votedForLeader:    votedForLeader,
		sendHbToFollowers: actionWhenLeader,
		Logger:            mLogger.Get("Raft" + self.HttpAddr()),
	}

	r.sendHbToFollowers = actionWhenLeader
	return &r
}

func (r *raft) Start() {
	switch r.self.Mode {
	case peer.LEADER:
		r.currentLeader = &r.self
		go r.sendHbs()
	default:
		if r.currentLeader != nil {
			r.follow(*r.currentLeader)
		}
		go r.waitOnHbs()
	}
}
func (r *raft) tryElection() bool {
	r.self.Mode = peer.CANDIDATE
	var termCount = r.getTerm() + 1
	grpcs := possiblePeers(r.self.Zone)
	term := &proto.Term{
		TermCount:      int32(termCount),
		LeaderHttpPort: r.self.HttpPort,
		LeaderGrpcPort: r.self.GrpcPort,
		LeaderHostname: r.self.GrpcPort,
	}
	voted := r.election(grpcs, term)
	fmt.Println(voted)
	if voted {
		r.self.Mode = peer.LEADER
		r.setTerm(termCount)
		r.currentLeader = &r.self
	} else {
		r.self.Mode = peer.FOLLOWER
	}
	return voted
}

func possiblePeers(zone int) []string {
	allLeaders := transport.DiscoverRaftLeaders(zone)
	var grpcs []string
	for _, leaders := range *allLeaders {
		grpcs = append(grpcs, leaders.GrpcAddr())
	}
	return grpcs
}
func (r *raft) election(raftPeers []string, term *proto.Term) bool {

	r.Warn("Requesting votes from peers  for term-" + strconv.Itoa(int(term.TermCount)))
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	votes := 0
	for _, peer := range raftPeers {
		p := transport.NewVotingClient(peer)
		if p == nil {
			continue
		}
		voted, err := p.RequestVotes(ctx, term)
		if voted.Elected && err == nil {
			votes++
		}
		p.Disconnect()
	}
	r.Warn("VOTED? " + fmt.Sprintf("%v,%v", votes, raftPeers))

	return votes == len(raftPeers)
}

// receive heartbeats or voted notification from http/rpc
// start election after failing to receive any
func (r *raft) waitOnHbs() {
	for {
		r.Info("waiting on hbs")
		try := 2
		goto wait
	wait:
		{
			<-time.After(Monitor_Timeout)
			select {
			case l, ok := <-r.hbFromLeader:
				if ok {
					r.follow(l)
					continue
				}
			case l, ok := <-r.votedForLeader:
				r.Trace("voted For Leader")
				if ok {
					r.follow(l)
					continue
				}
			default:
				try--
				if try != 0 {
					goto wait
				}
				if r.tryElection() {
					r.sendHbs()
					return
				} else {
					r.Warn("NOT ELECTED")
				}
			}
		}

	}
}

func (r *raft) follow(l peer.Peer) {
	r.Info("Following " + fmt.Sprintf("%v", l))
	r.currentLeader = &l
	if r.rmode == 0 {
		r.setTerm(l.RaftTerm)
	} else {
		r.setTerm(l.SyncTerm)
	}
	r.self.Mode = peer.FOLLOWER
	r.Info("Followed" + r.details() + "\n")
}
func (r *raft) details() string {
	dt := fmt.Sprintf("[SELF %s]\n[LEADER %s]", r.self.Details(), r.currentLeader.Details())
	return dt
}

// send heartbeats to followers
func (r *raft) sendHbs() {
	r.currentLeader = &r.self
	for {
		<-time.After(Hb_Timeout)
		if r.self != *r.currentLeader {
			r.Error(fmt.Sprintf("r.self != r.currentLeader - %v %v", r.self, r.currentLeader))
			panic("invalid state")
		}
		r.sendHbToFollowers()
	}
}
