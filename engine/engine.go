package engine

import (
	"fmt"
	"sync"

	"github.com/Ishan27g/go-utils/mLogger"
	gossip "github.com/Ishan27g/gossipProtocol"
	"github.com/Ishan27g/syncEngine/peer"
	"github.com/Ishan27g/syncEngine/provider"
	"github.com/Ishan27g/syncEngine/transport"
	"github.com/hashicorp/go-hclog"
)

type gossipW struct {
	Gossip    gossip.Gossip        // gossip interface to send data to network
	GossipRcv <-chan gossip.Packet // gossip from peers
}

type Engine struct {
	self     peer.Peer
	DataFile string

	zonePeers          map[string]*peer.Peer
	syncPeers          map[string]*peer.Peer
	zoneRaft           Raft
	hbFromRaftLeader   chan peer.Peer
	votedForRaftLeader chan peer.Peer

	syncRaft           Raft
	hbFromSyncLeader   chan peer.Peer
	votedForSyncLeader chan peer.Peer

	hclog.Logger
	HClient *transport.HttpClient
	jp      *provider.JaegerProvider
}

func (e *Engine) HbFromRaftLeader(from peer.Peer) {
	e.hbFromRaftLeader <- from
	e.self.RaftTerm = from.RaftTerm
}
func (e *Engine) HbFromSyncLeader(from peer.Peer) {
	e.Info("HB from Sync leader ")
	e.hbFromSyncLeader <- from
	e.Info("HB from Sync leader sent to channel")
	e.self.SyncTerm = from.SyncTerm
}
func (e *Engine) State() *peer.State {
	var syncLeader = peer.Peer{}
	if e.syncRaft != nil {
		syncLeader = e.syncRaft.GetLeader()
	}
	return peer.GetState(e.Self(), e.zoneRaft.GetLeader(), syncLeader)
}
func (e *Engine) Self() peer.Peer {
	if e.zoneRaft != nil {
		e.self.RaftTerm = e.zoneRaft.GetLeader().RaftTerm
	}
	if e.syncRaft != nil {
		e.self.SyncTerm = e.syncRaft.GetLeader().RaftTerm
	}
	return e.self
}
func (e *Engine) Start() {
	go func() {
		e.zoneRaft.Start()
	}()
}
func (e *Engine) AddSyncFollower(peer peer.Peer) {
	if e.syncPeers[peer.HttpAddr()] == nil {
		e.syncPeers[peer.HttpAddr()] = &peer
	}
}

func (e *Engine) AddFollower(peer peer.Peer) {
	if e.zonePeers[peer.HttpAddr()] == nil {
		e.zonePeers[peer.HttpAddr()] = &peer
	}
}
func (e *Engine) GetFollowers() []peer.Peer {
	var f []peer.Peer
	for _, v := range e.zonePeers {
		f = append(f, *v)
	}
	return f
}
func (e *Engine) GetSyncFollowers() []peer.Peer {
	var f []peer.Peer
	for _, v := range e.syncPeers {
		f = append(f, *v)
	}
	return f
}
func Init(self peer.Peer, hClient *transport.HttpClient) *Engine {
	dataFile := self.HttpPort + ".csv"
	mLogger.Apply(mLogger.Level(hclog.Trace), mLogger.Color(true))
	e := &Engine{
		self:               self,
		DataFile:           dataFile,
		Logger:             mLogger.Get(self.HttpPort),
		zoneRaft:           nil,
		syncRaft:           nil,
		zonePeers:          make(map[string]*peer.Peer),
		syncPeers:          make(map[string]*peer.Peer),
		HClient:            hClient,
		votedForRaftLeader: make(chan peer.Peer),
		votedForSyncLeader: make(chan peer.Peer),
		hbFromRaftLeader:   make(chan peer.Peer),
		hbFromSyncLeader:   make(chan peer.Peer),
	}

	var raftLeader peer.Peer
	var syncLeader peer.Peer
	peers := transport.Register(self)
	fmt.Println("Peers - ", peers)
	switch len(*peers) {
	case 0:
		e.self.Mode = peer.LEADER
		e.self.RaftTerm = 1
		raftLeader = e.self

		zoneLeaders := transport.DiscoverRaftLeaders(e.self.Zone)
		rsp := e.HClient.FindAndFollowSyncLeader(zoneLeaders, e.self)
		if rsp != nil {
			syncLeader = rsp.SyncLeader
			e.Info("Discovered sync leader")
		} else {
			e.self.SyncTerm = 1
			raftLeader = self
			syncLeader = self
			e.Info("Cannot discover raft & sync leaders, becoming both ")
		}
	default:
		e.self.Mode = peer.FOLLOWER
		rsp := e.HClient.FindAndFollowRaftLeader(peers, e.self)
		if rsp != nil {
			raftLeader = rsp.RaftLeader
			syncLeader = rsp.SyncLeader
			e.Info("Discovered raft & sync leaders - ") // + utils.PrintJson(rsp))
		} else {
			e.Error("Cannot discover raft leader")
		}
	}
	var startSync sync.Once

	// init raft for zone.
	// if syncLeader, send zone hbs & start syncRaft
	// if zoneLeader & not syncLeader, send zone hbs & start syncRaft
	// if follower, monitor zoneLeader hbs. Start syncRaft when elected as leader
	e.zoneRaft = InitRaft(0, e.votedForRaftLeader, e.hbFromRaftLeader, e.self, &raftLeader, func() {
		// only once, start zoneRaft hbs when elected as leader
		startSync.Do(e.startSyncRaft(syncLeader))
		// send hbs to followers
		//peers :=
		e.HClient.SendZoneHeartBeat(e.Self(), e.GetFollowers()...)
		//e.Trace("sent zone Hb to followers-" + fmt.Sprintf("%v", peers))
	})

	return e
}
func (e *Engine) BuildHttpCbs() []transport.HTTPCbs {
	return []transport.HTTPCbs{
		transport.WithStateCb(func() *peer.State {
			return e.State()
		}),
		transport.WithZoneHbCb(func(peer peer.Peer) {
			go e.HbFromRaftLeader(peer)
		}),
		transport.WithSyncHbCb(func(peer peer.Peer) {
			go e.HbFromSyncLeader(peer)
		}),
	}
}

// start sync raft
// if syncLeader, send sync hbs
// if not, monitor syncLeader hbs
func (e *Engine) startSyncRaft(syncLeader peer.Peer) func() {
	return func() {
		//zoneLeaders := transport.DiscoverRaftLeaders(e.self.Zone)
		// rsp := e.HClient.FindAndFollowSyncLeader(zoneLeaders, e.self)
		// if rsp != nil {
		// 	syncLeader = rsp.SyncLeader
		// } else {
		// 	e.Info("Becoming syncLeader for ", "zone", self.Zone)
		// 	syncLeader = e.self
		// }
		e.self.Mode = peer.FOLLOWER // syncMode follower
		e.syncRaft = InitRaft(1, e.votedForSyncLeader, e.hbFromSyncLeader, e.self, &syncLeader, e.syncHbs())
		e.self.Mode = peer.LEADER
		go e.syncRaft.Start()
	}
}

// leader sends sync-hbs to other leaders
func (e *Engine) syncHbs() func() {
	return func() {
		// start syncRaft hbs if syncLeader, or elected as syncLeader
		//leaders := transport.DiscoverRaftLeaders(self.Zone) // todo?
		var httpAddr []string
		for _, peer := range e.syncPeers {
			httpAddr = append(httpAddr, peer.HttpAddr())
		}
		e.HClient.SendSyncLeaderHb(e.Self(), httpAddr...)
		//e.Trace("Sent sync Hb to leaders-" + fmt.Sprintf("%v", httpAddr))
	}
}
