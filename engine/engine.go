package engine

import (
	"context"
	"fmt"
	"sync"

	"github.com/Ishan27g/go-utils/mLogger"
	gossip "github.com/Ishan27g/gossipProtocol"
	"github.com/Ishan27g/syncEngine/peer"
	"github.com/Ishan27g/syncEngine/provider"
	"github.com/Ishan27g/syncEngine/raft"
	"github.com/Ishan27g/syncEngine/transport"
	"github.com/Ishan27g/syncEngine/utils"
	"github.com/hashicorp/go-hclog"
)

type gossipW struct {
	Gossip    gossip.Gossip        // gossip interface to send data to network
	GossipRcv <-chan gossip.Packet // gossip from peers
}

type Engine struct {
	self     *peer.Peer
	DataFile string

	Gossip *gossipW

	zonePeers          []peer.Peer
	syncPeers          []peer.Peer
	zoneRaft           raft.Raft
	hbFromRaftLeader   chan peer.Peer
	votedForRaftLeader chan peer.Peer

	syncRaft           raft.Raft
	hbFromSyncLeader   chan peer.Peer
	votedForSyncLeader chan peer.Peer

	hclog.Logger
	HClient transport.HttpClient
	jp      *provider.JaegerProvider
}

func (e *Engine) HbFromRaftLeader(from peer.Peer) {
	e.hbFromRaftLeader <- from
	e.self.RaftTerm = from.RaftTerm
}
func (e *Engine) HbFromSyncLeader(from peer.Peer) {
	e.hbFromSyncLeader <- from
	e.self.SyncTerm = from.SyncTerm
}
func (e *Engine) State() *peer.State {
	var syncLeader = peer.Peer{}
	if e.syncRaft != nil {
		syncLeader = e.syncRaft.GetLeader()
		l := e.syncRaft.GetLeader()
		if e.self.HttpAddr() == l.HttpAddr() {

		}

	}
	return peer.GetState(*e.Self(), e.zoneRaft.GetLeader(), syncLeader)
}
func (e *Engine) Self() *peer.Peer {
	if e.zoneRaft != nil {
		e.self.RaftTerm = e.zoneRaft.GetLeader().RaftTerm
	}
	if e.syncRaft != nil {
		e.self.SyncTerm = e.syncRaft.GetLeader().RaftTerm
	}
	return e.self
}
func (e *Engine) Start(ctx context.Context) {
	go func() {
		e.zoneRaft.Start()
		<-ctx.Done()
		e.jp.Close()
	}()
}
func (e *Engine) AddSyncFollower(peer peer.Peer) {
	e.syncPeers = append(e.syncPeers, peer)
	e.addGossipPeer(peer)
}
func (e *Engine) addGossipPeer(peer peer.Peer) {
	e.Gossip.Gossip.Add(gossip.Peer{
		UdpAddress:        peer.UdpAddr(),
		ProcessIdentifier: peer.HttpAddr(),
		Hop:               0,
	})
}
func (e *Engine) AddFollower(peer peer.Peer) {
	e.zonePeers = append(e.zonePeers, peer)
	e.addGossipPeer(peer)
}
func (e *Engine) GetFollowers() []peer.Peer {
	return e.zonePeers
}
func (e *Engine) GetSyncFollowers() []peer.Peer {
	return e.syncPeers
}
func Init(self peer.Peer) *Engine {
	dataFile := self.HttpPort + ".csv"
	mLogger.Apply(mLogger.Level(hclog.Trace), mLogger.Color(true))

	gsp, gspRcv := gossip.Config(self.HostName, self.UdpPort, self.HttpAddr()) // id=availableAt for packet

	g := gossipW{
		Gossip:    gsp,
		GossipRcv: gspRcv,
	}

	e := &Engine{
		self:               &self,
		DataFile:           dataFile,
		Gossip:             &g,
		Logger:             mLogger.Get(self.HttpPort),
		zoneRaft:           nil,
		syncRaft:           nil,
		votedForRaftLeader: make(chan peer.Peer),
		votedForSyncLeader: make(chan peer.Peer),
		hbFromRaftLeader:   make(chan peer.Peer),
		hbFromSyncLeader:   make(chan peer.Peer),
	}
	tracerId := (*e.Self()).HttpAddr()

	url := "http://localhost:14268/api/traces"

	jp := provider.InitJaeger(context.Background(), tracerId, (*e.Self()).HttpPort, url)
	e.HClient = transport.NewHttpClient((*e.Self()).HttpPort, jp.Get().Tracer(tracerId))

	var raftLeader peer.Peer
	var syncLeader peer.Peer
	peers := transport.Register(self)
	fmt.Println("Peers - ", peers)
	switch len(*peers) {
	case 0:
		e.self.Mode = peer.LEADER
		e.self.RaftTerm = 1
		raftLeader = *e.self

		zoneLeaders := transport.DiscoverRaftLeaders(e.self.Zone)
		rsp := e.HClient.FindAndFollowSyncLeader(zoneLeaders, *e.self)
		if rsp != nil {
			syncLeader = rsp.SyncLeader
			e.Info("Discovered sync leader - " + utils.PrintJson(rsp))
		} else {
			e.self.SyncTerm = 1
			raftLeader = self
			syncLeader = self
			e.Info("Cannot discover raft & sync leaders, becoming both ")
		}
	default:
		e.self.Mode = peer.FOLLOWER
		rsp := e.HClient.FindAndFollowRaftLeader(peers, *e.self)
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
	// if zoneLeader, send zone hbs & start syncRaft
	// if follower, monitor zoneLeader hbs. Start syncRaft when elected as leader
	e.zoneRaft = raft.InitRaft(0, e.votedForRaftLeader, e.hbFromRaftLeader, *e.self, &raftLeader, func() {
		// only once, start zoneRaft hbs when elected as leader
		startSync.Do(e.startSyncRaft(syncLeader, self))
		// send hbs to followers
		peers := e.HClient.SendZoneHeartBeat(self, e.zonePeers...)
		e.Info("sending zone Hb to followers-" + fmt.Sprintf("%v", peers))
	})

	return e
}
func (e *Engine) BuildHttpCbs() []transport.HTTPCbs {
	return []transport.HTTPCbs{
		transport.WithStateCb(func() *peer.State {
			return e.State()
		}),
		transport.WithRaftFollowerCb(func(peer peer.Peer) {
			e.AddFollower(peer)
		}),
		transport.WithSyncFollowerCb(func(peer peer.Peer) {
			e.AddSyncFollower(peer)
		}),
		transport.WithZoneHbCb(func(peer peer.Peer) {
			e.HbFromRaftLeader(peer)
		}),
		transport.WithSyncHbCb(func(peer peer.Peer) {
			e.HbFromSyncLeader(peer)
		}),
	}
}

// start sync raft
// if syncLeader, send sync hbs
// if not, monitor syncLeader hbs
func (e *Engine) startSyncRaft(syncLeader peer.Peer, self peer.Peer) func() {
	return func() {
		zoneLeaders := transport.DiscoverRaftLeaders(e.self.Zone)
		rsp := e.HClient.FindAndFollowSyncLeader(zoneLeaders, *e.self)
		fmt.Println(rsp)
		if rsp != nil {
			syncLeader = rsp.SyncLeader
		} else {
			e.Info("Becoming syncLeader for ", "zone", self.Zone)
			syncLeader = *e.self
		}
		e.syncRaft = raft.InitRaft(1, e.votedForSyncLeader, e.hbFromSyncLeader, *e.self, &syncLeader, e.syncHbs(self))
		e.syncRaft.Start()
	}
}

// leader sends sync-hbs to other leaders
func (e *Engine) syncHbs(self peer.Peer) func() {
	return func() {
		// start syncRaft hbs if syncLeader, or elected as syncLeader
		leaders := transport.DiscoverRaftLeaders(self.Zone)
		var httpAddr []string
		for _, peer := range *leaders {
			httpAddr = append(httpAddr, peer.HttpAddr())
		}
		e.HClient.SendSyncLeaderHb(self, httpAddr...)
		e.Info("sending sync Hb to leaders-" + fmt.Sprintf("%v", httpAddr))
	}
}
