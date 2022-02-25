package main

import (
	"context"
	"fmt"
	"time"

	"github.com/Ishan27g/go-utils/mLogger"
	gossip "github.com/Ishan27g/gossipProtocol"
	"github.com/Ishan27g/syncEngine/data"
	"github.com/Ishan27g/syncEngine/engine"
	"github.com/Ishan27g/syncEngine/peer"
	"github.com/Ishan27g/syncEngine/proto"
	"github.com/Ishan27g/syncEngine/provider"
	"github.com/Ishan27g/syncEngine/snapshot"
	"github.com/Ishan27g/syncEngine/transport"
	"github.com/Ishan27g/syncEngine/utils"
	"github.com/hashicorp/go-hclog"
)

var envFile = ".envFiles/1.leader.env"

type dataManager struct {
	self func() *peer.Peer

	Data   data.Data  // gossip data
	Events data.Event // order of events maintained only by the leader
	Tmp    data.Event // tmp events as fallback if SyncLeader has timed out

	LastOrderHash string
}
type voteManager struct {
	self  func() *peer.Peer
	voted chan peer.Peer
}
type gossipManager struct {
	gsp func() gossip.Gossip
	rcv <-chan gossip.Packet
}
type snapshotManager struct {
	RoundNum          int
	Count             int
	snapshot.SnapShot // interface to persist data to file

	LastSnapshotHash string
}

func (d *dataManager) NewEvent(ctx context.Context, order *proto.Order) (*proto.Ok, error) {
	// data := *d.Data()
	panic("implement me")

}

func (d *dataManager) SaveOrder(ctx context.Context, order *proto.Order) (*proto.Ok, error) {
	panic("implement me")

}

func (d *dataManager) GetSyncData(ctx context.Context, ok *proto.Ok) (*proto.Order, error) {
	//TODO implement me
	panic("implement me")
}

func (d *dataManager) GetPacketAddresses(ctx context.Context, ok *proto.Ok) (*proto.Peers, error) {
	//TODO implement me
	panic("implement me")
}

func (d *dataManager) GetNetworkView(ctx context.Context, ok *proto.Ok) (*proto.Peers, error) {
	//TODO implement me
	panic("implement me")
}

func (g *gossipManager) Gossip(data string) {
	g.gsp().SendGossip(data)
}
func (g *gossipManager) Receive() gossip.Packet {
	return <-g.rcv
}
func (v *voteManager) RequestVotes(ctx context.Context, term *proto.Term) (*proto.Vote, error) {
	vote := &proto.Vote{Elected: false}
	if term.TermCount > int32(v.self().Term) {
		v.voted <- peer.Peer{
			Zone:     int(term.Zone),
			HostName: term.LeaderHostname,
			HttpPort: term.LeaderHttpPort,
			GrpcPort: term.LeaderGrpcPort,
			UdpPort:  term.LeaderUdpPort,
			Mode:     int(term.Mode),
			Term:     int(term.TermCount),
		}
		vote.Elected = true
	}
	return vote, nil
}

func getPacket(dm *dataManager) func(id string) *gossip.Packet {
	return func(id string) *gossip.Packet {
		return dm.Data.GetPacket(id)
	}
}

func getInitialData(eng *engine.Engine, sm *snapshotManager, dm *dataManager) func() transport.SyncRsp {
	return func() transport.SyncRsp {
		entries := snapshot.FromFile(eng.DataFile).Get()
		var ee []snapshot.Entry
		for _, entry := range entries {
			if sm.RoundNum > entry.Round {
				ee = append(ee, entry)
			}
		}
		order := dm.Events.GetOrder()
		return transport.SyncRsp{
			OrderedEvents: order,
			Entries:       entries,
			SyncLeader:    *eng.Self(),
		}
	}
}

func Start(ctx context.Context, envFile string) (*engine.Engine, *snapshotManager, *dataManager, *gossipManager,
	*provider.JaegerProvider) {

	var self peer.Peer
	self, transport.RegistryUrl = peer.FromEnv(envFile)

	eng := engine.Init(self)

	sm := snapshotManager{
		RoundNum:         0,
		Count:            0,
		SnapShot:         snapshot.Empty(eng.DataFile),
		LastSnapshotHash: "",
	}

	vm := voteManager{self: func() *peer.Peer {
		return eng.Self()
	}, voted: make(chan peer.Peer)}

	dm := dataManager{
		self: func() *peer.Peer {
			return eng.Self()
		},
		Data:          data.InitData(),
		Events:        data.InitEvents(),
		Tmp:           data.InitEvents(),
		LastOrderHash: "",
	}

	gm := gossipManager{
		gsp: func() gossip.Gossip {
			return eng.Gossip.Gossip
		},
		rcv: eng.Gossip.GossipRcv,
	}

	httpCbs := append(eng.BuildHttpCbs(), []transport.HTTPCbs{
		transport.WithSyncInitialOrderCb(getInitialData(eng, &sm, &dm)),
		transport.WithSnapshotFile(eng.DataFile),
		transport.WithPacketCb(getPacket(&dm)),
	}...)

	opts := []transport.RpcOption{
		transport.WithPort(self.GrpcPort),
		transport.WithDataServer(&dm),
		transport.WithVotingServer(&vm),
	}

	tracerId := (*eng.Self()).HttpAddr()

	url := "http://localhost:14268/api/traces"
	jp := provider.InitJaeger(ctx, tracerId, (*eng.Self()).HttpPort, url)

	rpcServer := transport.NewRpcServer(opts...)
	httpServer := transport.NewHttpSrv(self.HttpPort, tracerId, httpCbs...)

	rpcServer.Start(ctx)
	httpServer.Start(ctx)
	eng.Start(ctx)

	return eng, &sm, &dm, &gm, jp
}
func main() {
	mLogger.Apply(mLogger.Level(hclog.Trace), mLogger.Color(true))

	utils.MockRegistry()
	ctx, can := context.WithCancel(context.Background())
	defer can()

	eng, _, _, _, jp := Start(ctx, envFile)

	eng2, _, _, _, jp2 := Start(ctx, ".envFiles/1.follower.A.env")

	defer jp.Shutdown(ctx)
	defer jp2.Shutdown(ctx)

	<-time.After(10 * time.Second)
	//utils.PrintJson(eng)
	//utils.PrintJson(dm)
	//utils.PrintJson(sm)
	//utils.PrintJson(eng2)
	//utils.PrintJson(dm2)
	//utils.PrintJson(sm2)

	//fmt.Println(gm)
	//fmt.Println(gm2)

	fmt.Println(eng.State())
	fmt.Println(eng2.State())
	<-time.After(10 * time.Second)

}
