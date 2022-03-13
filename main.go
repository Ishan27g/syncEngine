package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/Ishan27g/go-utils/mLogger"
	gossip "github.com/Ishan27g/gossipProtocol"
	"github.com/Ishan27g/vClock"
	"github.com/hashicorp/go-hclog"

	"github.com/Ishan27g/syncEngine/data"
	"github.com/Ishan27g/syncEngine/engine"
	"github.com/Ishan27g/syncEngine/peer"
	"github.com/Ishan27g/syncEngine/proto"
	"github.com/Ishan27g/syncEngine/provider"
	"github.com/Ishan27g/syncEngine/snapshot"
	"github.com/Ishan27g/syncEngine/transport"
	"github.com/Ishan27g/syncEngine/utils"
)

// var envFile = ".envFiles/1.leader.env"
const RoundResolution = 1 // or the num of messages per round ~ number of events that will be ordered

func getData(eng *engine.Engine, dm *dataManager) func() transport.SyncRsp {
	return func() transport.SyncRsp {
		if dm.sm.RoundNum == 0 {
			return transport.SyncRsp{
				OrderedEvents: nil,
				Entries:       nil,
				SyncLeader:    *&eng.State().SyncLeader,
			}
		}
		entries := snapshot.FromFile(eng.DataFile).Get()
		dm.Warn("Snapshot read from file - " + utils.PrintJson(entries))
		var ee []snapshot.Entry
		for _, entry := range entries {
			if dm.sm.RoundNum > entry.Round {
				ee = append(ee, entry)
			}
		}
		order := dm.Events.GetOrder()
		r := transport.SyncRsp{
			OrderedEvents: order,
			Entries:       entries,
			SyncLeader:    *&eng.State().SyncLeader,
		}
		return r
	}
}

func Start(ctx context.Context, envFile string) (*dataManager, *gossipManager, *provider.TraceProvider) {
	mLogger.Apply(mLogger.Level(hclog.Trace), mLogger.Color(true))

	var self peer.Peer
	self, transport.RegistryUrl = peer.FromEnv(envFile)

	//self.GrpcPort = self.HttpPort

	tracerId := self.HostName + self.HttpPort
	//	tracerId := self.HttpAddr()

	jp := provider.Init("zipkin", tracerId, self.FakeName)
	go func(ctx context.Context, jp provider.TraceProvider) {
		<-ctx.Done()
		jp.Close()
	}(ctx, jp)

	hClient := transport.NewHttpClient(tracerId, jp.Get().Tracer(tracerId))

	var eng *engine.Engine
	var dm dataManager
	var gm gossipManager
	var vm voteManager

	gsp, gspRcv := gossip.Config(self.HostName, self.UdpPort, self.HttpAddr()) // id=availableAt for packet
	gm = newGossipManager(gsp, gspRcv)
	vm = voteManager{self: func() *peer.Peer {
		s := eng.Self()
		return &s
	}, voted: make(chan peer.Peer)}

	var cancelInit context.CancelFunc
	var ctxInit context.Context
	goto init

init:
	{
		ctxInit, cancelInit = context.WithCancel(ctx)
		go func() {
			<-ctx.Done()
			cancelInit()
		}()
		eng = engine.Init(self, &hClient)
		dm = dataManager{
			vm: data.VersionMap(),
			state: func() *peer.State {
				return eng.State()
			},
			sm: &snapshot.Manager{
				RoundNum:         0,
				Count:            0,
				SnapShot:         snapshot.Empty(eng.DataFile),
				LastSnapshotHash: "",
			},
			Data:          data.InitData(),
			Events:        data.InitEvents(),
			Tmp:           data.InitEvents(),
			LastOrderHash: "",
			zonePeers: func() []peer.Peer {
				return eng.GetFollowers(true).([]peer.Peer)
			},
			syncPeers: func() []peer.Peer {
				return eng.GetSyncFollowers()
			},
			Logger:    mLogger.Get("dm" + self.HttpPort),
			nextRound: make(chan int),
		}

		httpCbs := append(eng.BuildHttpCbs(), []transport.HTTPCbs{
			transport.WithRaftFollowerCb(func(p peer.Peer) {
				eng.AddFollower(peer.State{
					Self:       p,
					RaftLeader: peer.Peer{},
					SyncLeader: peer.Peer{},
				})
				gm.gsp.Add(gossip.Peer{
					UdpAddress:        p.UdpAddr(),
					ProcessIdentifier: p.HttpAddr(),
					Hop:               0,
				})
			}),
			transport.WithSyncFollowerCb(func(p peer.Peer) {
				eng.AddSyncFollower(peer.State{
					Self:       p,
					RaftLeader: peer.Peer{},
					SyncLeader: peer.Peer{},
				})
				gm.gsp.Add(gossip.Peer{
					UdpAddress:        p.UdpAddr(),
					ProcessIdentifier: p.HttpAddr(),
					Hop:               0,
				})
			}),
			transport.WithSyncInitialOrderCb(getData(eng, &dm)),
			transport.WithSnapshotFile(eng.DataFile),
			transport.WithPacketCb(getPacket(&dm)),
			transport.WithGossipSend(gm.Gossip),
			transport.WithRoundNumCb(func(roundNum int) {
				go func() {
					goto wait
				wait:
					{
						if !dm.canSnapshot() {
							<-time.After(1 * time.Second)
							goto wait
						}
					}
					dm.saveSnapshot()
					dm.sm.RoundNum++
					dm.sendRoundNum(&hClient)
					dm.sm.Round()
					dm.Events.Reset()
				}()
			}),
			transport.WithFollowerListCb(func() []peer.State {
				return eng.GetFollowers(false).([]peer.State)
			}),
		}...)

		opts := []transport.RpcOption{
			transport.WithPort(self.GrpcPort),
			transport.WithDataServer(&dm),
			transport.WithVotingServer(&vm),
		}

		rpcServer := transport.NewRpcServer(opts...)
		httpServer := transport.NewHttpSrv(self.HttpPort, tracerId, httpCbs...)

		// go transport.Listen(ctx, rpcServer, httpServer)
		rpcServer.Start(ctxInit, nil)
		httpServer.Start(ctxInit, nil)
		eng.Start()

		<-time.After(engine.Hb_Timeout)

		dm.Info("started...", "isZoneLeader", dm.isZoneLeader(), "isSyncLeader", dm.isSyncLeader())
	}
	var p *proto.Peers
	var gossipPeers []gossip.Peer

	// sync Initial order
	var initialEventOrder []vClock.Event // todo unused
	var entries []snapshot.Entry
	ctx1, cancel := context.WithDeadline(context.Background(), time.Now().Add(5*time.Second))
	defer cancel()
	if !dm.isZoneLeader() && !dm.isSyncLeader() { // follower
		if dm.state().RaftLeader.HttpAddr() == "" {
			cancelInit()
			goto init
		}
		hClient.SendSyncRequest(dm.state().RaftLeader.HttpAddr(), &initialEventOrder, &entries, dm.state().Self)
		c := transport.NewDataSyncClient(ctx1, dm.state().RaftLeader.GrpcAddr())
		p, _ = c.GetNetworkView(ctx1, &proto.Ok{})

	} else if dm.isZoneLeader() && !dm.isSyncLeader() { // zoneLeader
		if dm.state().SyncLeader.HttpAddr() == "" {
			cancelInit()
			goto init
		}
		dm.Warn("FUCk", "dm.state().SyncLeader.HttpAddr()", dm.state().SyncLeader.HttpAddr())
		hClient.SendSyncRequest(dm.state().SyncLeader.HttpAddr(), &initialEventOrder, &entries, dm.state().Self)
		c := transport.NewDataSyncClient(ctx1, dm.state().SyncLeader.GrpcAddr())
		p, _ = c.GetNetworkView(ctx1, &proto.Ok{})
	}
	// else { // syncLeader
	// first node in network
	// continue
	//	}
	if p != nil {
		for _, peer := range p.Peers {
			gossipPeers = append(gossipPeers, gossip.Peer{
				UdpAddress:        peer.UdpAddress,
				ProcessIdentifier: peer.PeerId,
				Hop:               0,
			})
		}
	}
	gm.gsp.Join(gossipPeers...)
	if len(entries) > 0 {
		dm.sm.Sync(entries...)
		dm.sm.Round()
		dm.sm.RoundNum = entries[len(entries)-1].Round + 1
	}
	dm.Events.MergeEvents(initialEventOrder...)

	dm.waitOnGossip(ctx, &gm, &hClient)
	dm.waitOnMissingPackets(ctx, &hClient)
	dm.startRoundSync(ctx, &gm, &hClient)

	return &dm, &gm, nil
	//	return &dm, &gm, jp
}

// func main() {

// 	utils.MockRegistry()
// 	ctx, can := context.WithCancel(context.Background())
// 	defer can()

// 	dm1, gm1, jp := Start(ctx, envFile)

// 	dm2, _, jp2 := Start(ctx, ".envFiles/1.follower.A.env")

// 	defer jp.Shutdown(ctx)
// 	defer jp2.Shutdown(ctx)

// 	<-time.After(1 * time.Second)
// 	fmt.Println("SENDING GOSSIP")
// 	go gm1.gsp.SendGossip("nice")
// 	<-time.After(5 * time.Second)
// 	fmt.Println("LastSnapshotHash - ", dm2.sm.LastSnapshotHash)

// 	<-time.After(1 * time.Second)
// 	fmt.Println("SENDING GOSSIP")
// 	go gm1.gsp.SendGossip("nice22222")
// 	<-time.After(5 * time.Second)
// 	//utils.PrintJson(eng)
// 	//utils.PrintJson(dm)
// 	//utils.PrintJson(sm)
// 	//utils.PrintJson(eng2)
// 	//utils.PrintJson(dm2)
// 	//utils.PrintJson(sm2)

// 	//fmt.Println(gm)
// 	//fmt.Println(gm2)

// 	fmt.Println(dm1.state())
// 	fmt.Println(dm2.state())
// 	<-make(chan bool)

// }
func main() {
	if len(os.Args) <= 1 {
		fmt.Println("go run main.go envFile")
		return
	}
	envFile := os.Args[1]
	ctx, can := context.WithCancel(context.Background())
	defer can()

	dm1, gm1, _ := Start(ctx, envFile)

	//defer jp.Shutdown(ctx)

	reader := bufio.NewReader(os.Stdin)
	for {
		text, _ := reader.ReadString('\n')
		text = strings.Replace(text, "\n", "", -1) // convert CRLF to LF
		if strings.Compare("send", text) == 0 {
			fmt.Println("Enter Data -\n\t$ ")
			text, _ := reader.ReadString('\n')
			text = strings.Replace(text, "\n", "", -1) // convert CRLF to LF
			gm1.Gossip(text)
		} else if strings.Compare("state", text) == 0 {
			fmt.Println(utils.PrintJson(dm1.state()))
		} else if strings.Compare("quit", text) == 0 {
			break
		}
	}
	<-make(chan bool)

}
