package main

import (
	"bufio"
	"context"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/Ishan27g/go-utils/mLogger"
	gossip "github.com/Ishan27g/gossipProtocol"
	"github.com/Ishan27g/syncEngine/data"
	"github.com/Ishan27g/syncEngine/engine"
	"github.com/Ishan27g/syncEngine/peer"
	"github.com/Ishan27g/syncEngine/proto"
	"github.com/Ishan27g/syncEngine/provider"
	"github.com/Ishan27g/syncEngine/raft"
	"github.com/Ishan27g/syncEngine/snapshot"
	"github.com/Ishan27g/syncEngine/transport"
	"github.com/Ishan27g/syncEngine/utils"
	"github.com/Ishan27g/vClock"
	"github.com/hashicorp/go-hclog"
)

// var envFile = ".envFiles/1.leader.env"

type dataManager struct {
	vm data.VersionAbleI

	state  func() *peer.State
	sm     *snapshotManager
	Data   data.Data  // gossip data
	Events data.Event // order of events maintained only by the leader
	Tmp    data.Event // tmp events as fallback if SyncLeader has timed out

	LastOrderHash string
	round         int

	zonePeers func() []peer.Peer
	syncPeers func() []peer.Peer
	hclog.Logger
}
type voteManager struct {
	self  func() *peer.Peer
	voted chan peer.Peer
}
type gossipManager struct {
	gsp gossip.Gossip
	rcv <-chan gossip.Packet
}
type snapshotManager struct {
	RoundNum          int
	Count             int
	snapshot.SnapShot // interface to persist data to file

	LastSnapshotHash string
}

func (d *dataManager) isZoneLeader() bool {
	return d.state().RaftLeader.HttpAddr() == d.state().Self.HttpAddr()
}
func (d *dataManager) isSyncLeader() bool {
	// d.Info("SyncLeader", "HttpAddr", d.state().SyncLeader.HttpAddr())
	// d.Info("Self", "HttpAddr", d.state().Self.HttpAddr())
	return d.state().SyncLeader.HttpAddr() == d.state().Self.HttpAddr()
}
func (d *dataManager) NewEvent(ctx context.Context, order *proto.Order) (*proto.Ok, error) {
	rsp := &proto.Ok{}
	if !d.isZoneLeader() {
		return rsp, nil
	}
	var localEvents = new(data.Event)
	if d.isSyncLeader() {
		localEvents = &d.Events
	} else {
		localEvents = &d.Tmp // tmp events
	}
	events := utils.OrderToEvents(order)
	for _, event := range events {
		if d.vm.GetVersion(event.EventId) == -1 {
			localEvents.MergeEvent(event.EventId, event.EventClock)
		}
		d.vm.UpdateVersion(event.EventId)
	}
	return rsp, nil
}
func (d *dataManager) saveSnapshot() {
	o := d.Data.GetOrderedPackets()
	if len(o) == 0 {
		return
	}
	entries := utils.OrderToEntries(o...)
	// d.Info("Saving " + utils.PrintJson(entries))
	if entries == nil || len(entries) == 0 {
		d.Info("Nothing to save in snapshot")
		return
	}
	currentHash := utils.DefaultHash(entries)
	if d.sm.LastSnapshotHash != currentHash {
		d.sm.Apply(entries...)
		fmt.Println(d.sm.Get())
		d.sm.Save()
		d.Info("Saved snapshot")
	}
	d.sm.LastSnapshotHash = currentHash
}
func (d *dataManager) sendOrderToFollowers(order *proto.Order) {
	if !d.isZoneLeader() {
		return
	}
	ctx, can := context.WithDeadline(context.Background(), time.Now().Add(10*time.Second))
	defer can()
	for _, f := range d.zonePeers() {
		c := transport.NewDataSyncClient(f.UdpAddr())
		c.SaveOrder(ctx, order)
		c.Disconnect()
	}
}
func (d *dataManager) SaveOrder(ctx context.Context, order *proto.Order) (*proto.Ok, error) {
	e := utils.OrderToEvents(order)
	//	d.Info("Saving order " + utils.PrintJson(e))
	d.Events.MergeEvents(e...)
	o := d.Events.GetOrderedIds()
	//	d.Info("App order " + utils.PrintJson(e))
	d.Data.ApplyOrder(o)
	orderHash := utils.DefaultHash(o)

	d.saveSnapshot()

	if d.isZoneLeader() && !d.isSyncLeader() {
		if d.LastOrderHash != orderHash {
			d.sendOrderToFollowers(order)
			d.Tmp.Reset()
			d.LastOrderHash = orderHash
		}
	}

	return &proto.Ok{}, nil
}

func (d *dataManager) GetSyncData(ctx context.Context, ok *proto.Ok) (*proto.Order, error) {
	o := d.Events.GetOrder()
	return utils.EventsToOrder(o), nil
}

func (d *dataManager) GetPacketAddresses(ctx context.Context, ok *proto.Ok) (*proto.Peers, error) {
	atPeers := d.Data.GetPacketAvailableAt(ok.Id)
	var peers []*proto.Peer
	for _, p := range atPeers {
		peers = append(peers, &proto.Peer{
			UdpAddress: "",
			PeerId:     p,
		})
	}
	return &proto.Peers{Peers: peers}, nil
}

func (d *dataManager) GetNetworkView(ctx context.Context, ok *proto.Ok) (*proto.Peers, error) {
	if !d.isZoneLeader() || !d.isSyncLeader() {
		return &proto.Peers{Peers: []*proto.Peer{}}, nil
	}
	gossipPeers := transport.RandomGossipPeers("")
	var peers []*proto.Peer
	for _, peer := range gossipPeers {
		peers = append(peers, &proto.Peer{
			UdpAddress: peer.UdpAddress,
			PeerId:     peer.ProcessIdentifier,
		})
	}
	return &proto.Peers{Peers: peers}, nil
}

func (g *gossipManager) Gossip(data string) {
	g.gsp.SendGossip(data)
}
func (g *gossipManager) Receive() gossip.Packet {
	return <-g.rcv
}
func (v *voteManager) RequestVotes(ctx context.Context, term *proto.Term) (*proto.Vote, error) {
	vote := &proto.Vote{Elected: false}
	if term.TermCount == -100 { // sync leader election
		v.voted <- peer.Peer{
			Zone:     int(term.Zone),
			HostName: term.LeaderHostname,
			HttpPort: term.LeaderHttpPort,
			GrpcPort: term.LeaderGrpcPort,
			UdpPort:  term.LeaderUdpPort,
			Mode:     term.Mode,
			RaftTerm: -1,
			SyncTerm: int(term.TermCount),
		}
		vote.Elected = true
	} else {
		if term.TermCount > int32(v.self().RaftTerm) {
			v.voted <- peer.Peer{
				Zone:     int(term.Zone),
				HostName: term.LeaderHostname,
				HttpPort: term.LeaderHttpPort,
				GrpcPort: term.LeaderGrpcPort,
				UdpPort:  term.LeaderUdpPort,
				Mode:     term.Mode,
				RaftTerm: int(term.TermCount),
				SyncTerm: -1,
			}
			vote.Elected = true
		}
	}

	return vote, nil
}

func getPacket(dm *dataManager) func(id string) *gossip.Packet {
	return func(id string) *gossip.Packet {
		return dm.Data.GetPacket(id)
	}
}

func getData(eng *engine.Engine, dm *dataManager) func() transport.SyncRsp {
	return func() transport.SyncRsp {
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
		fmt.Println("Returning ", r)
		return r
	}
}

func Start(ctx context.Context, envFile string) (*dataManager, *gossipManager, *provider.JaegerProvider) {

	var self peer.Peer
	self, transport.RegistryUrl = peer.FromEnv(envFile)

	tracerId := self.HttpAddr()

	url := "http://localhost:14268/api/traces"

	jp := provider.InitJaeger(context.Background(), tracerId, self.HttpPort, url)
	go func(ctx context.Context, jp *provider.JaegerProvider) {
		<-ctx.Done()
		jp.Close()
	}(ctx, jp)

	hClient := transport.NewHttpClient(self.HttpPort, jp.Get().Tracer(tracerId))

	eng := engine.Init(self, &hClient)

	gsp, gspRcv := gossip.Config(self.HostName, self.UdpPort, self.HttpAddr()) // id=availableAt for packet

	mLogger.Apply(mLogger.Level(hclog.Trace), mLogger.Color(true))
	gm := gossipManager{
		gsp: gsp,
		rcv: gspRcv,
	}
	vm := voteManager{self: func() *peer.Peer {
		s := eng.Self()
		return &s
	}, voted: make(chan peer.Peer)}

	dm := dataManager{
		vm: data.VersionMap(),
		state: func() *peer.State {
			return eng.State()
		},
		sm: &snapshotManager{
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
			return eng.GetFollowers()
		},
		syncPeers: func() []peer.Peer {
			return eng.GetSyncFollowers()
		},
		Logger: mLogger.Get("dm" + self.HttpPort),
	}

	httpCbs := append(eng.BuildHttpCbs(), []transport.HTTPCbs{
		transport.WithRaftFollowerCb(func(peer peer.Peer) {
			eng.AddFollower(peer)
			gm.gsp.Add(gossip.Peer{
				UdpAddress:        peer.UdpAddr(),
				ProcessIdentifier: peer.HttpAddr(),
				Hop:               0,
			})
		}),
		transport.WithSyncFollowerCb(func(peer peer.Peer) {
			eng.AddSyncFollower(peer)
			gm.gsp.Add(gossip.Peer{
				UdpAddress:        peer.UdpAddr(),
				ProcessIdentifier: peer.HttpAddr(),
				Hop:               0,
			})
		}),
		transport.WithSyncInitialOrderCb(getData(eng, &dm)),
		transport.WithSnapshotFile(eng.DataFile),
		transport.WithPacketCb(getPacket(&dm)),
	}...)

	opts := []transport.RpcOption{
		transport.WithPort(self.GrpcPort),
		transport.WithDataServer(&dm),
		transport.WithVotingServer(&vm),
	}

	rpcServer := transport.NewRpcServer(opts...)
	httpServer := transport.NewHttpSrv(self.HttpPort, tracerId, httpCbs...)

	rpcServer.Start(ctx)
	httpServer.Start(ctx)
	eng.Start()
	<-time.After(raft.Hb_Timeout * 2)

	dm.Info("started...", "isZoneLeader", dm.isZoneLeader(), "isSyncLeader", dm.isSyncLeader())

	var p *proto.Peers
	var gossipPeers []gossip.Peer

	// sync Initial order
	var initialEventOrder []vClock.Event // todo unused
	var entries []snapshot.Entry
	ctx1, cancel := context.WithDeadline(context.Background(), time.Now().Add(5*time.Second))
	defer cancel()
	if !dm.isZoneLeader() && !dm.isSyncLeader() { // follower
		hClient.SendSyncRequest(dm.state().RaftLeader.HttpAddr(), &initialEventOrder, &entries, dm.state().Self)
		c := transport.NewDataSyncClient(dm.state().RaftLeader.GrpcAddr())
		p, _ = c.GetNetworkView(ctx1, &proto.Ok{})

	} else if dm.isZoneLeader() && !dm.isSyncLeader() { // zoneLeader
		hClient.SendSyncRequest(dm.state().SyncLeader.HttpAddr(), &initialEventOrder, &entries, dm.state().Self)
		c := transport.NewDataSyncClient(dm.state().SyncLeader.GrpcAddr())
		p, _ = c.GetNetworkView(ctx1, &proto.Ok{})
	} else { // syncLeader
		// first node in network
	}
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
	}

	dm.waitOnGossip(ctx, &gm, &hClient)
	dm.waitOnMissingPackets(ctx, &hClient)
	dm.startRoundSync(ctx, &gm, &hClient)

	return &dm, &gm, jp
}
func (dm *dataManager) startRoundSync(ctx context.Context, gm *gossipManager, hClient *transport.HttpClient) {
	var syncDelay = 5000 * time.Millisecond

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(syncDelay):
				if !dm.isZoneLeader() {
					//dm.Info("As follower, Saving snapshot")
					//dm.Data.ApplyOrder(dm.Events.GetOrderedIds())
					// dm.saveSnapshot()
					continue
				}
				if !dm.isSyncLeader() {
					//dm.Info("Not follower or syncLeader")
					continue
				}
				//dm.Trace("syncLeader: Syncing round data ... 1/4")
				// send current events to other leaders and merge with response,

				for _, l := range dm.syncPeers() {
					returnedEventsUnordered := hClient.SyncOrder(l.HttpAddr(), dm.Events.GetOrder())
					dm.Events.MergeEvents(returnedEventsUnordered...)
				}

				fo := utils.EventsToOrder(dm.Events.GetOrder())
				if fo == nil {
					continue
				}
				// send calculated order to other leaders and zone followers
				ctx1, cancel := context.WithDeadline(context.Background(), time.Now().Add(5*time.Second))
				var wg sync.WaitGroup
				for _, l := range dm.syncPeers() {
					wg.Add(1)
					go func(wg *sync.WaitGroup, l peer.Peer) {
						defer wg.Done()
						c := transport.NewDataSyncClient(l.GrpcAddr())
						c.SaveOrder(ctx1, fo)
						c.Disconnect()
					}(&wg, l)
				}
				for _, f := range dm.zonePeers() {
					wg.Add(1)
					go func(wg *sync.WaitGroup, f peer.Peer) {
						defer wg.Done()
						c := transport.NewDataSyncClient(f.GrpcAddr())
						c.SaveOrder(ctx1, fo)
						c.Disconnect()
					}(&wg, f)
				}
				wg.Wait()
				cancel()
				//dm.Trace("syncLeader: Applying snapshot ... 2/4")

				dm.Data.ApplyOrder(dm.Events.GetOrderedIds())
				dm.saveSnapshot()

				// sync if new round
				const RoundResolution = 5
				if dm.round < RoundResolution {
					dm.round++
					//dm.Trace("syncLeader: in the same round ... 3/4 & 4/4")
					continue
				}
				dm.round = 0
				dm.sm.RoundNum++
				//dm.Trace("syncLeader: Syncing round number ... 3/4")

				ctx1, cancel = context.WithDeadline(context.Background(), time.Now().Add(15*time.Second))
				for _, l := range dm.syncPeers() {
					wg.Add(1)
					go hClient.SendRoundNum(ctx1, &wg, dm.sm.RoundNum, l.HttpAddr())

				}
				for _, f := range dm.zonePeers() {
					wg.Add(1)
					go hClient.SendRoundNum(ctx1, &wg, dm.sm.RoundNum, f.HttpAddr())
				}
				wg.Wait()

				dm.sm.Round()
				dm.Events.Reset()
				cancel()
				//dm.Trace("syncLeader: Data sync complete...4/4")

			}
		}
	}()
}
func (dm *dataManager) waitOnGossip(ctx context.Context, gm *gossipManager, hClient *transport.HttpClient) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case gp := <-gm.rcv:
				dm.Info("Received gossip packet from network", "id", gp.GetId())
				var leader string
				if !dm.isZoneLeader() {
					// send event to zoneLeader
					leader = dm.state().RaftLeader.GrpcAddr()
				} else {
					// send to syncLeader
					leader = dm.state().SyncLeader.GrpcAddr()
				}
				ctx1, cancel := context.WithDeadline(context.Background(), time.Now().Add(10*time.Second))
				c := transport.NewDataSyncClient(leader)
				c.NewEvent(ctx1, utils.PacketToOrder(gp))
				cancel()
				c.Disconnect()

				dm.Data.SaveUnorderedPacket(gp)
				dm.Info("Saved unordered packet ", "id", gp.GetId())
				dm.Info("Available at", "addrs", dm.Data.GetPacketAvailableAt(gp.GetId()))
			}
		}
	}()
}

func (dm *dataManager) waitOnMissingPackets(ctx context.Context, hClient *transport.HttpClient) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case mp := <-dm.Data.MissingPacket():
				if dm.Data.GetPacket(mp) != nil {
					dm.Info("Already retrieved packet ", "id", mp)
					continue
				}
				dm.Info("Retrieving missing packet ", "id", mp)
				var peers []string
				if !dm.isZoneLeader() {
					// ask raftLeader for missing packet addresses

					peers = append(peers, dm.state().RaftLeader.GrpcAddr())
				} else if dm.isSyncLeader() {
					for _, p := range dm.zonePeers() {
						peers = append(peers, p.GrpcAddr())
					}
				} else {
					// ask syncLeader for missing packet addresses

					peers = append(peers, dm.state().SyncLeader.GrpcAddr())
				}
				// ask leader for peers where packet is available
				fmt.Println(dm.state())
				dm.Info("Asking for packet at", "peers", utils.PrintJson(peers))
				for _, p := range peers {
					c := transport.NewDataSyncClient(p)
					ctx1, can := context.WithDeadline(context.Background(), time.Now().Add(10*time.Second))
					dm.Info("Asking for packet", "peer", p)
					peers, _ := c.GetPacketAddresses(ctx1, &proto.Ok{Id: mp})
					rand.Seed(time.Now().UnixMicro())
					dm.Warn("Packet available at - " + utils.PrintJson(peers))
					r := rand.Intn(len(peers.Peers))

					// retrive missing packet from a random peer
					gp := hClient.DownloadPacket(peers.Peers[r].PeerId, mp)
					c.Disconnect()
					can()
					if gp != nil {
						dm.Data.SaveOrderedPacket(*gp)
						dm.Info("Saved missing packet ", "id", mp)
						break
					} else {
						dm.Error("Cannot retrieve missing packet ", "id", mp)
					}
				}
			}

		}
	}()
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

	dm1, gm1, jp := Start(ctx, envFile)

	defer jp.Shutdown(ctx)

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
