package main

import (
	"context"
	"fmt"
	"math/rand"
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

var envFile = ".envFiles/1.leader.env"

type dataManager struct {
	vm data.VersionAbleI

	state  func() *peer.State
	sm     *snapshotManager
	Data   data.Data  // gossip data
	Events data.Event // order of events maintained only by the leader
	Tmp    data.Event // tmp events as fallback if SyncLeader has timed out

	LastOrderHash string
	round         int

	enginePeers func() []peer.Peer
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

func (d *dataManager) isZoneLeader() bool {
	return d.state().RaftLeader.HttpAddr() == d.state().Self.HttpAddr()
}
func (d *dataManager) isSyncLeader() bool {
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
	entries := utils.OrderToEntries(d.Data.GetOrderedPackets()...)
	currentHash := utils.DefaultHash(entries)
	if d.sm.LastSnapshotHash != currentHash {
		d.sm.Apply(entries...)
		d.sm.Save()
	}
	d.sm.LastSnapshotHash = currentHash
}
func (d *dataManager) sendOrderToFollowers(order *proto.Order) {
	if !d.isZoneLeader() {
		return
	}
	ctx, can := context.WithDeadline(context.Background(), time.Now().Add(10*time.Second))
	defer can()
	for _, f := range d.enginePeers() {
		c := transport.NewDataSyncClient(f.UdpAddr())
		c.SaveOrder(ctx, order)
		c.Disconnect()
	}
}
func (d *dataManager) SaveOrder(ctx context.Context, order *proto.Order) (*proto.Ok, error) {

	e := utils.OrderToEvents(order)
	d.Events.MergeEvents(e...)
	o := d.Events.GetOrderedIds()
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
	if !d.isSyncLeader() {
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
	g.gsp().SendGossip(data)
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
		var ee []snapshot.Entry
		for _, entry := range entries {
			if dm.sm.RoundNum > entry.Round {
				ee = append(ee, entry)
			}
		}
		order := dm.Events.GetOrder()
		return transport.SyncRsp{
			OrderedEvents: order,
			Entries:       entries,
			SyncLeader:    *&eng.State().SyncLeader,
		}
	}
}

func Start(ctx context.Context, envFile string) (*engine.Engine, *snapshotManager, *dataManager, *gossipManager,
	*provider.JaegerProvider) {

	var self peer.Peer
	self, transport.RegistryUrl = peer.FromEnv(envFile)

	eng := engine.Init(self)

	mLogger.Apply(mLogger.Level(hclog.Trace), mLogger.Color(true))

	vm := voteManager{self: func() *peer.Peer {
		return eng.Self()
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
		enginePeers: func() []peer.Peer {
			return eng.GetFollowers()
		},
	}

	gm := gossipManager{
		gsp: func() gossip.Gossip {
			return eng.Gossip.Gossip
		},
		rcv: eng.Gossip.GossipRcv,
	}

	httpCbs := append(eng.BuildHttpCbs(), []transport.HTTPCbs{
		transport.WithSyncInitialOrderCb(getData(eng, &dm)),
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

	<-time.After(raft.Hb_Timeout * 2)
	// sync Initial order
	var initialEventOrder []vClock.Event // todo unused
	var entries []snapshot.Entry
	fmt.Println(!dm.isZoneLeader(), !dm.isSyncLeader())
	if !dm.isZoneLeader() && !dm.isSyncLeader() { // follower
		eng.HClient.SendSyncRequest(dm.state().RaftLeader.HttpAddr(), &initialEventOrder, &entries, dm.state().Self)
	} else if dm.isZoneLeader() && !dm.isSyncLeader() { // zoneLeader
		eng.HClient.SendSyncRequest(dm.state().SyncLeader.HttpAddr(), &initialEventOrder, &entries, dm.state().Self)
	} else { // syncLeader
		// first node in network
	}
	dm.sm.Sync(entries...)

	dm.waitOnGossip(ctx, &gm, eng.HClient)
	dm.waitOnMissingPackets(ctx, eng.HClient)

	startRoundSync(ctx, eng, &dm, &gm)
	return eng, dm.sm, &dm, &gm, jp
}
func startRoundSync(ctx context.Context, eng *engine.Engine, dm *dataManager, gm *gossipManager) {
	var syncDelay = 1200 * time.Millisecond

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(syncDelay):
				if !dm.isZoneLeader() {
					dm.Data.ApplyOrder(dm.Events.GetOrderedIds())
					dm.saveSnapshot()
					continue
				}
				if !dm.isSyncLeader() {
					continue
				}
				// send current events to other leaders and merge with response,
				otherLeaders := eng.GetSyncFollowers()
				for _, l := range otherLeaders {
					returnedEventsUnordered := eng.HClient.SyncOrder(l.HttpAddr(), dm.Events.GetOrder())
					dm.Events.MergeEvents(returnedEventsUnordered...)
				}

				fo := utils.EventsToOrder(dm.Events.GetOrder())

				// send calculated order to other leaders and zone followers
				ctx1, cancel := context.WithDeadline(context.Background(), time.Now().Add(15*time.Second))
				var wg sync.WaitGroup
				for _, l := range otherLeaders {
					wg.Add(1)
					go func(wg *sync.WaitGroup, l peer.Peer) {
						defer wg.Done()
						c := transport.NewDataSyncClient(l.GrpcAddr())
						c.SaveOrder(ctx1, fo)
						c.Disconnect()
					}(&wg, l)
				}
				for _, f := range eng.GetFollowers() {
					wg.Add(1)
					go func(wg *sync.WaitGroup, f peer.Peer) {
						c := transport.NewDataSyncClient(f.GrpcAddr())
						c.SaveOrder(ctx1, fo)
						c.Disconnect()
					}(&wg, f)
				}
				wg.Wait()
				cancel()

				dm.Data.ApplyOrder(dm.Events.GetOrderedIds())
				dm.saveSnapshot()

				// sync if new round
				const RoundResolution = 5
				if dm.round < RoundResolution {
					dm.round++
					continue
				}
				dm.round = 0
				dm.sm.RoundNum++

				ctx1, cancel = context.WithDeadline(context.Background(), time.Now().Add(15*time.Second))
				for _, l := range otherLeaders {
					wg.Add(1)
					go eng.HClient.SendRoundNum(ctx1, &wg, dm.sm.RoundNum, l.HttpAddr())

				}
				for _, f := range eng.GetFollowers() {
					wg.Add(1)
					go eng.HClient.SendRoundNum(ctx1, &wg, dm.sm.RoundNum, f.HttpAddr())
				}
				wg.Wait()

				dm.sm.Round()
				dm.Events.Reset()
				cancel()

				continue
			}
		}
	}()
}
func (dm *dataManager) waitOnGossip(ctx context.Context, gm *gossipManager, hClient transport.HttpClient) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case gp := <-gm.rcv:
				var leader string
				if !dm.isZoneLeader() {
					// todo
					// send event to zoneLeader
					leader = dm.state().RaftLeader.GrpcAddr()
				} else {
					// todo
					// send to syncLeader
					leader = dm.state().SyncLeader.GrpcAddr()
				}
				ctx1, cancel := context.WithDeadline(context.Background(), time.Now().Add(10*time.Second))
				c := transport.NewDataSyncClient(leader)
				c.NewEvent(ctx1, utils.PacketToOrder(gp))
				cancel()
				c.Disconnect()

				dm.Data.SaveUnorderedPacket(gp)
			}
		}
	}()
}

func (dm *dataManager) waitOnMissingPackets(ctx context.Context, hClient transport.HttpClient) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case mp := <-dm.Data.MissingPacket():
				try := 2
				var leader string
				if !dm.isZoneLeader() {
					// ask raftLeader for missing packet addresses

					leader = dm.state().RaftLeader.GrpcAddr()
					goto download
				} else {
					// ask syncLeader for missing packet addresses

					leader = dm.state().SyncLeader.GrpcAddr()
					goto download
				}
			download:
				{
					// ask leader for peers where packet is available

					c := transport.NewDataSyncClient(leader)
					ctx1, can := context.WithDeadline(context.Background(), time.Now().Add(10*time.Second))
					peers, _ := c.GetPacketAddresses(ctx1, &proto.Ok{Id: mp})
					rand.Seed(time.Now().UnixMicro())
					r := rand.Intn(len(peers.Peers))
					// retrive missing packet from a random peer

					gp := hClient.DownloadPacket(peers.Peers[r].PeerId, mp)
					if gp != nil {
						dm.Data.SaveOrderedPacket(*gp)
					} else if try > 0 {
						try--
						goto download
					} else {
						fmt.Println("Missing packet not downloaded -", mp)
					}
					c.Disconnect()
					can()
				}
			}

		}

	}()
}
func main() {

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
	<-make(chan bool)

}
