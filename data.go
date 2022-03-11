package main

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	gossip "github.com/Ishan27g/gossipProtocol"
	"github.com/hashicorp/go-hclog"

	"github.com/Ishan27g/syncEngine/data"
	"github.com/Ishan27g/syncEngine/peer"
	"github.com/Ishan27g/syncEngine/proto"
	"github.com/Ishan27g/syncEngine/snapshot"
	"github.com/Ishan27g/syncEngine/transport"
	"github.com/Ishan27g/syncEngine/utils"
)

type dataManager struct {
	vm data.VersionAbleI

	state  func() *peer.State
	sm     *snapshot.Manager
	Data   data.Data  // gossip data
	Events data.Event // order of events maintained only by the leader
	Tmp    data.Event // tmp events as fallback if SyncLeader has timed out

	downloadingPacket int
	LastOrderHash     string

	zonePeers func() []peer.Peer
	syncPeers func() []peer.Peer
	hclog.Logger
	round     int
	nextRound chan int
}

func (dm *dataManager) isZoneLeader() bool {
	return dm.state().RaftLeader.HttpAddr() == dm.state().Self.HttpAddr()
}
func (dm *dataManager) isSyncLeader() bool {
	// d.Info("SyncLeader", "HttpAddr", d.state().SyncLeader.HttpAddr())
	// d.Info("Self", "HttpAddr", d.state().Self.HttpAddr())
	return dm.state().SyncLeader.HttpAddr() == dm.state().Self.HttpAddr()
}

func (dm *dataManager) saveSnapshot() {
	if !dm.canSnapshot() {
		go func() {
			<-time.After(1 * time.Second)
			dm.saveSnapshot()
		}()
		return
	}
	entries := utils.OrderToEntries(dm.Data.GetOrderedPackets()...)
	currentHash := utils.DefaultHash(entries)
	if dm.sm.LastSnapshotHash != currentHash {
		dm.sm.Apply(entries...)
		dm.sm.Save()
		dm.Info("Saved snapshot")
	}
	dm.sm.LastSnapshotHash = currentHash
}

func (dm *dataManager) canSnapshot() bool {
	if len(dm.Data.GetOrderedPackets()) == 0 {
		dm.Info("no packets to save")
		return false
	}
	entries := utils.OrderToEntries(dm.Data.GetOrderedPackets()...)
	if entries == nil || len(entries) == 0 {
		// dm.Info("Nothing to save in snapshot")
		return false
	}
	if dm.downloadingPacket > 0 {
		dm.Warn("Still downloading...", "count", dm.downloadingPacket)
		return false
	}
	return true
}
func (dm *dataManager) sendOrderToFollowers(order *proto.Order) {
	if !dm.isZoneLeader() {
		return
	}
	ctx, can := context.WithDeadline(context.Background(), time.Now().Add(10*time.Second))
	defer can()
	for _, f := range dm.zonePeers() {
		c := transport.NewDataSyncClient(ctx, f.GrpcAddr())
		c.SaveOrder(ctx, order)
	}
}
func (dm *dataManager) NewEvent(ctx context.Context, order *proto.Order) (*proto.Ok, error) {
	rsp := &proto.Ok{}
	if !dm.isZoneLeader() {
		dm.Warn("new event from peer, cannot receive")
		return rsp, nil
	}
	var localEvents = new(data.Event)
	if dm.isSyncLeader() {
		localEvents = &dm.Events
	} else {
		localEvents = &dm.Tmp // tmp events
	}
	events := utils.OrderToEvents(order)
	for _, event := range events {
		if dm.vm.GetVersion(event.EventId) == -1 {
			localEvents.MergeEvent(event.EventId, event.EventClock)
		}
		dm.vm.UpdateVersion(event.EventId)
	}
	dm.Warn("new event from peer")
	return rsp, nil
}
func (dm *dataManager) SaveOrder(ctx context.Context, order *proto.Order) (*proto.Ok, error) {
	e := utils.OrderToEvents(order)
	// dm.Info("Saving order " + utils.PrintJson(e))
	dm.Events.MergeEvents(e...)
	o := dm.Events.GetOrderedIds()
	//	d.Info("App order " + utils.PrintJson(e))
	dm.Data.ApplyOrder(o)
	orderHash := utils.DefaultHash(o)

	dm.saveSnapshot()

	if dm.isZoneLeader() && !dm.isSyncLeader() {
		if dm.LastOrderHash != orderHash {
			dm.sendOrderToFollowers(order)
			dm.Tmp.Reset()
			dm.LastOrderHash = orderHash
		}
	}

	return &proto.Ok{}, nil
}

func (dm *dataManager) GetSyncData(ctx context.Context, ok *proto.Ok) (*proto.Order, error) {
	o := dm.Events.GetOrder()
	return utils.EventsToOrder(o), nil
}

func (dm *dataManager) GetPacketAddresses(ctx context.Context, ok *proto.Ok) (*proto.Peers, error) {
	atPeers := dm.Data.GetPacketAvailableAt(ok.Id)
	var peers []*proto.Peer
	for _, p := range atPeers {
		peers = append(peers, &proto.Peer{
			UdpAddress: "",
			PeerId:     p,
		})
	}
	return &proto.Peers{Peers: peers}, nil
}

func (dm *dataManager) GetNetworkView(ctx context.Context, ok *proto.Ok) (*proto.Peers, error) {
	if !dm.isZoneLeader() || !dm.isSyncLeader() {
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

func getPacket(dm *dataManager) func(id string) *gossip.Packet {
	return func(id string) *gossip.Packet {
		return dm.Data.GetPacket(id)
	}
}

func (dm *dataManager) startRoundSync(ctx context.Context, gm *gossipManager, hClient *transport.HttpClient) {
	var syncDelay = 3000 * time.Millisecond

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(syncDelay):
				if !dm.isZoneLeader() {
					//dm.Info("follower: ...", "roundNum", dm.sm.RoundNum)
					//dm.Data.ApplyOrder(dm.Events.GetOrderedIds())
					// dm.saveSnapshot()
					continue
				}
				if !dm.isSyncLeader() {
					// dm.Info("Not follower or syncLeader")
					continue
				}
				// dm.Trace("syncLeader: Syncing network events...", "roundNum", dm.sm.RoundNum)

				// send current events to other leaders and merge with response,
				for _, l := range dm.syncPeers() {
					returnedEventsUnordered := hClient.SyncOrder(l.HttpAddr(), dm.Events.GetOrder())
					dm.Events.MergeEvents(returnedEventsUnordered...)
				}
				if len(dm.Events.GetOrder()) == 0 {
					continue
				}
				ctx1, cancel := context.WithDeadline(context.Background(), time.Now().Add(10*time.Second))

				dm.Trace("syncLeader: Data sync for round ...", "roundNum", dm.sm.RoundNum)
				var wg sync.WaitGroup

				if dm.applyOrderAtPeers(ctx1, cancel, &wg) {
					dm.Data.ApplyOrder(dm.Events.GetOrderedIds())
				}
				//
				if !dm.canSnapshot() {
					dm.Trace("syncLeader: Data sync complete...", "roundNum", dm.sm.RoundNum)
					continue
				}

				dm.saveSnapshot()

				//sync if new round
				//if dm.round < RoundResolution {
				//	dm.round++
				//	dm.Trace("syncLeader: Data sync complete...in the same round ... 3/4 & 4/4")
				//	continue
				//}
				//dm.round = 0

				dm.sm.RoundNum++

				dm.sendRoundNum(hClient)

				dm.sm.Round()
				dm.Events.Reset()

				cancel()
				dm.Trace("syncLeader: Data sync complete...", "roundNum", dm.sm.RoundNum)

			}
		}
	}()
}

func (dm *dataManager) sendRoundNum(hClient *transport.HttpClient) {
	var wg sync.WaitGroup
	ctx1, cancel := context.WithDeadline(context.Background(), time.Now().Add(15*time.Second))
	defer cancel()
	if dm.isSyncLeader() {
		for _, l := range dm.syncPeers() {
			wg.Add(1)
			go hClient.SendRoundNum(ctx1, &wg, dm.sm.RoundNum, l.HttpAddr())
		}
	}
	if dm.isZoneLeader() {
		for _, f := range dm.zonePeers() {
			wg.Add(1)
			go hClient.SendRoundNum(ctx1, &wg, dm.sm.RoundNum, f.HttpAddr())
		}
		wg.Wait()
	}
}

func (dm *dataManager) applyOrderAtPeers(ctx1 context.Context, cancel context.CancelFunc, wg *sync.WaitGroup) bool {
	if len(dm.Events.GetOrder()) == 0 {
		return false
	}
	fo := utils.EventsToOrder(dm.Events.GetOrder())
	// send calculated order to other leaders and zone followers
	for _, l := range dm.syncPeers() {
		wg.Add(1)
		go func(wg *sync.WaitGroup, l peer.Peer) {
			defer wg.Done()
			c := transport.NewDataSyncClient(ctx1, l.GrpcAddr())
			_, err := c.SaveOrder(ctx1, fo)
			if err != nil {
				dm.Warn("Error from", "zone-leader", l.GrpcAddr())
				fmt.Println(err.Error())
			}
		}(wg, l)
	}
	for _, f := range dm.zonePeers() {
		wg.Add(1)
		go func(wg *sync.WaitGroup, f peer.Peer) {
			defer wg.Done()
			c := transport.NewDataSyncClient(ctx1, f.GrpcAddr())
			_, err := c.SaveOrder(ctx1, fo)
			if err != nil {
				dm.Warn("Error from", "follower", f.GrpcAddr())
				fmt.Println(err.Error())
			}
		}(wg, f)
	}
	wg.Wait()
	cancel()
	return true
}
func (dm *dataManager) waitOnGossip(ctx context.Context, gm *gossipManager, hClient *transport.HttpClient) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				dm.Error("context done")
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
				if dm.isSyncLeader() {
					events := utils.OrderToEvents(utils.PacketToOrder(gp))
					for _, event := range events {
						if dm.vm.GetVersion(event.EventId) == -1 {
							dm.Events.MergeEvent(event.EventId, event.EventClock)
						}
						dm.vm.UpdateVersion(event.EventId)
					}
					dm.Data.SaveUnorderedPacket(gp)
					continue
				}
				ctx1, cancel := context.WithDeadline(context.Background(), time.Now().Add(10*time.Second))
				c := transport.NewDataSyncClient(ctx1, leader)
				dm.Info("Sending event to", "leader - ", leader)
				_, err := c.NewEvent(ctx1, utils.PacketToOrder(gp))
				if err != nil {
					dm.Warn("Error from", "leader - ", leader)
				}
				cancel()

				dm.Data.SaveUnorderedPacket(gp)
				dm.Info("Saved unordered packet ", "id", gp.GetId())
				//dm.Info("Available at", "addrs", dm.Data.GetPacketAvailableAt(gp.GetId()))
			}
		}
	}()
}

// func (dm *dataManager) waitOnRoundUpdate(ctx context.Context, hClient *transport.HttpClient) {
// 	go func() {
// 		for {
// 			select {
// 			case <-ctx.Done():
// 				return
// 			case <-time.After(5 * time.Second):
// 				if dm.missingPacketCount == 0{

// 				}
// 			}
// 		}
// 	}()
// }
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
				go dm.download(mp, hClient)
			}
		}
	}()
}

func (dm *dataManager) download(mp string, hClient *transport.HttpClient) {
	dm.downloadingPacket++
	dm.Info("Retrieving missing packet ", "id", mp)
	var peers []string
	goto lookup
lookup:
	{
		peers = []string{}
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
	}
	// ask leader for peers where packet is available
	ctx1, cancel := context.WithDeadline(context.Background(), time.Now().Add(60*time.Second))
	var peersHttp *proto.Peers
	var found = false
	var err error
	for _, p := range peers {
		c := transport.NewDataSyncClient(ctx1, p)
		dm.Info("Asking for packet", "peer", p)
		peersHttp, err = c.GetPacketAddresses(ctx1, &proto.Ok{Id: mp})
		if err != nil {
			dm.Error(err.Error())
		}
		if len(peersHttp.Peers) > 0 {
			found = true
			break
		}
	}
	cancel()
	if !found {
		dm.Warn("Retrying packet lookup in network", "id", mp)
		<-time.After(300 * time.Millisecond)
		goto lookup
	}
	goto download
download:
	{
		rand.Seed(time.Now().UnixMicro())
		r := rand.Intn(len(peersHttp.Peers))
		//r := 0
		if peersHttp.Peers[r].PeerId == dm.state().Self.HttpAddr() {
			goto download
		}
		dm.Warn("Packet downloading from  - " + peersHttp.Peers[r].PeerId)
		// retrieve missing packet from a random peer
		gp := hClient.DownloadPacket(peersHttp.Peers[r].PeerId, mp)
		if gp != nil {
			dm.Data.SaveOrderedPacket(*gp)
			dm.Info("Saved missing packet ", "id", mp)
			dm.downloadingPacket--
			return
		} else {
			dm.Error("Cannot retrieve missing packet ", "id", mp, "peer", peersHttp.Peers[r].PeerId)
			goto download
		}
	}
}
