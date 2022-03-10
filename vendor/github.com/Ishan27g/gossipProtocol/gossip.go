package gossipProtocol

import (
	"context"
	"sync"
	"time"

	"github.com/Ishan27g/vClock"
)

type Gossip interface {
	// Join with some initial peers
	Join(...Peer)
	// Add peers
	Add(...Peer)
	// SendGossip  to the network
	SendGossip(data string)
	// CurrentView returned as a String
	CurrentView() map[string]Peer
}

type gossip struct {
	ctx       context.Context
	cancel    context.CancelFunc
	lock      sync.Mutex
	env       *envConfig
	sampling  iSampling
	udpClient client

	selfDescriptor Peer

	allGossip map[string]*Packet
	allEvents *vClock.VectorClock

	gossipToUser chan Packet
}

func (g *gossip) CurrentView() map[string]Peer {
	return toMap(g.sampling.getView(), g.selfDescriptor.ProcessIdentifier)
}

func (g *gossip) Join(initialPeers ...Peer) {
	g.sampling.SetInitialPeers(initialPeers...)
	g.sampling.Start()

	go Listen(g.ctx, g.env.UdpPort, g.serverCb, g.sampling.ViewFromPeer)
}
func (g *gossip) Add(peer ...Peer) {
	g.sampling.AddPeer(peer...)
}

// SendGossip from User to the network
func (g *gossip) SendGossip(data string) {
	gP := NewGossipMessage(data, g.env.ProcessIdentifier, nil)
	g.lock.Lock()
	newPacket := g.savePacket(&gP)
	g.lock.Unlock()
	g.newGossip(newPacket, gP, g.selfDescriptor)
}

// from peer
func (g *gossip) serverCb(gP Packet, from Peer) []byte {
	g.lock.Lock()
	newPacket := g.savePacket(&gP)
	(*g.allEvents).ReceiveEvent(gP.GetId(), gP.VectorClock)
	g.lock.Unlock()
	go g.newGossip(newPacket, gP, from)
	return []byte("OKAY")
}

func (g *gossip) newGossip(newPacket bool, gP Packet, from Peer) {
	if newPacket {
		g.startRounds(gP.GossipMessage, from)
		g.lock.Lock()
		gP.VectorClock = (*g.allEvents).Get(gP.GetId()) // update packet's clock
		g.lock.Unlock()
		g.gossipToUser <- gP
	}
}
func (g *gossip) savePacket(gP *Packet) bool {
	newGossip := false
	if g.allGossip[gP.GetId()] == nil {
		gP.AvailableAt = append(gP.AvailableAt, g.selfDescriptor.ProcessIdentifier)
		g.allGossip[gP.GetId()] = gP
		newGossip = true
	} else {
		if g.allGossip[gP.GetId()].GetVersion() < gP.GetVersion() {
			g.allGossip[gP.GetId()] = gP
		}
	}
	return newGossip
}
func (g *gossip) startRounds(gm gossipMessage, exclude Peer) {
	for i := 1; i <= rounds; i++ {
		<-time.After(g.env.RoundDelay)
		if g.sampling.Size() == 0 {
			return
		}
		g.fanOut(gm, exclude)
		gm.Version++
	}
}

func (g *gossip) fanOut(gm gossipMessage, exclude Peer) {
	id := gm.GossipMessageHash
	if g.sampling.Size() == 0 {
		return
	}
	for i := 0; i < g.env.FanOut; i++ {
		peer := g.sampling.GetPeer(exclude)
		if peer.UdpAddress != "" && peer.ProcessIdentifier != g.selfDescriptor.ProcessIdentifier {
			g.lock.Lock()
			tmp := vClock.Copy(*g.allEvents)
			clock := tmp.SendEvent(id, []string{peer.ProcessIdentifier})
			buffer := gossipToByte(gm, g.selfDescriptor, clock)
			g.lock.Unlock()
			if g.udpClient.send(peer.UdpAddress, buffer) != nil {
				g.lock.Lock()
				*g.allEvents = tmp
				g.lock.Unlock()
			} else {
				g.sampling.removePeer(peer)
				g.lock.Lock()
				(*g.allEvents).SendEvent(id, nil)
				g.lock.Unlock()
			}

		}
	}
}

func Config(hostname string, port string, id string) (Gossip, <-chan Packet) {
	ctx, cancel := context.WithCancel(context.Background())
	g := gossip{
		lock:   sync.Mutex{},
		ctx:    ctx,
		cancel: cancel,
		selfDescriptor: Peer{
			UdpAddress:        hostname + ":" + port,
			Hop:               0,
			ProcessIdentifier: id,
		},
		env:          defaultEnv(hostname, port, id),
		udpClient:    getClient(id),
		allGossip:    make(map[string]*Packet),
		allEvents:    new(vClock.VectorClock),
		gossipToUser: make(chan Packet, 100),
		sampling:     initSampling(port, id, defaultStrategy),
	}
	*g.allEvents = vClock.Init(id)
	return &g, g.gossipToUser
}
