package gossipProtocol

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/Ishan27g/vClock"
)

// Packet exchanged between peers
type Packet struct {
	AvailableAt   []string          `json:"AvailableAt"` // at which addresses the data is available
	GossipMessage gossipMessage     `json:"GossipMessage"`
	VectorClock   vClock.EventClock `json:"VectorClock"`
}

type udpGossip struct {
	Packet Packet `json:"Packet"`
	From   Peer   `json:"From"`
}

func packetToUdp(packet Packet, from Peer) udpGossip {
	return udpGossip{
		Packet: packet, From: from,
	}
}

type gossipMessage struct {
	Data              string
	CreatedAt         time.Time // unused
	GossipMessageHash string
	Version           int
}

func (p *Packet) GetId() string {
	return p.GossipMessage.GossipMessageHash
}
func (p *Packet) GetVersion() int {
	return p.GossipMessage.Version
}
func (p *Packet) GetData() string {
	return p.GossipMessage.Data
}

func gossipToByte(g gossipMessage, from Peer, clock vClock.EventClock) []byte {
	packet := gossipToPacket(g, from.ProcessIdentifier, clock)
	udp := packetToUdp(*packet, from)
	b, e := json.Marshal(&udp)
	if e != nil {
		fmt.Println("gossipToByte ", e.Error())
	}
	return b
}

func gossipToPacket(g gossipMessage, from string, clock vClock.EventClock) *Packet {
	return &Packet{
		AvailableAt:   []string{from},
		GossipMessage: g,
		VectorClock:   clock,
	}
}
func ByteToPacket(b []byte) (Packet, Peer) {
	udp := udpGossip{
		Packet: Packet{
			AvailableAt:   []string{},
			GossipMessage: gossipMessage{},
			VectorClock:   vClock.EventClock{},
		},
		From: Peer{
			UdpAddress:        "",
			ProcessIdentifier: "",
			Hop:               -1,
		},
	}
	_ = json.Unmarshal(b, &udp)
	return udp.Packet, udp.From
}

// NewGossipMessage creates a Gossip message with current timestamp,
// creating a unique hash for every message
func NewGossipMessage(data string, from string, clock vClock.EventClock) Packet {
	g := gossipMessage{
		Data:              data,
		CreatedAt:         time.Now().UTC(),
		GossipMessageHash: "",
		Version:           0,
	}
	g.GossipMessageHash = defaultHashMethod(g.Data + g.CreatedAt.String())
	return *gossipToPacket(g, from, clock)
}
