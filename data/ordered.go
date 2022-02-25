package data

import (
	gossip "github.com/Ishan27g/gossipProtocol"
	"github.com/emirpasic/gods/maps/linkedhashmap"
)

type orderedData struct {
	orderedGossip *linkedhashmap.Map // ordered id's and gossip-packet
}

// addPacket adds a new packet
func (od *orderedData) addPacket(gP ...*gossip.Packet) {
	if od.orderedGossip == nil {
		od.orderedGossip = linkedhashmap.New()
	}
	for _, packet := range gP {
		od.orderedGossip.Put(packet.GetId(), packet)
	}
}

// updatePacket the packet which was already added, maintaining order
func (od *orderedData) updatePacket(gP ...*gossip.Packet) {
	for _, packet := range gP {
		od.orderedGossip.Put(packet.GetId(), packet)
	}
}

// addIds adds id with empty packet
func (od *orderedData) addIds(ids ...string) {
	for _, id := range ids {
		od.addId(id)
	}
}
func (od *orderedData) addId(id string) {
	if od.orderedGossip == nil {
		od.orderedGossip = linkedhashmap.New()
	}
	empty := gossip.NewGossipMessage("", "", nil)
	od.orderedGossip.Put(id, &empty)
}
func (od *orderedData) checkIdPreviouslyAdded(id string) bool {
	_, found := od.orderedGossip.Get(id)
	return found
}

// getPacket returns packet if not empty packet, or nil if id not added
func (od *orderedData) getPacket(id string) *gossip.Packet {
	packet, found := od.orderedGossip.Get(id)
	if !found {
		return nil
	}
	if packet.(*gossip.Packet).GossipMessage.Data == "" {
		return nil
	}
	return packet.(*gossip.Packet)
}
