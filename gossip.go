package main

import gossip "github.com/Ishan27g/gossipProtocol"

type gossipManager struct {
	gsp gossip.Gossip
	rcv <-chan gossip.Packet
}

func newGossipManager(gsp gossip.Gossip, rcv <-chan gossip.Packet) gossipManager {
	gm := gossipManager{gsp: gsp, rcv: rcv}
	return gm
}
func (g *gossipManager) Gossip(data string) {
	g.gsp.SendGossip(data)
}
