package main

import gossip "github.com/Ishan27g/gossipProtocol"

type gossipManager struct {
	gsp gossip.Gossip
	rcv <-chan gossip.Packet
}

func (g *gossipManager) Gossip(data string) {
	go g.gsp.SendGossip(data)
}
func (g *gossipManager) Receive() gossip.Packet {
	return <-g.rcv
}
