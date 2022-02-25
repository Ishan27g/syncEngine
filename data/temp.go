package data

import (
	gossip "github.com/Ishan27g/gossipProtocol"
	"github.com/emirpasic/gods/trees/btree"
)

type tmpData struct {
	data *btree.Tree
}

func (t *tmpData) Add(packet gossip.Packet) {
	t.data.Put(packet.GetId(), packet)
}
func (t *tmpData) Remove(id string) *gossip.Packet {
	removed := t.Get(id)
	t.data.Remove(id)
	return removed
}
func (t *tmpData) Get(id string) *gossip.Packet {
	value, found := t.data.Get(id)
	if !found {
		return nil
	}
	gP := value.(gossip.Packet)
	return &gP
}

var Reverse = func(s []string) []string {
	newNumbers := make([]string, len(s))
	for i := len(s) - 1; i >= 0; i-- {
		newNumbers = append(newNumbers, s[i])
	}
	return newNumbers
}
