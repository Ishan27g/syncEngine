package data

import (
	"fmt"
	"sync"

	gossip "github.com/Ishan27g/gossipProtocol"
	"github.com/emirpasic/gods/maps/linkedhashmap"
	"github.com/emirpasic/gods/trees/btree"
)

type Data interface {
	SaveOrderedPacket(gP gossip.Packet) bool
	SaveUnorderedPacket(gP gossip.Packet)
	ApplyOrder([]string)
	GetOrderedPackets() []gossip.Packet
	MissingPacket() <-chan string
	GetPacket(id string) *gossip.Packet
	GetPacketAvailableAt(id string) []string
}
type localData struct {
	mx             sync.Mutex
	tmpGossip      tmpData     // from all rounds
	orderedData    orderedData // per round
	missingPackets chan string // ids of packets not present in orderedGossip
}

func (ld *localData) GetPacket(id string) *gossip.Packet {
	if gP := ld.orderedData.getPacket(id); gP != nil {
		return gP
	}
	if gP := ld.tmpGossip.Get(id); gP != nil {
		return gP
	}
	fmt.Println("no packet - ", id)
	return nil
}
func (ld *localData) GetOrderedPackets() []gossip.Packet {
	ld.mx.Lock()
	defer ld.mx.Unlock()
	var orderedPackets []gossip.Packet
	ld.orderedData.orderedGossip.Each(func(key interface{}, value interface{}) {
		orderedPackets = append(orderedPackets, *value.(*gossip.Packet))
	})
	return orderedPackets
}
func (ld *localData) GetPacketAvailableAt(id string) []string {
	ld.mx.Lock()
	defer ld.mx.Unlock()
	if gP := ld.GetPacket(id); gP != nil {
		return gP.AvailableAt
	}
	return nil
}
func (ld *localData) MissingPacket() <-chan string {
	return ld.missingPackets
}

// ApplyOrder saves ids in provided order
// if present in tmp, add data to ordered, else adds id (without data) to ordered
// and sends to the missing data's id to missingPackets chan
func (ld *localData) ApplyOrder(order []string) {
	ld.mx.Lock()
	prv := ld.orderedData
	ld.orderedData.orderedGossip = linkedhashmap.New()
	for _, id := range order {
		ld.orderedData.addId(id)
	}
	for _, id := range order {
		if gP := ld.tmpGossip.Get(id); gP != nil { // if saved in tmp
			ld.orderedData.updatePacket(gP) // {
		}
		if gP := prv.getPacket(id); gP != nil { // if previously ordered
			ld.orderedData.updatePacket(gP) // {
		}
		if ld.orderedData.getPacket(id) == nil {
			go func(id string) {
				ld.missingPackets <- id
			}(id)
		}
	}
	ld.mx.Unlock()
}

func (ld *localData) SaveOrderedPacket(gP gossip.Packet) bool {
	ld.mx.Lock()
	defer ld.mx.Unlock()
	if ld.orderedData.checkIdPreviouslyAdded(gP.GetId()) {
		ld.orderedData.updatePacket(&gP)
		ld.tmpGossip.Remove(gP.GetId())
		return true
	}
	fmt.Println("Not added previously", gP.GetId())
	return false
}

func (ld *localData) SaveUnorderedPacket(gP gossip.Packet) {
	ld.mx.Lock()
	defer ld.mx.Unlock()
	ld.tmpGossip.Add(gP)
}

func InitData() Data {
	ld := &localData{
		mx:             sync.Mutex{},
		tmpGossip:      tmpData{},
		orderedData:    orderedData{},
		missingPackets: make(chan string),
	}
	ld.orderedData.orderedGossip = linkedhashmap.New()
	ld.tmpGossip.data = btree.NewWithStringComparator(8)

	return ld
}
