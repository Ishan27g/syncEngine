package utils

import (
	"crypto/sha1"
	"encoding/json"
	"fmt"

	gossip "github.com/Ishan27g/gossipProtocol"

	"github.com/Ishan27g/syncEngine/proto"
	"github.com/Ishan27g/syncEngine/snapshot"
	"github.com/Ishan27g/vClock"
	registry "github.com/Ishan27gOrg/registry/golang/registry/package"
)

func PrintJson(js interface{}) string {
	data, err := json.MarshalIndent(js, "", " ")
	if err != nil {
		fmt.Println("error:", err)
	}
	return string(data)
}

func MockRegistry() {
	go func() {
		c := registry.Setup()
		registry.Run("9999", c)

	}()
}

func EventsToOrder(events []vClock.Event) *proto.Order {
	var order = new(proto.Order)
	for _, event := range events {
		ev := proto.Event{
			EventId:      event.EventId,
			ClockEntries: []*proto.VClock{},
		}
		for s, i2 := range event.EventClock {
			ev.ClockEntries = append(ev.ClockEntries, &proto.VClock{
				Address:    s,
				ClockCount: int32(i2),
			})
		}
		order.Events = append(order.Events, &ev)
	}
	return order
}

func OrderToEvents(order *proto.Order) []vClock.Event {
	var events []vClock.Event
	for _, event := range order.Events {
		e := vClock.Event{
			EventId:    event.EventId,
			EventClock: vClock.EventClock{},
		}
		for _, clock := range event.ClockEntries {
			e.EventClock[clock.Address] = int(clock.ClockCount)
		}
		events = append(events, e)
	}
	return events
}

func OrderToEntries(packets ...gossip.Packet) []snapshot.Entry {
	var entries []snapshot.Entry
	for _, packet := range packets {
		entries = append(entries, snapshot.NewEntry(
			packet.GossipMessage.CreatedAt, packet.GossipMessage.Data))
	}
	return entries
}
func DefaultHash(obj interface{}) string {
	h := sha1.New()
	h.Write([]byte(fmt.Sprintf("%v", obj)))
	return fmt.Sprintf("%x", h.Sum(nil))
}

func PacketToOrder(packet gossip.Packet) *proto.Order {
	var vClock []*proto.VClock
	for s, i := range packet.VectorClock {
		vClock = append(vClock, &proto.VClock{
			Address:    s,
			ClockCount: int32(i),
		})
	}
	g := &proto.Order{Events: []*proto.Event{}}
	var clock []*proto.VClock
	for s, i := range packet.VectorClock {
		clock = append(clock, &proto.VClock{
			Address:    s,
			ClockCount: int32(i),
		})
	}
	g.Events = append(g.Events, &proto.Event{
		EventId:      packet.GossipMessage.GossipMessageHash,
		ClockEntries: clock,
	})
	return g
}
