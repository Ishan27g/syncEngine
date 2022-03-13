package vClock

//
//import (
//	"math/rand"
//	"strconv"
//	"strings"
//	"sync"
//	"testing"
//	"time"
//
//	"github.com/stretchr/testify/assert"
//)
//
///*
//- Each peer node (follower) maintains a very partial view of the entire network. An Event received by a peer can be sent to a peer in another zone, which means that an Event is recorded at the other zone's leader.
//- A round lasts (T) time duration and events are a part of a round. This means, at the end of a round, each zone leader has a partial order of some events across all zones
//
//Every Round (R) :
//    Follower in zone-x
//        - exchanges events with other peers (across all zones) and maintains a partial order of events
//        - sends it sends to its leader. (leader requests at end of round)
//    Leader in zone-x :
//        - calculates order of events received from its followers for this round -> expected global order
//            (Concurrently)
//                - send this global order with round number to all zone leaders
//                    -> Merge each response order with expected order.
//                - receive global order for this round number from all leaders, send back current value of expected order.
//                    -> Merge each request order with expected order
//                A merge will only update the expected order if the response/request order includes a new Event. This is merged in order.
//        After sending to & receiving from all leaders
//               -> expected order is the final order of all global events for this rounds
//               -> send snapshot to raft-followers
//                        - If follower has eventId, persist the Event data to a file
//                        - If follower has not seen eventId, lookup eventMetadata and request it from a peer that has this data, then persist the Event data to a file.
//                        (similar to IPFS)
//*/
//const tClientsPerZone = 5
//const tZones = 3
//
//// type testZone map[string]peer
//type mockNetwork struct {
//	mockZones []testZone
//}
//type testZone struct {
//	zoneId int
//	peers  map[string]VectorClock
//}
//
//// mock peers for a zone
//func mockZonePeers(zone int) map[string]VectorClock {
//	peers := make(map[string]VectorClock)
//	for i := 0; i < tClientsPerZone; i++ {
//		addr := buildAddr(zone, i)
//		peers[addr] = Init(addr)
//	}
//	return peers
//}
//
//// build mock address for a peer belonging to this zone
//func buildAddr(zone int, i int) string {
//	port := (zone * 10) + 9000 + i
//	addr := "http://service-zone-" + strconv.Itoa(zone) + ":" + strconv.Itoa(port)
//	return addr
//}
//
//// setup peers across zones
//func setupNetwork() []testZone {
//	var t []testZone
//	for i := 1; i <= tZones; i++ {
//		t = append(t, testZone{
//			zoneId: i,
//			peers:  mockZonePeers(i),
//		})
//	}
//	return t
//}
//
//// getZoneId for an address
//func (mn *mockNetwork) getZoneId(address string) int {
//	for _, zone := range mn.mockZones {
//		for addr := range zone.peers {
//			if addr == address {
//				return zone.zoneId
//			}
//		}
//	}
//	return -1
//}
//
////allPeerNames across all zones
//func (mn *mockNetwork) allPeerNames() []string {
//	var addresses []string
//	for _, zone := range mn.mockZones {
//		for addr, _ := range zone.peers {
//			addresses = append(addresses, addr)
//		}
//	}
//	return addresses
//}
//
//// all peers and their clocks across all zones
//func (mn *mockNetwork) getPeersClocks(zone int) map[string]VectorClock {
//	peers := make(map[string]VectorClock)
//	for _, z := range mn.mockZones {
//		if zone == z.zoneId {
//			for addr, clock := range z.peers {
//				peers[addr] = clock
//			}
//		}
//	}
//	return peers
//}
//
//// getPeerClock returns clock for this peer
//func (mn *mockNetwork) getPeerClock(address string) VectorClock {
//	for _, zone := range mn.mockZones {
//		for addr, clock := range zone.peers {
//			if addr == address {
//				return clock
//			}
//		}
//	}
//	return nil
//}
//
///// User sends gossip -> 1 peer in zone X receives it. Sends to leader of X and N peers in M zones, and
///*
//- Random peer in this zone receives a user Event,
//- Peer selects 3 random peers across all zones
//- Peer updates clock for this sendEvent with these peers
//- Peer sends a `new-event` to zone-leader
//- Returns clock of the new Event received by a zone leader for this Event
//*/
//func (mn *mockNetwork) mockUserEventAtZone(zone int, eventId string) EventClock {
//	peerAddr := ""
//	zonePeers := mn.getPeersClocks(zone)
//	randomPeer := rand.Intn(len(zonePeers) - 1)
//	i := 0
//	for addr := range zonePeers {
//		if i == randomPeer {
//			peerAddr = addr
//		}
//		i++
//	}
//	allPeers := mn.allPeerNames()
//	randomGossipReceivers := make([]string, len(zonePeers))
//	i = 0
//	for {
//		randomPeer := rand.Intn(len(allPeers) - 1)
//		if strings.Compare(allPeers[randomPeer], peerAddr) != 0 {
//			randomGossipReceivers = append(randomGossipReceivers, allPeers[randomPeer])
//			i++
//		}
//		if i == 3 {
//			break
//		}
//	}
//	// fmt.Println(peerAddr + " received data [" + eventId + "] from user -> informing zone-leader ")
//	return mn.getPeerClock(peerAddr).SendEvent(eventId, randomGossipReceivers)
//}
//
//type leader struct {
//	addr   string
//	events Events
//}
//type evt struct {
//	eventId string
//	clock   EventClock
//}
//
//func TestSomeEventsAtSomeLeaders(t *testing.T) {
//	/*t.Cleanup(func() {*/
//
//	mn, zoneLeaders, leader1ReciveEvent, leader2ReciveEvent, leader3ReciveEvent, eventIds := setupRegistry()
//
//	// mock events in each zone. Some events are not gossiped to all zones.
//	// mocked events are sent to leader1 after a random timeout
//	go func() {
//		d := rand.Intn(10000)
//		<-time.After(time.Duration(d))
//		mn.mockGossip([]string{eventIds[0], eventIds[1]}, leader1ReciveEvent)
//	}()
//	// mocked events are sent to leader2 after a random timeout
//	go func() {
//		d := rand.Intn(10000)
//		<-time.After(time.Duration(d))
//		mn.mockGossip([]string{eventIds[2]}, leader2ReciveEvent)
//	}()
//	// mocked events are sent to leader3 after a random timeout
//	go func() {
//		d := rand.Intn(10000)
//		<-time.After(time.Duration(d))
//		mn.mockGossip([]string{eventIds[1], eventIds[0]}, leader3ReciveEvent)
//	}()
//
//	// receive the events at leader1
//	for i := range leader1ReciveEvent {
//		zoneLeaders[1].events.MergeEvent(Event{EventId: i.eventId, EventClock: i.clock})
//	}
//	// receive the events at leader2
//	for i := range leader2ReciveEvent {
//		zoneLeaders[2].events.MergeEvent(Event{EventId: i.eventId, EventClock: i.clock})
//	}
//	// receive the events at leader2
//	for i := range leader3ReciveEvent {
//		zoneLeaders[3].events.MergeEvent(Event{EventId: i.eventId, EventClock: i.clock})
//	}
//
//	// leaders exchange their snapshots
//	leader1Events := zoneLeaders[1].events
//	leader2Events := zoneLeaders[2].events
//	leader3Events := zoneLeaders[3].events
//	leader1expectedOrder := leader1Events.GetCurrentEvents()
//	leader2expectedOrder := leader2Events.GetCurrentEvents()
//	leader3expectedOrder := leader3Events.GetCurrentEvents()
//
//	var wg sync.WaitGroup
//	wg.Add(1)
//	go func() {
//		defer wg.Done()
//		// leader 1 sends to all leaders, other leaders merge with this
//		d := rand.Intn(5000)
//		<-time.After(time.Duration(d))
//		leader2Events.MergeEvents(leader1expectedOrder...)
//		leader3Events.MergeEvents(leader1expectedOrder...)
//	}()
//	wg.Add(1)
//	// leader 1 receives from all leaders, merges with this
//	go func() {
//		defer wg.Done()
//		d := rand.Intn(5000)
//		<-time.After(time.Duration(d))
//		leader1Events.MergeEvents(leader2Events.GetCurrentEvents()...)
//		leader1Events.MergeEvents(leader3Events.GetCurrentEvents()...)
//	}()
//	wg.Add(1)
//	// leader 2 sends to all leaders, other leaders merge with this
//	go func() {
//		defer wg.Done()
//		d := rand.Intn(5000)
//		<-time.After(time.Duration(d))
//		leader1Events.MergeEvents(leader2expectedOrder...)
//		leader3Events.MergeEvents(leader2expectedOrder...)
//	}()
//	wg.Add(1)
//	// leader 2 receives from all leaders, merges with this
//	go func() {
//		defer wg.Done()
//		d := rand.Intn(5000)
//		<-time.After(time.Duration(d))
//		leader2Events.MergeEvents(leader1Events.GetCurrentEvents()...)
//		leader2Events.MergeEvents(leader3Events.GetCurrentEvents()...)
//	}()
//	wg.Add(1)
//	// leader 3 sends to all leaders, other leaders merge with this
//	go func() {
//		defer wg.Done()
//		d := rand.Intn(5000)
//		<-time.After(time.Duration(d))
//		leader1Events.MergeEvents(leader3expectedOrder...)
//		leader2Events.MergeEvents(leader3expectedOrder...)
//	}()
//	wg.Add(1)
//	// leader 3 receives from all leaders, merges with this
//	go func() {
//		defer wg.Done()
//		d := rand.Intn(5000)
//		<-time.After(time.Duration(d))
//		leader3Events.MergeEvents(leader2Events.GetCurrentEvents()...)
//		leader3Events.MergeEvents(leader3Events.GetCurrentEvents()...)
//	}()
//	// MergeClocks(leader1expected)
//	wg.Wait()
//	// order of all events for all 3 leaders match
//	assert.Equal(t, leader1Events.GetEventsOrder(), leader2Events.GetEventsOrder())
//	assert.Equal(t, leader2Events.GetEventsOrder(), leader3Events.GetEventsOrder())
//
//	//b := leader1Events.GetCurrentEvents()
//	//for _, b2 := range b {
//	//	marshalJSON, err := b2.MarshalJSON()
//	//	assert.NoError(t, err)
//	//	var c = new(cloudevents.Event)
//	//	err = c.UnmarshalJSON(marshalJSON)
//	//	assert.NoError(t, err)
//	//}
//
//	//	})
//	// fmt.Println(leader1Events.GetEventsOrder())
//	// fmt.Println(leader2Events.GetEventsOrder())
//	// fmt.Println(leader3Events.GetEventsOrder())
//}
//func setupRegistry() (mockNetwork, map[int]leader, chan evt, chan evt, chan evt, []string) {
//	rand.Seed(time.Now().Unix())
//	mn := mockNetwork{
//		mockZones: setupNetwork(),
//	}
//	zoneLeaders := make(map[int]leader)
//	for i := 1; i <= tZones; i++ {
//		l := buildAddr(i, 1)
//		zoneLeaders[mn.getZoneId(l)] = leader{
//			addr:   l,
//			events: NewEventVector(),
//		}
//	}
//	// channel over which a zone leader receives `new-event` from its zone's followers
//	leader1ReciveEvent := make(chan evt, 3)
//	leader2ReciveEvent := make(chan evt, 3)
//	leader3ReciveEvent := make(chan evt, 3)
//
//	// eventIds of events created by user
//	eventIds := []string{"event-1-hash", "event-2-hash", "event-3-hash"}
//	return mn, zoneLeaders, leader1ReciveEvent, leader2ReciveEvent, leader3ReciveEvent, eventIds
//}
//
//// randomEvent creates a new Event at this zone after a random timeout
//func (mn *mockNetwork) randomEvent(zoneId int, eventId string) evt {
//	//rand.Seed(time.Now().Unix())
//	return evt{
//		eventId: eventId,
//		clock:   mn.mockUserEventAtZone(zoneId, eventId),
//	}
//}
//
//// mockGossip mocks a `new-event` at the corresponding zone after a random timeout
//// it returns the Event to eventChan.
//func (mn *mockNetwork) mockGossip(eventIds []string, eventChan chan evt) {
//	wg := sync.WaitGroup{}
//	for i, id := range eventIds {
//		wg.Add(1)
//		go func(i int, id string) {
//			defer wg.Done()
//			d := rand.Intn(1000)
//			<-time.After(time.Duration(d))
//			eventChan <- mn.randomEvent(i+1, id)
//		}(i, id)
//	}
//	wg.Wait()
//	close(eventChan)
//}
//
//var added bool
//
//func randomSlice(of []string, size int) []string {
//	var result []string
//	rand.Seed(time.Now().Unix())
//	for {
//		r := rand.Intn(len(of))
//		added = false
//		for _, s := range result {
//			if s == of[r] {
//				added = true
//			}
//		}
//		if !added {
//			result = append(result, of[r])
//		}
//		if len(result) == size {
//			break
//		}
//	}
//	return result
//}
