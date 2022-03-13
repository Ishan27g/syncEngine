package vClock

import (
	"fmt"
	"sync"
)

var processClocks peerClock
var once sync.Once

// VectorClock provides interface to implement vector clock
type VectorClock interface {
	// Get returns the current vector clock
	Get(eventIdOrHash string) EventClock
	// SendEvent returns the current vector clock after first updating self clock and then
	// updating the individual clocks for corresponding addresses
	SendEvent(eventIdOrHash string, addresses []string) EventClock
	// ReceiveEvent updates the current vector clock using element wise maximum with the passed vector clock
	ReceiveEvent(eventIdOrHash string, v EventClock)
	Clear(eventIdOrHash string)
	get() (string, map[string]EventClock)
	print()
}

// EventClock is vector-clock of peer-address and its individual clock
type EventClock map[string]int

func (v *EventClock) mergeWith(v2 EventClock) *EventClock {
	return MergeClocks(*v, v2)
}

type vClock struct {
	lock        sync.Mutex
	self        string
	vectorClock map[string]EventClock // clock per event
}

func (v *vClock) get() (string, map[string]EventClock) {
	return v.self, v.vectorClock
}

type peerClock map[string]*int

var peerLock = sync.Mutex{}

func (p peerClock) add(peer string) bool {
	peerLock.Lock()
	defer peerLock.Unlock()
	if p.get(peer) == -1 {
		p[peer] = new(int)
		*p[peer] = 0
		return true
	}
	return false
}
func (p peerClock) update(peer string) {
	if p.add(peer) {
		p.updateTo(peer, p.get(peer)+1)
	}
}
func (p peerClock) get(peer string) int {
	if p[peer] == nil {
		return -1
	}
	return *p[peer]
}

func (p peerClock) updateTo(address string, clock int) {
	peerLock.Lock()
	defer peerLock.Unlock()
	*p[address] = clock
}
func (v *vClock) Clear(eventIdOrHash string) {
	v.lock.Lock()
	v.vectorClock[eventIdOrHash] = make(EventClock)
	v.initClock(eventIdOrHash, v.self)
	v.lock.Unlock()
}

func (v *vClock) print() {
	for a, c := range v.vectorClock {
		fmt.Println(a, c)
	}
}

// Get returns the current vector clock
func (v *vClock) Get(eventIdOrHash string) EventClock {
	v.lock.Lock()
	defer v.lock.Unlock()
	return v.vectorClock[eventIdOrHash]
}

// SendEvent returns the current vector clock after updating the individual clocks for these entries
func (v *vClock) SendEvent(eventIdOrHash string, address []string) EventClock {
	v.lock.Lock()
	// update the individual clock entry for self
	v.event(eventIdOrHash, v.self)
	for _, a := range address {
		v.event(eventIdOrHash, a)
	}
	v.lock.Unlock()
	return v.Get(eventIdOrHash)
}

// ReceiveEvent updates the current vector clock using element wise maximum with the passed vector clock
func (v *vClock) ReceiveEvent(eventIdOrHash string, v1 EventClock) {
	v.lock.Lock()
	defer v.lock.Unlock()
	// update local clock
	v.event(eventIdOrHash, v.self)
	if v.vectorClock[eventIdOrHash] == nil {
		v.vectorClock[eventIdOrHash] = make(EventClock)
	}
	// merge with received clock
	for address, newClock := range v1 {
		if address == v.self { // todo revert ???
			v.vectorClock[eventIdOrHash][address] = newClock
		}
		if processClocks.get(address) != -1 {
			v.updateClock(eventIdOrHash, address, newClock)
		} else { // if new address
			processClocks.add(address)
			v.updateClock(eventIdOrHash, address, newClock)
		}
		processClocks.updateTo(address, newClock)
	}
}

// Event updates the individual clock entry for this entry
func (v *vClock) event(eventIdOrHash, address string) {
	processClocks.update(address)
	if v.vectorClock[eventIdOrHash] == nil {
		v.initClock(eventIdOrHash, address)
	}
	v.vectorClock[eventIdOrHash][address] = processClocks.get(address)
}

// updateClock updates the individual clock if it is lower than the new clock
func (v *vClock) updateClock(eventIdOrHash, address string, newClock int) {
	if v.vectorClock[eventIdOrHash][address] < newClock {
		v.vectorClock[eventIdOrHash][address] = newClock
	}
}

func (v *vClock) initClock(event, peer string) {
	v.vectorClock[event] = EventClock{
		peer: 0,
	}
	if processClocks.get(peer) != -1 {
		v.vectorClock[event][peer] = processClocks.get(peer)
	}

}
func Init(self string) VectorClock {
	once.Do(func() {
		processClocks = make(map[string]*int)
	})
	v := vClock{
		lock:        sync.Mutex{},
		vectorClock: make(map[string]EventClock),
		self:        self,
	}
	return &v
}

func Copy(v VectorClock) VectorClock {
	self, clocks := v.get()
	var copy = make(map[string]EventClock)
	for id, clock := range clocks {
		copy[id] = clock
	}
	return &vClock{
		lock:        sync.Mutex{},
		vectorClock: copy,
		self:        self,
	}
}
