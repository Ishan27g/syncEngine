package data

import (
	"sync"

	"github.com/Ishan27g/syncEngine/peer"
)

type SyncMap struct {
	s sync.Map
}

func NewMap() SyncMap {
	return SyncMap{
		s: sync.Map{},
	}
}
func (sm *SyncMap) Add(id string, val *peer.State) {
	sm.s.Store(id, val)
}
func (sm *SyncMap) Get(id string) *peer.State {
	if s, f := sm.s.Load(id); f {
		return s.(*peer.State)
	}

	return nil
}
func (sm *SyncMap) All() map[string]*peer.State {
	var m = make(map[string]*peer.State)
	sm.s.Range(func(key, value interface{}) bool {
		m[key.(string)] = value.(*peer.State)
		return true
	})
	return m
}
