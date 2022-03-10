package _package

import (
	"sync"
	"time"

	"github.com/Ishan27g/go-utils/mLogger"
	"github.com/emirpasic/gods/trees/avltree"
	"github.com/gin-gonic/gin"
	"github.com/hashicorp/go-hclog"
	"github.com/jedib0t/go-pretty/v6/table"
)

func Clear() {
	if reg != nil {
		reg.clear()
	}
}

type MetaData interface{}
type peer RegisterRequest
type peers map[string]*peer // peer-address : peer
func (ps *peers) getPeers() []peer {
	var p []peer
	for _, p2 := range *ps {
		p = append(p, *p2)
	}
	return p
}

type registry struct {
	lock         sync.Mutex
	zones        *avltree.Tree // zoneId : peers
	logger       hclog.Logger
	serverEngine *gin.Engine
}

func (r *registry) clear() {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.zones.Clear()
}
func (r *registry) getPeers(zone int) []peer {
	r.lock.Lock()
	defer r.lock.Unlock()
	peersI, found := r.zones.Get(zone)
	if !found {
		return nil
	}
	peerMap := peersI.(peers)
	return peerMap.getPeers()
}
func (r *registry) getPeerMap(zone int) peers {
	peersI, found := r.zones.Get(zone)
	if found {
		return peersI.(peers)
	}
	return nil
}

// removePeer removes peers from the map
func (r *registry) removePeer(p peer) {
	r.lock.Lock()
	defer r.lock.Unlock()
	if peerMap := r.getPeerMap(p.Zone); peerMap != nil {
		peerMap[p.Address] = nil
		delete(peerMap, p.Address)
		if len(peerMap) == 0 {
			r.zones.Remove(p.Zone)
		} else {
			r.zones.Put(p.Zone, peerMap)
		}
		r.logger.Debug("Removed inactive peer - " + p.Address)
	}
}

// checkPeerExists returns true if peer is present in map
func (r *registry) checkPeerExists(p peer) bool {
	if peerMap := r.getPeerMap(p.Zone); peerMap != nil {
		return peerMap[p.Address] != nil
	}
	return false
}

// monitorPeer periodically pings the peer, removing it if unreachable
func (r *registry) monitorPeer(p peer) {
	for {
		<-time.After(5 * time.Second)
		if !r.checkPeerExists(p) {
			break // already deleted
		}
		if RegistryClient("").ping(p.Address) {
			continue
		}
		<-time.After(2 * time.Second) // try again
		if RegistryClient("").ping(p.Address) {
			continue
		}
		r.removePeer(p)
	}
}
func (r *registry) addPeer(p peer) bool {
	r.lock.Lock()
	defer r.lock.Unlock()
	added := false
	peerMap := r.getPeerMap(p.Zone)
	if peerMap == nil {
		ps := peers{
			p.Address: &p,
		}
		r.zones.Put(p.Zone, ps)
		added = true
	} else {
		if peerMap[p.Address] == nil { // new peer
			peerMap[p.Address] = &p
			added = true
		} else { // existing peer
			existingEntryForPeer := peerMap[p.Address]
			if existingEntryForPeer.RegisterAt.Before(p.RegisterAt) {
				peerMap[p.Address] = &p
				added = true
			}
		}
	}
	if added {
		go r.monitorPeer(p)
	}
	return added
}
func (r *registry) zoneIds() []int {
	r.lock.Lock()
	defer r.lock.Unlock()
	var zoneIds []int
	for _, i2 := range r.zones.Keys() {
		zoneIds = append(zoneIds, i2.(int))
	}
	return zoneIds
}
func (r *registry) allDetails(tbl bool) interface{} {
	r.lock.Lock()
	defer r.lock.Unlock()
	if !tbl {
		t := table.NewWriter()
		//t.SetOutputMirror(os.Stdout)
		t.SetStyle(table.StyleLight)
		t.Style().Options.DrawBorder = false
		t.AppendHeader(table.Row{"Zone", "Peer Address", "Registered At", "Meta"})

		for it := r.zones.Iterator(); it.Next(); {
			p := it.Value().(peers)
			var logs []table.Row
			for _, r := range p {
				logs = append(logs, table.Row{r.Zone, r.Address, r.RegisterAt.String(), r.MetaData})
			}
			t.AppendRows(logs)
			t.AppendSeparator()
		}
		t.AppendSeparator()
		return t.Render()
	} else {
		allPeers := make(map[int]peers) // zone : []peers
		z := r.zones.Keys()
		for _, zone := range z {
			zoneId := zone.(int)
			p, _ := r.zones.Get(zoneId)
			allPeers[zoneId] = p.(peers)
		}
		return allPeers
	}

}

var reg *registry

func Setup() *registry {
	mLogger.Apply(mLogger.Level(hclog.Error), mLogger.Color(true))
	reg = &registry{
		lock:   sync.Mutex{},
		zones:  avltree.NewWithIntComparator(),
		logger: mLogger.New("registry"),
	}
	return reg
}
