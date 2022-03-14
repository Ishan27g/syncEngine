package transport

import (
	"strings"
	"time"

	gossip "github.com/Ishan27g/gossipProtocol"

	_package "github.com/Ishan27g/registry/golang/registry/package"
	"github.com/Ishan27g/syncEngine/peer"
	"github.com/patrickmn/go-cache"
)

var RegistryUrl string
var nwPeersCache *cache.Cache

const (
	RefreshRegistryCache = 10 * time.Second
	Cache_Key            = "network-peers"
)

func init() {
	nwPeersCache = cache.New(RefreshRegistryCache, RefreshRegistryCache*3)
}
func DiscoverRaftLeaders(exceptZone int) *[]peer.Peer {
	var leaders []peer.Peer
	zones := _package.RegistryClient(RegistryUrl).GetZoneIds()
	for _, otherZone := range zones {
		if otherZone != exceptZone {
			peers := _package.RegistryClient(RegistryUrl).GetZonePeers(otherZone)
			for _, zonePeer := range peers {
				l := peer.FromMeta(zonePeer.MetaData.(map[string]interface{}))
				leaders = append(leaders, l)
				break
			}
		}
	}
	return &leaders
}

func Register(who peer.Peer) *[]peer.Peer {
	peers := _package.RegistryClient(RegistryUrl).Register(who.Zone, who.HttpAddr(), who)
	p := peer.TransformRegistryResponse(peers, who)
	return p
}

func getAllPeersFromRegistry() map[int]_package.PeerResponse {
	zones := _package.RegistryClient(RegistryUrl).GetZoneIds()
	allPeers := make(map[int]_package.PeerResponse)
	for _, zone := range zones {
		allPeers[zone] = _package.RegistryClient(RegistryUrl).GetZonePeers(zone)
	}
	return allPeers
}
func convertToGossipPeer(peers _package.PeerResponse, exclude string) []gossip.Peer {
	var allPeers []gossip.Peer
	for _, p := range peers {
		e := peer.FromMeta(p.MetaData.(map[string]interface{}))
		if exclude != "" && strings.Contains(exclude, e.UdpAddr()) {
			continue
		}
		allPeers = append(allPeers, gossip.Peer{
			UdpAddress:        e.UdpAddr(),
			ProcessIdentifier: e.HttpAddr(),
		})
	}
	return allPeers
}

// return nw from cache or registry
func getNw() map[int]_package.PeerResponse {
	var allPeers map[int]_package.PeerResponse
	if p, f := nwPeersCache.Get(Cache_Key); f {
		allPeers = p.(map[int]_package.PeerResponse)
	}
	allPeers = getAllPeersFromRegistry()
	nwPeersCache.Set(Cache_Key, allPeers, RefreshRegistryCache)
	return allPeers
}

// RandomGossipPeers calls the registry and returns udp address of at most 4 random peers
func RandomGossipPeers(exclude string) []gossip.Peer {
	var peers []gossip.Peer
	for _, peer := range getNw() {
		peers = append(peers, convertToGossipPeer(peer, exclude)...)
	}
	return peers
}

// GetNetwork queries all peers from cache which is updated from the registry every interval
func GetNetwork(self peer.Peer) []peer.Peer {
	var peers []peer.Peer
	for _, p := range getNw() {
		peers = append(peers, *peer.TransformRegistryResponse(p, self)...)
	}
	return peers
}
