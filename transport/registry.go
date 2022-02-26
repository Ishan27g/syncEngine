package transport

import (
	"strings"

	gossip "github.com/Ishan27g/gossipProtocol"
	"github.com/Ishan27g/syncEngine/peer"

	_package "github.com/Ishan27gOrg/registry/golang/registry/package"
)

var RegistryUrl string

func DiscoverRaftLeaders(exceptZone int) *[]peer.Peer {
	var leaders []peer.Peer
	zones := _package.RegistryClient(RegistryUrl).GetZoneIds()
	for _, otherZone := range zones {
		if otherZone != exceptZone {
			peers := _package.RegistryClient(RegistryUrl).GetZonePeers(otherZone)
			for _, zonePeer := range peers {
				l := peer.PeerFromMeta(zonePeer.MetaData.(map[string]interface{}))
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
		e := peer.PeerFromMeta(p.MetaData.(map[string]interface{}))
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

// RandomGossipPeers calls the registry and returns udp address of at most 4 random peers
func RandomGossipPeers(exclude string) []gossip.Peer {
	allPeers := getAllPeersFromRegistry()
	var peers []gossip.Peer
	for _, peer := range allPeers {
		peers = append(peers, convertToGossipPeer(peer, exclude)...)
	}
	return peers
}
