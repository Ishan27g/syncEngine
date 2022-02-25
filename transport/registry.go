package transport

import (
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
