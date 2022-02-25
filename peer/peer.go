package peer

import (
	"fmt"
	"strings"

	_package "github.com/Ishan27gOrg/registry/golang/registry/package"
)

const (
	LEADER    = iota + 1 // 2
	CANDIDATE            // 3
	FOLLOWER             // 4
)

// Peer identifies a peer/node in the network
type Peer struct {
	Zone     int    `json:"Zone"`
	HostName string `json:"HostName"`
	HttpPort string `json:"HttpPort"`
	GrpcPort string `json:"GrpcPort"`
	UdpPort  string `json:"UdpPort"`

	Mode int `json:"Mode"`
	Term int `json:"Term"`
}

func (p *Peer) GrpcAddr() string {
	host := strings.TrimPrefix(p.HostName, "https://")
	host = strings.TrimPrefix(host, "http://")
	return host + p.GrpcPort
}
func (p *Peer) UdpAddr() string {
	host := strings.TrimPrefix(p.HostName, "https://")
	host = strings.TrimPrefix(host, "http://")
	return host + ":" + p.UdpPort
}
func (p *Peer) HttpAddr() string {
	host := "http://" + p.HostName
	return host + p.HttpPort
}

func (p *Peer) Details() string {
	return fmt.Sprintf("[Zone-%d]-%s [HTTP %s][GRPC %s][UDP %s][%s-Term-%d]",
		p.Zone, p.HostName, p.HttpPort, p.GrpcPort, p.UdpPort, asString(p.Mode), p.Term)
}

func PeerFromMeta(data map[string]interface{}) Peer {
	z := data["Zone"].(float64)
	s := data["Mode"].(float64)
	t := data["Term"].(float64)
	return Peer{
		Zone:     int(z),
		HostName: data["HostName"].(string),
		HttpPort: data["HttpPort"].(string),
		GrpcPort: data["GrpcPort"].(string),
		UdpPort:  data["UdpPort"].(string),
		Mode:     int(s),
		Term:     int(t),
	}
}

func asString(state int) string {
	switch state {
	case LEADER:
		return "LEADER"
	case CANDIDATE:
		return "CANDIDATE"
	default:
		return "FOLLOWER"
	}
}

func FromEnv(envFile string) (Peer, string) {
	e := InitEnv(envFile)
	return e.Self, e.RegistryAddr
}

func TransformRegistryResponse(zonePeers _package.PeerResponse, self Peer) *[]Peer {
	var raftPeers = new([]Peer)
	peerMeta := zonePeers.GetPeerMeta()
	for _, pe := range peerMeta {
		pi := pe.(map[string]interface{})
		p := PeerFromMeta(pi)
		if p.HttpAddr() == self.HttpAddr() {
			continue
		}
		*raftPeers = append(*raftPeers, p)
	}
	return raftPeers
}
