package peer

import (
	"fmt"
	"strings"

	_package "github.com/Ishan27g/registry/golang/registry/package"
)

const (
	LEADER    = "LEADER"
	CANDIDATE = "CANDIDATE"
	FOLLOWER  = "FOLLOWER"
)

// Peer identifies a peer/node in the network
type Peer struct {
	FakeName string `json:"FakeName"`

	Zone     int    `json:"Zone"`
	HostName string `json:"HostName"`
	HttpPort string `json:"HttpPort"`
	GrpcPort string `json:"GrpcPort"`
	UdpPort  string `json:"UdpPort"`

	Mode     string `json:"Mode"`
	RaftTerm int    `json:"RaftTerm"`
	SyncTerm int    `json:"SyncTerm"`
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
	return fmt.Sprintf("%s[Zone-%d]-%s [HTTP %s][GRPC %s][UDP %s][%s] [RaftTerm-%d] [SyncTerm-%d]",
		p.FakeName, p.Zone, p.HostName, p.HttpPort, p.GrpcPort, p.UdpPort, p.Mode, p.RaftTerm, p.SyncTerm)
}

func FromMeta(data map[string]interface{}) Peer {
	z := data["Zone"].(float64)
	r := data["RaftTerm"].(float64)
	s := data["SyncTerm"].(float64)
	return Peer{
		FakeName: data["FakeName"].(string),
		Zone:     int(z),
		HostName: data["HostName"].(string),
		HttpPort: data["HttpPort"].(string),
		GrpcPort: data["GrpcPort"].(string),
		UdpPort:  data["UdpPort"].(string),
		Mode:     data["Mode"].(string),
		RaftTerm: int(r),
		SyncTerm: int(s),
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
		p := FromMeta(pi)
		if p.HttpAddr() == self.HttpAddr() {
			continue
		}
		*raftPeers = append(*raftPeers, p)
	}
	return raftPeers
}
