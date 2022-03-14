package gossipProtocol

type Peer struct {
	UdpAddress        string
	ProcessIdentifier string
	Hop               int
}
