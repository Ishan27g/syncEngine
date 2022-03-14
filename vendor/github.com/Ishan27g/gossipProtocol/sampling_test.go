package gossipProtocol

import (
	"context"
	"testing"
	"time"

	"github.com/go-playground/assert"
)

var mockGossipCb = func(gp Packet, p Peer) []byte {
	return []byte("")
}

type args struct {
	self         Peer
	initialPeers []Peer
	sampling     iSampling
	cancel       context.CancelFunc
}

var setupPeer = func(base string, i int, strategy PeerSamplingStrategy, numProcesses int) args {
	self := network(base, -1, numProcesses)[i] // all peers, this index
	peers := network(base, i, numProcesses)    // all peers except this index
	s := initSampling(self.UdpAddress, self.ProcessIdentifier, strategy)
	s.SetInitialPeers(peers...)
	ctx, cancel := context.WithCancel(context.Background())
	a := args{
		self:         self,
		initialPeers: peers,
		sampling:     s,
		cancel:       cancel,
	}
	s.Start()
	go Listen(ctx, trimHost(self.UdpAddress), mockGossipCb, s.ViewFromPeer)
	return a
}

func setupProcesses(base string, strategy PeerSamplingStrategy) []args {
	var processes []args
	var numProcesses = 4
	for i := 0; i < numProcesses; i++ {
		processes = append(processes, setupPeer(base, i, strategy, numProcesses))
	}
	return processes
}
func Test_Sampling_View(t *testing.T) {
	t.Run("Current View does not contain self", func(t *testing.T) {
		t.Parallel()
		processes := setupProcesses("50", defaultStrategy)

		<-time.After(6 * time.Second)
		for _, p := range processes { // check self does not exist in view
			view := p.sampling.getView()
			view.Nodes.Each(func(index int, value interface{}) {
				n := value.(Peer)
				assert.NotEqual(t, p.self.UdpAddress, n.UdpAddress)
			})
		}
	})

}
