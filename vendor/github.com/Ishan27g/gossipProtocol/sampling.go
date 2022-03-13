package gossipProtocol

import (
	"context"
	"math/rand"
	"time"

	sll "github.com/emirpasic/gods/lists/singlylinkedlist"
)

type iSampling interface {
	SetInitialPeers(...Peer)
	Start()
	AddPeer(...Peer)
	GetPeer(exclude Peer) Peer
	ViewFromPeer(View, Peer) []byte
	Size() int
	removePeer(peer Peer)
	printView() string
	getView() View
}
type sampling struct {
	ctx            context.Context
	cancel         context.CancelFunc
	strategy       PeerSamplingStrategy
	view           View
	selfDescriptor Peer
	knownPeers     map[string]Peer
	udpClient      client

	previousPeer Peer
}

func (s *sampling) getView() View {
	return s.view
}
func (s *sampling) printView() string {
	return PrintView(s.view)
}
func (s *sampling) Size() int {
	return s.view.Nodes.Size()
}
func (s *sampling) Start() {
	go s.passive()
}
func (s *sampling) SetInitialPeers(initialPeers ...Peer) {
	s.fillView(initialPeers...)
	s.selectView(&s.view)
}

// fillView fills the current view as these peers
func (s *sampling) fillView(peers ...Peer) {
	for _, peer := range peers {
		s.addPeerToView(peer)
	}
	s.view.sortNodes()
}

func (s *sampling) addPeerToView(peer Peer) {
	if s.knownPeers[peer.ProcessIdentifier].ProcessIdentifier == "" {
		s.view.add(peer)
		s.knownPeers[peer.ProcessIdentifier] = peer
	}
}

func (s *sampling) removePeer(peer Peer) {
	s.view.remove(peer)
	delete(s.knownPeers, peer.ProcessIdentifier)
}
func (s *sampling) selectView(view *View) {
	switch s.strategy.ViewSelectionStrategy {
	case Random: // select random MaxNodesInView nodes
		view.RandomView()
	case Head: // select first MaxNodesInView nodes
		view.headView()
	case Tail: // select last MaxNodesInView nodes
		view.tailView()
	}

	s.view = mergeViewExcludeNode(*view, *view, s.selfDescriptor)
}

func (s *sampling) passive() {
	wait := ViewExchangeDelay
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-time.After((wait) + time.Duration(rand.Intn(int(wait.Milliseconds()/2)))): // add random delay

			receivedView := new(View)
			nwPeer := s.getPeer()
			if nwPeer.UdpAddress == "" {
				continue
			}
			switch s.strategy.ViewPropagationStrategy {
			case Push, PushPull:
				mergedView := MergeView(s.view, selfDescriptor(s.selfDescriptor))
				buffer := ViewToBytes(mergedView, s.knownPeers[s.selfDescriptor.ProcessIdentifier])
				rspView, from, err := BytesToView(s.udpClient.send(nwPeer.UdpAddress, buffer))
				if err == nil {
					s.knownPeers[from.ProcessIdentifier] = from
					_ = &rspView // for PUSH -> []byte("OKAY")
				} else {
					s.removePeer(nwPeer)
				}
			default:
				// send emptyView to nwPeer to trigger response
				rspView, from, err := BytesToView(s.udpClient.send(nwPeer.UdpAddress,
					ViewToBytes(View{Nodes: sll.New()}, s.knownPeers[s.selfDescriptor.ProcessIdentifier])))
				if err == nil {
					s.knownPeers[from.ProcessIdentifier] = from
					receivedView = &rspView
				} else {
					s.removePeer(nwPeer)
				}
			}
			switch s.strategy.ViewPropagationStrategy {
			case PushPull, Pull:
				if receivedView.Nodes != nil {
					increaseHopCount(receivedView)
					mergedView := mergeViewExcludeNode(s.view, *receivedView, s.selfDescriptor)
					s.selectView(&mergedView)
				}
			}
		}
	}
}

func (s *sampling) ViewFromPeer(receivedView View, peer Peer) []byte {
	s.knownPeers[peer.ProcessIdentifier] = peer
	var rsp []byte
	increaseHopCount(&receivedView)
	if s.strategy.ViewPropagationStrategy == Pull || s.strategy.ViewPropagationStrategy == PushPull {
		mergedView := MergeView(s.view, selfDescriptor(s.selfDescriptor))
		rsp = ViewToBytes(mergedView, s.knownPeers[s.selfDescriptor.ProcessIdentifier])
	}

	merged := mergeViewExcludeNode(s.view, receivedView, s.selfDescriptor)
	s.selectView(&merged)
	return rsp // empty incase of PUSH
}

func (s *sampling) AddPeer(peer ...Peer) {
	for _, p := range peer {
		//	if p.ProcessIdentifier != s.selfDescriptor.ProcessIdentifier {
		s.addPeerToView(p)
		//	}
	}
	s.selectView(&s.view)
}

// GetPeer returns a peer from the current view except self
func (s *sampling) GetPeer(exclude Peer) Peer {
	rand.Seed(rand.Int63n(100000))

	node := Peer{}
	goto selectPeer
selectPeer:
	{
		node = s.view.randomNode()
	}
	if s.Size() == 2 && node.ProcessIdentifier == exclude.ProcessIdentifier {
		return Peer{}
	}
	if node.ProcessIdentifier == s.selfDescriptor.ProcessIdentifier {
		goto selectPeer
	}
	if s.previousPeer.ProcessIdentifier == node.ProcessIdentifier && s.Size() != 1 {
		goto selectPeer
	}
	if exclude.ProcessIdentifier == node.ProcessIdentifier && s.Size() != 1 {
		goto selectPeer
	}

	s.previousPeer = node
	return s.previousPeer
}

// getPeer returns a peer from the current view based on applied strategy
func (s *sampling) getPeer() Peer {
	node := Peer{}
	switch s.strategy.PeerSelectionStrategy {
	case Random: // select random peer
		node = s.view.randomNode()
	case Head: // select peer with the lowest hop
		node = s.view.headNode()
	case Tail: // select peer with the highest hop
		node = s.view.tailNode()
	}
	// println(s.selfDescriptor.ProcessIdentifier, " - SELECTED PEER ", node.UdpAddress)
	return Peer(node)
}

func initSampling(udpAddress string, identifier string, strategy PeerSamplingStrategy) iSampling {
	ctx, cancel := context.WithCancel(context.Background())
	s := sampling{
		ctx:      ctx,
		cancel:   cancel,
		strategy: strategy,
		view: View{
			Nodes: sll.New(),
		},
		selfDescriptor: Peer{
			UdpAddress:        udpAddress,
			Hop:               0,
			ProcessIdentifier: identifier,
		},
		knownPeers:   make(map[string]Peer),
		udpClient:    getClient(identifier),
		previousPeer: Peer{},
	}
	s.knownPeers[s.selfDescriptor.ProcessIdentifier] = Peer(s.selfDescriptor)
	return &s
}
