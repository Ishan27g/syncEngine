package gossipProtocol

/*
	Partial View & strategies based on http://lpdwww.epfl.ch/upload/documents/publications/neg--1184036295all.pdf
*/
import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"strings"

	"github.com/emirpasic/gods/containers"
	sll "github.com/emirpasic/gods/lists/singlylinkedlist"
	"github.com/emirpasic/gods/utils"
)

// View of at max MaxNodesInView nodes in the network
// updated during selectView()
type View struct {
	Nodes *sll.List
}

// increaseHopCount for each node in the View
func increaseHopCount(v *View) {
	nodes := sll.New()
	for it := v.Nodes.Iterator(); it.Next(); {
		n1 := it.Value().(Peer)
		n1.Hop++
		nodes.Add(n1)
	}
	v.Nodes.Clear()
	v.Nodes = nodes
	v.sortNodes()
}

func selfDescriptor(n Peer) View {
	return View{Nodes: sll.New(n)}
}
func (v *View) add(peer Peer) {
	v.Nodes.Add(peer)
	v.sortByAddr()
}
func (v *View) remove(peer Peer) *View {
	exists, _, index := v.checkExists(peer.UdpAddress)
	if exists && index != -1 {
		v.Nodes.Remove(index)
	}
	return v
}

// checkExists checks whether the udpaddress exists in the View
func (v *View) checkExists(udpaddress string) (bool, int, int) {
	index := 0
	for it := v.Nodes.Iterator(); it.Next(); {
		n1 := it.Value().(Peer)
		if n1.UdpAddress == udpaddress {
			return true, n1.Hop, index
		}
		index++
	}
	return false, -1, -1
}

// MergeView view2 into View 1, discarding duplicate nodes with higher Hop count
func mergeViewExcludeNode(view1, view2 View, n Peer) View {
	m1 := toMap(view1, n.UdpAddress)
	m2 := toMap(view2, n.UdpAddress)
	return mergeViews(m1, m2)
}

func MergeView(view1, view2 View) View {
	m1 := toMap(view1, "")
	m2 := toMap(view2, "")
	return mergeViews(m1, m2)
}

func toMap(view1 View, excludeAddr string) map[string]Peer {
	// convert View to hashmap
	v1map := make(map[string]Peer) //address:Peer
	for it := view1.Nodes.Iterator(); it.Next(); {
		n1 := it.Value().(Peer)
		if excludeAddr != "" {
			if strings.Compare(excludeAddr, n1.UdpAddress) != 0 {
				v1map[n1.UdpAddress] = n1
			}
		} else {
			v1map[n1.UdpAddress] = n1
		}
	}
	return v1map
}
func mergeViews(m1, m2 map[string]Peer) View {
	m3 := make(map[string]Peer)
	merged := View{Nodes: sll.New()}

	for addr, peer := range m1 {
		m2Peer, present := m2[addr]
		if present {
			// add lower hop
			if m2Peer.Hop > peer.Hop {
				m3[addr] = peer
			} else {
				m3[addr] = m2Peer
			}
		} else {
			merged.Nodes.Add(peer)
		}
	}
	for addr, peer := range m2 {
		m1Peer, present := m1[addr]
		if present {
			// add lower hop
			if m1Peer.Hop > peer.Hop {
				m3[addr] = peer
			} else {
				m3[addr] = m1Peer
			}
		} else {
			merged.Nodes.Add(peer)
		}
	}
	for _, peer := range m3 {
		merged.add(peer)
	}
	merged.sortNodes()
	return merged
}

// sortNodes according to process identifier
func (v *View) sortByAddr() {
	c := utils.Comparator(func(a, b interface{}) int {
		n1 := a.(Peer)
		n2 := b.(Peer)
		return strings.Compare(n1.UdpAddress, n2.UdpAddress)
	})
	sortedNodes := containers.GetSortedValues(v.Nodes, c)
	v.Nodes.Clear()
	for _, va := range sortedNodes {
		n := va.(Peer)
		v.Nodes.Add(n)
	}
}

// sortNodes according to increasing Hop count
func (v *View) sortNodes() {
	c := utils.Comparator(func(a, b interface{}) int {
		n1 := a.(Peer)
		n2 := b.(Peer)
		if n1.Hop > n2.Hop {
			return 1
		}
		if n1.Hop < n2.Hop {
			return -1
		}
		return 0
	})
	v.Nodes.Sort(c)
	v.sortByAddr()
}

// headNode returns the node with the lowest Hop count
func (v *View) headNode() Peer {
	node, _ := v.Nodes.Get(0)
	return node.(Peer)
}

// tailNode returns the node with the highest Hop count
func (v *View) tailNode() Peer {
	node, _ := v.Nodes.Get(v.Nodes.Size() - 1)
	return node.(Peer)
}

// randomNode returns a random node from the list
func (v *View) randomNode() Peer {
	if v.Nodes.Size() == 0 {
		return Peer{}
	}
	node, _ := v.Nodes.Get(rand.Intn(v.Nodes.Size()))
	return node.(Peer)
}

// RandomView sets the current View as a random subset of current View
func (v *View) RandomView() {
	if v.Nodes.Size() == 0 {
		return
	}
	selection := sll.New()
	for {
		node, _ := v.Nodes.Get(rand.Intn(v.Nodes.Size()))

		if selection.IndexOf(node) == -1 {
			selection.Add(node)
		}
		if selection.Size() == MaxNodesInView || selection.Size() == v.Nodes.Size() { // todo v.Nodes.Size() not need
			break
		}
	}
	v.Nodes.Clear()
	v.Nodes = selection
	v.sortNodes()
}

// headView sets the current View as the subset of first MaxNodesInView Nodes in the current View
func (v *View) headView() {
	selection := sll.New()
	max := MaxNodesInView
	if v.Nodes.Size() < MaxNodesInView {
		max = v.Nodes.Size()
	}
	for i := 0; i < max; i++ {
		node, _ := v.Nodes.Get(i)
		selection.Add(node)
	}
	v.Nodes.Clear()
	v.Nodes = selection
	v.sortNodes()
}

// tailView sets the current View as the subset of last MaxNodesInView Nodes in the current View
func (v *View) tailView() {
	selection := sll.New()
	min := v.Nodes.Size() - MaxNodesInView
	if v.Nodes.Size() < MaxNodesInView {
		min = 0
	}
	for i := v.Nodes.Size() - 1; i >= min; i-- {
		node, _ := v.Nodes.Get(i)
		selection.Add(node.(Peer))
	}
	v.Nodes.Clear()
	v.Nodes = selection
	v.sortNodes()
}

type data struct {
	View    map[string]*Peer
	PeerUdp string
	PeerId  string
}

func ViewToBytes(view View, from Peer) []byte {
	m := make(map[string]*Peer)
	if view.Nodes.Size() == 0 {
		m["emptyView"] = &Peer{}
	}
	for it := view.Nodes.Iterator(); it.Next(); {
		node := it.Value().(Peer)
		m[node.ProcessIdentifier] = &node
	}
	var data = data{
		View:    m,
		PeerUdp: from.UdpAddress,
		PeerId:  from.ProcessIdentifier,
	}
	b, err := json.Marshal(&data)
	if err != nil {
		fmt.Println(err.Error())
		return nil
	}
	return b
}
func BytesToView(bytes []byte) (View, Peer, error) {
	if bytes == nil {
		return View{}, Peer{}, errors.New("emptyView")
	}
	var data = data{}
	if err := json.Unmarshal(bytes, &data); err != nil {
		fmt.Println(err.Error())
		return View{}, Peer{}, err
	}
	v := View{Nodes: sll.New()}
	if data.View["emptyView"] != nil {
		return v, Peer{
			UdpAddress:        data.PeerUdp,
			ProcessIdentifier: data.PeerId,
		}, nil
	}
	for _, node := range data.View {
		v.Nodes.Add(*node)
	}
	return v, Peer{
		UdpAddress:        data.PeerUdp,
		ProcessIdentifier: data.PeerId,
	}, nil
}
func PrintView(view View) string {
	str := "\tView len - " + strconv.Itoa(view.Nodes.Size())
	view.Nodes.Each(func(_ int, value interface{}) {
		n := value.(Peer)
		str += "\n\t" + n.UdpAddress + "[" + strconv.Itoa(n.Hop) + "]" + " for - " + n.ProcessIdentifier
	})
	return str
}
