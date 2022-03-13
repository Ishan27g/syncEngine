package gossipProtocol

import (
	"strconv"
	"testing"

	sll "github.com/emirpasic/gods/lists/singlylinkedlist"
	"github.com/stretchr/testify/assert"
)

func mockView(hop int) View {
	n := sll.New()
	for i := 1; i <= 9; i++ {
		n.Add(Peer{
			UdpAddress:        "120" + strconv.Itoa(i),
			Hop:               hop,
			ProcessIdentifier: "process-" + strconv.Itoa(i),
		})
	}
	return View{Nodes: n}
}
func TestMerge(t *testing.T) {
	t.Parallel()

	lowerHop, higherHop := 0, 2
	v1 := mockView(lowerHop)
	v2 := mockView(higherHop)

	merged := mergeViews(toMap(v1, ""), toMap(v2, ""))

	assert.Equal(t, merged.Nodes.Size(), v1.Nodes.Size())
	merged.Nodes.Each(func(_ int, value interface{}) {
		n := value.(Peer)
		assert.Equal(t, n.Hop, lowerHop)
	})
}

func TestViewNodes(t *testing.T) {
	t.Parallel()
	v1 := mockView(0)
	assert.Equal(t, v1.headNode().UdpAddress, "1201")
	assert.Equal(t, v1.tailNode().UdpAddress, "1209")
	assert.NotNil(t, v1.randomNode())
}
func TestRandomView(t *testing.T) {
	t.Parallel()

	v := mockView(0)
	v.RandomView()
	assert.NotNil(t, v.randomNode())
	assert.Equal(t, MaxNodesInView, v.Nodes.Size())
}
func TestHeadView(t *testing.T) {
	t.Parallel()

	v := mockView(0)
	v.headView()
	assert.Equal(t, MaxNodesInView, v.Nodes.Size())
	assert.Equal(t, "1201", v.headNode().UdpAddress)
	assert.Equal(t, "1206", v.tailNode().UdpAddress)
	exists, hop, index := v.checkExists("1205")
	assert.Equal(t, true, exists)
	assert.Equal(t, 0, hop)
	assert.Equal(t, 4, index)
}
func TestTailView(t *testing.T) {
	t.Parallel()

	v := mockView(0)
	v.tailView()
	v.sortByAddr()
	assert.Equal(t, MaxNodesInView, v.Nodes.Size())
	assert.Equal(t, "1204", v.headNode().UdpAddress)
	assert.Equal(t, "1209", v.tailNode().UdpAddress)
	exists, hop, index := v.checkExists("1205")
	assert.Equal(t, true, exists)
	assert.Equal(t, 0, hop)
	assert.Equal(t, 1, index)
}

func TestSerialization(t *testing.T) {
	t.Parallel()

	v := mockView(4)
	peer := Peer{
		UdpAddress:        "udp",
		ProcessIdentifier: "id",
	}
	bytes := ViewToBytes(v, peer)
	v2, from, e := BytesToView(bytes)
	assert.NoError(t, e)
	v2.Nodes.Each(func(_ int, value interface{}) {
		n := value.(Peer)
		assert.NotEqual(t, -1, v.Nodes.IndexOf(n))
	})
	assert.Equal(t, peer, from)
}
