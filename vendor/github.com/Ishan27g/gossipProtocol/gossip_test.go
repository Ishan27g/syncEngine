package gossipProtocol

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/stretchr/testify/assert"
)

type gArgs struct {
	self         Peer
	initialPeers []Peer
	gossip       Gossip
	rcvGossip    <-chan Packet
}

var Reset = "\033[0m"
var Purple = "\033[35m"

func (g *gArgs) printView() {
	tr := table.NewWriter()
	tr.SetOutputMirror(os.Stdout)
	tr.SetStyle(table.StyleBold)
	tr.Style().Options.DrawBorder = false
	tr.AppendHeader(table.Row{"View at " + g.self.ProcessIdentifier, "Peer hop"})

	var logs []table.Row
	for _, peer := range g.gossip.CurrentView() {
		logs = append(logs, table.Row{peer.ProcessIdentifier, peer.Hop})
	}
	tr.AppendRows(logs)
	tr.AppendSeparator()
	tr.Render()

	fmt.Println()
}
func setupGossipProcesses(base string, numProcesses int, withPeers bool) []gArgs {
	var processes = make(chan gArgs, numProcesses)
	var wg sync.WaitGroup

	for i := 0; i < numProcesses; i++ {
		wg.Add(1)
		go func(wg *sync.WaitGroup, processes chan gArgs, i int) {
			defer wg.Done()
			var peers []Peer
			self := network(base, -1, numProcesses)[i] // all peers, this index
			peers = network(base, i, numProcesses)     // all peers except this index
			gossip, rcvGossip := Config("localhost", self.UdpAddress, self.ProcessIdentifier)
			if withPeers {
				gossip.Join(peers...)
			}
			processes <- gArgs{
				self:         self,
				initialPeers: peers,
				gossip:       gossip,
				rcvGossip:    rcvGossip,
			}
		}(&wg, processes, i)
	}
	wg.Wait()
	close(processes)
	var pro []gArgs
	for p := range processes {
		pro = append(pro, p)
	}
	<-time.After(2 * time.Second)
	return pro
}
func matchGossip(wg *sync.WaitGroup, r <-chan Packet, data string) bool {
	defer wg.Done()
	g := <-r
	return g.GetData() == data
}
func Test_Peer_Join(t *testing.T) {
	var numProcesses = 4
	processes := setupGossipProcesses("30", numProcesses, false)
	for _, p := range processes {
		p.gossip.Add(p.initialPeers...)
		assert.Equal(t, numProcesses-1, len(p.gossip.CurrentView()))
	}
}
func Test_Peer_Gossip(t *testing.T) {
	var numProcesses = 2
	processes := setupGossipProcesses("30", numProcesses, false)

	<-time.After(2 * time.Second)
	for _, p := range processes {
		p.gossip.Add(p.initialPeers...)
		assert.Equal(t, numProcesses-1, len(p.gossip.CurrentView()))
	}

	var wg sync.WaitGroup
	for i, p := range processes {
		var data = "some data" + strconv.Itoa(i)
		p.gossip.SendGossip(data)
		wg.Add(1)
		go func(p gArgs, data string) {
			<-time.After(5 * time.Second)
			assert.True(t, matchGossip(&wg, p.rcvGossip, data))
		}(p, data)
	}
	wg.Wait()
}
func Test_Gossip(t *testing.T) {

	var data = "some data"
	var numProcesses = 5

	processes := setupGossipProcesses("40", numProcesses, true)

	t.Parallel()

	var wg sync.WaitGroup

	processes[0].gossip.SendGossip(data)
	<-time.After(1 * time.Second)
	for _, p := range processes {
		wg.Add(1)
		go func(p gArgs) {
			assert.True(t, matchGossip(&wg, p.rcvGossip, data))
		}(p)
	}

	processes[numProcesses-1].gossip.SendGossip(data + data)
	<-time.After(1 * time.Second)
	for _, p := range processes {
		wg.Add(1)
		go func(p gArgs) {
			assert.True(t, matchGossip(&wg, p.rcvGossip, data+data))
		}(p)
	}

	processes[4].gossip.SendGossip(data + data + data)
	<-time.After(1 * time.Second)
	for _, p := range processes {
		wg.Add(1)
		go func(p gArgs) {
			assert.True(t, matchGossip(&wg, p.rcvGossip, data+data+data))
		}(p)
	}
	wg.Wait()
	t.Cleanup(func() {
		<-time.After(ViewExchangeDelay * 2)
		for _, p := range processes {
			p.printView()
		}
		<-time.After(ViewExchangeDelay)
		for _, p := range processes {
			p.printView()
		}
	})
}

func Test_Bulk_Gossip(t *testing.T) {

	var numProcesses = 5
	var numMessages = 300

	processes := setupGossipProcesses("10", numProcesses, true)
	var wg sync.WaitGroup

	for i := 0; i < numMessages; i++ {
		rand.Seed(time.Now().Unix())
		var data = "data" + strconv.Itoa(i)
		r := rand.Intn(len(processes))
		processes[r].gossip.SendGossip(data)
		<-time.After(125 * time.Millisecond)
		for _, p := range processes {
			wg.Add(1)
			go func(p gArgs, data string) {
				if !matchGossip(&wg, p.rcvGossip, data) {
					t.Error("mismatch")
				}
			}(p, data)
		}
		fmt.Println(i)
	}
	wg.Wait()
}
