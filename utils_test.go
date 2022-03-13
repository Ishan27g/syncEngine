package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	registry "github.com/Ishan27g/registry/golang/registry/package"
	"github.com/Ishan27g/syncEngine/engine"
	"github.com/Ishan27g/syncEngine/peer"
	"github.com/Ishan27g/syncEngine/snapshot"
	"github.com/stretchr/testify/assert"
)

var EnvFile = "./.envFiles/"

func randomInt() time.Duration {
	return time.Duration(rand.Intn(500))
}

const (
	atLeaderOnly   = 1
	atFollowerOnly = 2
	atAny          = 3
)

var envFile = "./.envFiles/"
var envFiles = envMap{
	envFile + "1.leader.env": []string{
		envFile + "1.follower.A.env",
		envFile + "1.follower.B.env",
		envFile + "1.follower.C.env",
		envFile + "1.follower.D.env",
		envFile + "1.follower.E.env",
	},
	envFile + "2.leader.env": []string{
		envFile + "2.follower.A.env",
		// envFile + "2.follower.B.env",
	},
	envFile + "3.leader.env": []string{
		envFile + "3.follower.A.env",
		envFile + "3.follower.B.env",
	},
}

func randBool() bool {
	rand.Seed(time.Now().Unix())
	return rand.Intn(2) == 1
}

type envMap = map[string][]string
type delay func() time.Duration

type process struct {
	dm *dataManager
	gm *gossipManager
}

type Zone struct {
	leader    process
	followers []process
}

type network struct {
	ctx          context.Context
	allProcesses map[string]*Zone
}

func (p *process) gossip(data string) {
	p.gm.Gossip(data)
}
func (p *process) self() peer.Peer {
	return p.dm.state().Self
}
func (z *Zone) removeFiles() {
	_ = os.Remove(engine.DataFile(z.leader.self()))
	for _, follower := range z.followers {
		_ = os.Remove(engine.DataFile(follower.self()))
	}
}
func (z *Zone) matchSnapshot(t *testing.T, numMessages int) {

	var dataFiles []string
	var fileData []string

	l := engine.DataFile(z.leader.self())
	leaderFile, err := ioutil.ReadFile(l)
	assert.NoError(t, err)
	for _, follower := range z.followers {
		dataFiles = append(dataFiles, engine.DataFile(follower.self()))
	}
	compareEntries := func(t *testing.T, f1 string) {
		// assert.Equal(t, numMessages, len(snapshot.FromFile(f1).Get()))
		//for i, entry := range snapshot.FromFile(f1).Get() {
		//assert.Equal(t, with[i], entry.Data)
		//}
		//f, err := ioutil.ReadFile(f1)
		//assert.NoError(t, err)
		// assert.Equal(t, leaderFile, f)
	}
	fmt.Println("DataFiles", dataFiles)
	for _, file := range dataFiles {
		// t.Run("comparing order with entries for "+file, func(t *testing.T) {
		assert.FileExists(t, file)
		f, err := ioutil.ReadFile(file)
		fileData = append(fileData, string(f))
		assert.NoError(t, err)
		compareEntries(t, file)
		fmt.Println("File Length - ", len(snapshot.FromFile(file).Get()))
		//})
	}
	fmt.Println("Expected Length - ", numMessages)
	//t.Run("comparing random entries", func(t *testing.T) {
	rand.Seed(time.Now().Unix())
	sort.Strings(fileData)
	if len(fileData) > 0 {
		assert.Equal(t, fileData[rand.Intn(len(fileData))], fileData[rand.Intn(len(fileData))])
		assert.Equal(t, fileData[rand.Intn(len(fileData))], fileData[rand.Intn(len(fileData))])
	}
	//})
}
func (z *Zone) sendData(toLeader bool, data string) {
	if toLeader {
		z.leader.gossip(data)
	} else {
		rand.Seed(time.Now().Unix())
		r := len(z.followers)
		r = rand.Intn(r)
		z.followers[r].gossip(data)
	}
}

func setupNetwork(ctx context.Context, leaders ...string) network {
	rand.Seed(time.Now().UnixNano())
	go func() {
		registry.Run("9999", registry.Setup())
	}()
	n := network{
		ctx:          ctx,
		allProcesses: map[string]*Zone{},
	}
	var p = make(chan map[string]process, 20)
	for _, leader := range leaders {
		followers := envFiles[leader]
		dm, gm, _ := Start(ctx, leader)
		go discardGossipReceived(gm)
		n.allProcesses[leader] = new(Zone)
		n.allProcesses[leader].leader = process{dm: dm, gm: gm}
		var wg sync.WaitGroup
		for _, follower := range followers {
			var dm *dataManager
			var gm *gossipManager
			wg.Add(1)
			go func(follower string, dm *dataManager, gm *gossipManager) {
				defer wg.Done()
				dm, gm, _ = Start(ctx, follower)
				go discardGossipReceived(gm)
				p <- map[string]process{leader: {dm: dm, gm: gm}}
			}(follower, dm, gm)
		}
		wg.Wait()
	}
	close(p)
	for pr := range p {
		for leader, follower := range pr {
			n.allProcesses[leader].followers = append(n.allProcesses[leader].followers, follower)
		}
	}
	<-time.After(1 * time.Second)
	return n
}

func discardGossipReceived(gm *gossipManager) {
	for {
		<-gm.rcv
	}
}

func (nw *network) sendGossip(numMessages int, l string, at int, delay time.Duration) {
	for i := 0; i < numMessages; i++ {
		data := "data " + strconv.Itoa(i)
		switch at {
		case atLeaderOnly:
			nw.allProcesses[l].sendData(true, data)
		case atFollowerOnly:
			nw.allProcesses[l].sendData(false, data)
		case atAny:
			if randBool() {
				nw.allProcesses[l].sendData(true, data)
			} else {
				nw.allProcesses[l].sendData(false, data)
			}
		}
		<-time.After(delay)
	}
}

func runTest(t *testing.T, l string, numMessages int, at int, d delay) {
	if at == atAny && numMessages%2 != 0 {
		fmt.Println("even number of messages")
		os.Exit(1)
	}
	ctx, can := context.WithCancel(context.Background())
	defer can()
	nw := setupNetwork(ctx, l)
	defer registry.ShutDown()

	nw.sendGossip(numMessages, l, atAny, d())

	<-time.After(10 * time.Second)
	nw.allProcesses[l].matchSnapshot(t, numMessages)

	<-time.After(2 * time.Second)
}
