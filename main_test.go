package main

import (
	"context"
	"io/ioutil"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/Ishan27g/syncEngine/engine"
	"github.com/Ishan27g/syncEngine/peer"
	"github.com/stretchr/testify/assert"

	registry "github.com/Ishan27g/registry/golang/registry/package"
)

var envFile = "./.envFiles/"

type envMap = map[string][]string

type process struct {
	dm *dataManager
	gm *gossipManager
}

func (p *process) gossip(data string) {
	p.gm.Gossip(data)
}
func (p *process) self() peer.Peer {
	return p.dm.state().Self
}

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

type zone struct {
	leader    process
	followers []process
}

func (z *zone) removeFiles() {
	_ = os.Remove(engine.DataFile(z.leader.self()))
	for _, follower := range z.followers {
		_ = os.Remove(engine.DataFile(follower.self()))
	}
}
func (z *zone) matchSnapshot(t *testing.T, sentOrder []string) {

	t.Run("comparing snapshot for zone", func(t *testing.T) {
		var dataFiles []string
		var fileData []string

		dataFiles = append(dataFiles, engine.DataFile(z.leader.self()))
		for _, follower := range z.followers {
			dataFiles = append(dataFiles, engine.DataFile(follower.self()))
		}
		compareEntries := func(t *testing.T, with []string, f1 string) {
			//for i, entry := range snapshot.FromFile(f1).Get() {
			//assert.Equal(t, with[i], entry.Data)
			//}
		}
		for _, file := range dataFiles {
			t.Run("comparing order with entries for "+file, func(t *testing.T) {
				t.Parallel()
				assert.FileExists(t, file)
				f, err := ioutil.ReadFile(file)
				fileData = append(fileData, string(f))
				assert.NoError(t, err)
				compareEntries(t, sentOrder, file)
			})
		}
		t.Run("comparing random entries", func(t *testing.T) {
			rand.Seed(time.Now().Unix())
			sort.Strings(fileData)
			if len(fileData) > 0 {
				assert.Equal(t, fileData[rand.Intn(len(fileData))], fileData[rand.Intn(len(fileData))])
				assert.Equal(t, fileData[rand.Intn(len(fileData))], fileData[rand.Intn(len(fileData))])
			}
		})
	})
}
func (z *zone) sendData(toLeader bool, data string) {
	if toLeader {
		z.leader.gossip(data)
	} else {
		rand.Seed(time.Now().Unix())
		r := len(z.followers)
		r = rand.Intn(r)
		z.followers[r].gossip(data)
	}
}

type network struct {
	ctx          context.Context
	allProcesses map[string]*zone
}

func setupNetwork(ctx context.Context, leaders ...string) network {
	rand.Seed(time.Now().UnixNano())
	go func() {
		registry.Run("9999", registry.Setup())
	}()
	n := network{
		ctx:          ctx,
		allProcesses: map[string]*zone{},
	}
	for _, leader := range leaders {
		followers := envFiles[leader]
		dm, gm, _ := Start(ctx, leader)

		n.allProcesses[leader] = new(zone)
		n.allProcesses[leader].leader = process{
			dm: dm,
			gm: gm,
		}
		for _, follower := range followers {
			dm, gm, _ := Start(ctx, follower)
			n.allProcesses[leader].followers = append(n.allProcesses[leader].followers, process{
				dm: dm,
				gm: gm,
			})
		}
	}
	return n
}
func randomInt() time.Duration {
	return time.Duration(rand.Intn(1000))
}
func Test_Single_Round(t *testing.T) {
	l := envFile + "1.leader.env"
	ctx, can := context.WithCancel(context.Background())
	defer can()

	var numMessages = 10
	nw := setupNetwork(ctx, l)
	defer registry.ShutDown()

	t.Run("Zone-"+l+" messages - "+strconv.Itoa(numMessages), func(t *testing.T) {
		defer nw.allProcesses[l].removeFiles()

		var sentOrder = make(chan string, numMessages)
		var wg sync.WaitGroup

		<-time.After(1 * time.Second)
		for i := 0; i < numMessages; i += 2 {
			wg.Add(2)
			<-time.After(15 * time.Millisecond) // timeout between consecutive gossip requests at same process
			go func(i int) {
				defer wg.Done()
				go func(i int) {
					defer wg.Done()
					<-time.After(randomInt())
					data := "data " + strconv.Itoa(i)
					nw.allProcesses[l].sendData(true, data)
					sentOrder <- data
				}(i)
				<-time.After(randomInt())
				data := "data " + strconv.Itoa(i+1+1)
				nw.allProcesses[l].sendData(false, data)
				sentOrder <- data
			}(i)
			wg.Wait()
		}
		close(sentOrder)
		<-time.After(10 * time.Second)
		var so []string
		for s := range sentOrder {
			so = append(so, s)
		}
		nw.allProcesses[l].matchSnapshot(t, so)
	})

}

func Test_Multiple_Rounds(t *testing.T) {

	l := envFile + "1.leader.env"
	ctx, can := context.WithCancel(context.Background())
	defer can()

	var numMessages = 16
	nw := setupNetwork(ctx, l)
	defer registry.ShutDown()

	t.Run("Zone-"+l+" messages - "+strconv.Itoa(numMessages), func(t *testing.T) {
		defer nw.allProcesses[l].removeFiles()

		var sentOrder = make(chan string, numMessages)
		var wg sync.WaitGroup

		<-time.After(1 * time.Second)
		for i := 0; i < numMessages; i += 4 {
			wg.Add(2)
			<-time.After(2 * time.Second) // new round
			go func(i int) {
				defer wg.Done()
				go func(i int) {
					defer wg.Done()
					<-time.After(randomInt())
					data := "data " + strconv.Itoa(i)
					nw.allProcesses[l].sendData(true, data)
					sentOrder <- data
					<-time.After(randomInt())
					data = "data " + strconv.Itoa(i+1)
					nw.allProcesses[l].sendData(true, data)
					sentOrder <- data
				}(i)
				<-time.After(randomInt())
				data := "data " + strconv.Itoa(i+1+1)
				nw.allProcesses[l].sendData(false, data)
				sentOrder <- data
				<-time.After(randomInt())
				data = "data " + strconv.Itoa(i+1+1+1)
				nw.allProcesses[l].sendData(false, data)
				sentOrder <- data
			}(i)
			wg.Wait()
		}
		close(sentOrder)
		<-time.After(6 * time.Second)
		var so []string
		for s := range sentOrder {
			so = append(so, s)
		}
		nw.allProcesses[l].matchSnapshot(t, so)
	})
}
