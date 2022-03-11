package main

import (
	"context"
	"io/ioutil"
	"math/rand"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/Ishan27g/syncEngine/engine"
	"github.com/Ishan27g/syncEngine/peer"
	"github.com/Ishan27g/syncEngine/snapshot"
	"github.com/stretchr/testify/assert"

	registry "github.com/Ishan27g/registry/golang/registry/package"
)

var once sync.Once
var envFile = "./.envFiles/"

type envMap = map[string][]string

type process struct {
	dm *dataManager
	gm *gossipManager
}

func (p *process) gossip(data string) {
	go p.gm.Gossip(data)
}
func (p *process) self() peer.Peer {
	return p.dm.state().Self
}

var envFiles = envMap{
	envFile + "1.leader.env": []string{
		envFile + "1.follower.A.env",
		//	envFile + "1.follower.B.env",
	},
	envFile + "2.leader.env": []string{
		envFile + "2.follower.A.env",
		envFile + "2.follower.B.env",
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

func (z *zone) matchSnapshot(t *testing.T, sentOrder []string) {

	t.Run("comparing snapshot for zone", func(t *testing.T) {
		t.Parallel()
		var dataFiles []string
		var fileData []string

		dataFiles = append(dataFiles, engine.DataFile(z.leader.self()))
		for _, follower := range z.followers {
			dataFiles = append(dataFiles, engine.DataFile(follower.self()))
		}
		compareEntries := func(t *testing.T, with []string, f1 string) {
			for i, entry := range snapshot.FromFile(f1).Get() {
				assert.Equal(t, with[i], entry.Data)
			}
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
			t.Parallel()
			rand.Seed(time.Now().Unix())
			sort.Strings(fileData)
			assert.Equal(t, fileData[0], fileData[len(fileData)-1])
			assert.Equal(t, fileData[rand.Intn(len(fileData))], fileData[rand.Intn(len(fileData))])
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
	once.Do(func() {
		rand.Seed(time.Now().UnixNano())
		go func() {
			registry.Run("9999", registry.Setup())
		}()
	})
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

func Test_Simple(t *testing.T) {
	ctx, can := context.WithCancel(context.Background())
	defer can()
	nw := setupNetwork(ctx, envFile+"1.leader.env")

	<-time.After(2 * time.Second)
	nw.allProcesses[envFile+"1.leader.env"].sendData(true, "ok")
	<-time.After(25 * time.Millisecond)
	nw.allProcesses[envFile+"1.leader.env"].sendData(false, "okay")
	<-time.After(6 * time.Second)
	nw.allProcesses[envFile+"1.leader.env"].matchSnapshot(t, []string{"ok", "okay"})
}
