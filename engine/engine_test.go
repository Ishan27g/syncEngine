package engine

import (
	"context"
	"testing"
	"time"

	"github.com/Ishan27g/syncEngine/peer"
	"github.com/Ishan27g/syncEngine/transport"
	registry "github.com/Ishan27gOrg/registry/golang/registry/package"
	"github.com/stretchr/testify/assert"
)

var envFile = "../.envFiles/1.leader.env"

func mockRegistry() {
	go func() {
		c := registry.Setup()
		registry.Run("9999", c)

	}()
}
func TestInit(t *testing.T) {
	ctx, can := context.WithCancel(context.Background())
	defer can()
	mockRegistry()
	<-time.After(2 * time.Second)
	var self peer.Peer
	self, transport.RegistryUrl = peer.FromEnv(envFile)

	e := Init(self)
	e.Start(ctx)

	assert.NotEmpty(t, e.Self())
	assert.NotEmpty(t, e.DataFile)

	assert.NotEmpty(t, e.Gossip.Gossip)
	assert.NotNil(t, e.Gossip.GossipRcv)

	assert.NotEmpty(t, e.Logger)

	<-time.After(10 * time.Second)
	assert.NotNil(t, e.zoneRaft)
	assert.NotNil(t, e.syncRaft)

}
