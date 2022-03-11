package engine

import (
	"context"
	"testing"
	"time"

	registry "github.com/Ishan27g/registry/golang/registry/package"
	"github.com/Ishan27g/syncEngine/peer"
	"github.com/Ishan27g/syncEngine/provider"
	"github.com/Ishan27g/syncEngine/transport"
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
	mockRegistry()
	<-time.After(2 * time.Second)
	var self peer.Peer
	self, transport.RegistryUrl = peer.FromEnv(envFile)

	url := "http://localhost:14268/api/traces"
	tracerId := self.HttpAddr()

	jp := provider.InitJaeger(context.Background(), tracerId, self.HttpPort, url)
	defer jp.Close()

	hClient := transport.NewHttpClient(self.HttpPort, jp.Get().Tracer(tracerId))

	e := Init(self, &hClient)
	e.Start()

	assert.NotEmpty(t, e.Self())
	assert.NotEmpty(t, e.DataFile)

	assert.NotEmpty(t, e.Logger)

	<-time.After(10 * time.Second)
	assert.NotNil(t, e.zoneRaft)
	assert.NotNil(t, e.syncRaft)

}
