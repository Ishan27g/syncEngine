package transport

import (
	"context"
	"testing"
	"time"

	"github.com/Ishan27g/syncEngine/proto"
	"github.com/stretchr/testify/assert"
)

func TestNewVotingClient(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	mockRpcServer(":9000").Start(ctx, nil)

	rc := NewVotingClient("localhost:9000")
	assert.NotNil(t, rc)
	defer rc.Disconnect()

	votes, err := rc.RequestVotes(ctx, &proto.Term{
		TermCount:      0,
		LeaderHttpPort: "",
		LeaderGrpcPort: "",
		LeaderHostname: "",
	})
	assert.NoError(t, err)
	assert.NotNil(t, votes)

	<-time.After(timeout)
}
