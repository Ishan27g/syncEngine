package transport

import (
	"context"
	"testing"
	"time"

	"github.com/Ishan27g/syncEngine/proto"
	"github.com/stretchr/testify/assert"
)

var timeout = 5 * time.Second

type mockVotingServer struct {
	s proto.RaftVotingServer
}

func (m mockVotingServer) RequestVotes(ctx context.Context, term *proto.Term) (*proto.Vote, error) {
	return &proto.Vote{Elected: false}, nil
}

type mockDataServer struct {
	s proto.DataSyncServer
}

func (m mockDataServer) NewEvent(ctx context.Context, order *proto.Order) (*proto.Ok, error) {
	return &proto.Ok{Id: "id"}, nil
}

func (m mockDataServer) SaveOrder(ctx context.Context, order *proto.Order) (*proto.Ok, error) {
	return &proto.Ok{Id: "id"}, nil
}

func (m mockDataServer) GetSyncData(ctx context.Context, ok *proto.Ok) (*proto.Order, error) {
	return &proto.Order{Events: []*proto.Event{{
		EventId:      "id",
		ClockEntries: nil,
	}}}, nil
}

func (m mockDataServer) GetPacketAddresses(ctx context.Context, ok *proto.Ok) (*proto.Peers, error) {
	return &proto.Peers{Peers: []*proto.Peer{{
		UdpAddress: "ok",
		PeerId:     "ok",
	}}}, nil
}

func (m mockDataServer) GetNetworkView(ctx context.Context, ok *proto.Ok) (*proto.Peers, error) {
	return &proto.Peers{Peers: []*proto.Peer{{
		UdpAddress: "ok",
		PeerId:     "ok",
	}}}, nil
}

func TestNewRpcServer_BadOptions(t *testing.T) {
	var cases = map[string][]RpcOption{
		"no-option": nil,
		"only-port": {
			WithPort(":9000"),
		},
		"no-voting-server": {
			WithPort(":9000"),
			WithDataServer(mockDataServer{}),
		},
		"no-data-server": {
			WithPort(":9000"),
			WithVotingServer(mockVotingServer{}),
		},
		"no-port": {
			WithDataServer(mockDataServer{}),
			WithVotingServer(mockVotingServer{}),
		},
	}
	for name, options := range cases {
		t.Run(name, func(t *testing.T) {
			srv := NewRpcServer(options...)
			assert.Nil(t, srv)
		})
	}
}
func TestNewRpcServer(t *testing.T) {
	srv := mockRpcServer(":9000")
	assert.NotNil(t, srv)

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	srv.Start(ctx)
	<-time.After(timeout)
	cancel()
	<-time.After(timeout)

	// restart
	ctx, cancel = context.WithTimeout(context.Background(), timeout)
	defer cancel()
	srv.Start(ctx)
	<-time.After(timeout)

}

func mockRpcServer(port string) *RpcServer {
	opts := []RpcOption{
		WithPort(port),
		WithVotingServer(mockVotingServer{}),
		WithDataServer(mockDataServer{}),
	}
	srv := NewRpcServer(opts...)
	return srv
}
