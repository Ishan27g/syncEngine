package transport

import (
	"context"
	"fmt"
	"time"

	"github.com/Ishan27g/syncEngine/proto"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type VotingClient struct {
	can context.CancelFunc
	proto.RaftVotingClient
}

func (vc *VotingClient) Disconnect() {
	vc.can()
}
func NewVotingClient(serverAddress string) *VotingClient {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	grpc.WaitForReady(true)
	grpcClient, err := grpc.DialContext(ctx, serverAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(otelgrpc.UnaryClientInterceptor()),
		grpc.WithStreamInterceptor(otelgrpc.StreamClientInterceptor()))
	if err != nil {
		fmt.Println(err.Error())
		cancel()
		return nil
	}
	vc := VotingClient{cancel, proto.NewRaftVotingClient(grpcClient)}
	return &vc
}
