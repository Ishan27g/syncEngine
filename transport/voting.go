package transport

import (
	"context"
	"fmt"

	"github.com/Ishan27g/syncEngine/proto"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type VotingClient struct {
	proto.RaftVotingClient
}

// NewVotingClient returns vote grpc client. Wraps connections close based on default conn timeout
func NewVotingClient(ctx context.Context, serverAddress string) *VotingClient {
	var grpcClient *grpc.ClientConn
	var err error

	ctxClose, can := context.WithTimeout(ctx, ConnectionTimeout)
	go func() {
		<-ctx.Done()
		can()
		grpcClient.Close()
	}()

	grpc.WaitForReady(false)
	grpcClient, err = grpc.DialContext(ctxClose, serverAddress,
		grpc.WithBlock(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(otelgrpc.UnaryClientInterceptor()),
		grpc.WithStreamInterceptor(otelgrpc.StreamClientInterceptor()))
	if err != nil {
		fmt.Println(err.Error())
		return nil
	}
	vc := VotingClient{proto.NewRaftVotingClient(grpcClient)}
	return &vc
}
