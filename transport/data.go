package transport

import (
	"context"
	"fmt"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/Ishan27g/syncEngine/proto"
)

type DataSyncClient struct {
	proto.DataSyncClient
}

// NewDataSyncClient returns data grpc client. Wraps connections close based on default conn timeout
func NewDataSyncClient(ctx context.Context, serverAddress string) *DataSyncClient {
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
		fmt.Println("Error connecting to grpc client-", err.Error())
		return nil
	}

	vc := DataSyncClient{proto.NewDataSyncClient(grpcClient)}
	return &vc
}
