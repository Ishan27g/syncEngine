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

func NewDataSyncClient(ctx context.Context, serverAddress string) *DataSyncClient {
	grpc.WaitForReady(true)
	grpcClient, err := grpc.DialContext(ctx, serverAddress,
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
