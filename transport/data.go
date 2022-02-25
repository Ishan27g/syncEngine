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

type DataSyncClient struct {
	can context.CancelFunc
	proto.DataSyncClient
}

func (dc *DataSyncClient) Disconnect() {
	dc.can()
}
func NewDataSyncClient(serverAddress string) *DataSyncClient {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	grpc.WaitForReady(true)
	fmt.Println("Connecting to rpc -", serverAddress)
	grpcClient, err := grpc.DialContext(ctx, serverAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(otelgrpc.UnaryClientInterceptor()),
		grpc.WithStreamInterceptor(otelgrpc.StreamClientInterceptor()))
	if err != nil {
		fmt.Println(err.Error())
		cancel()
		return nil
	}
	vc := DataSyncClient{cancel, proto.NewDataSyncClient(grpcClient)}
	return &vc
}
