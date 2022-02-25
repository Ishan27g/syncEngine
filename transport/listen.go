package transport

import (
	"context"
	"net"
	"os"

	"github.com/Ishan27g/go-utils/mLogger"
	"github.com/Ishan27g/syncEngine/proto"
	"github.com/hashicorp/go-hclog"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
)

type RpcOption func(*RpcServer)

type RpcServer struct {
	port         string
	votingServer proto.RaftVotingServer
	dataServer   proto.DataSyncServer
	hclog.Logger
}

func WithPort(port string) RpcOption {
	return func(r *RpcServer) {
		r.port = port
	}
}
func WithVotingServer(votingServer proto.RaftVotingServer) RpcOption {
	return func(r *RpcServer) {
		r.votingServer = votingServer
	}
}
func WithDataServer(dataServer proto.DataSyncServer) RpcOption {
	return func(r *RpcServer) {
		r.dataServer = dataServer
	}
}
func NewRpcServer(opts ...RpcOption) *RpcServer {

	r := &RpcServer{
		port:         "",
		votingServer: nil,
		dataServer:   nil,
		Logger:       nil,
	}
	for _, opt := range opts {
		opt(r)
	}
	if r.port == "" {
		return nil
	}
	if r.dataServer == nil {
		return nil
	}
	if r.votingServer == nil {
		return nil
	}
	r.Logger = mLogger.Get("rpc-server" + r.port)
	return r
}
func (r *RpcServer) Start(ctx context.Context) {
	go func() {
		grpcServer := grpc.NewServer(
			grpc.UnaryInterceptor(otelgrpc.UnaryServerInterceptor()),
			grpc.StreamInterceptor(otelgrpc.StreamServerInterceptor()),
		)
		proto.RegisterRaftVotingServer(grpcServer, r.votingServer)
		proto.RegisterDataSyncServer(grpcServer, r.dataServer)
		grpcAddr, err := net.Listen("tcp", r.port)
		if err != nil {
			r.Error(err.Error())
			os.Exit(1)
		}
		go func() {
			r.Info("RPC server started", "port", grpcAddr.Addr().String())
			if err := grpcServer.Serve(grpcAddr); err != nil {
				r.Error("failed to serve: " + err.Error())
				return
			}
		}()
		<-ctx.Done()
		grpcServer.Stop()
	}()

}
