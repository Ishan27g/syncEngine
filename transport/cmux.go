package transport

import (
    "context"
    "log"
    "net"
    "strings"

    "github.com/soheilhy/cmux"
)

func Listen(ctx context.Context, rpcServer *RpcServer, httpSrv *HttpSrv){
    l, err := net.Listen("tcp", rpcServer.port) //  httpSrv.httpSrv.Addr == rpcServer.port
    if err != nil {
        log.Panic(err)
    }

    m := cmux.New(l)
    go rpcServer.Start(ctx, m.Match(cmux.HTTP2HeaderFieldPrefix("content-type", "application/grpc")))
    go httpSrv.Start(ctx, m.Match(cmux.HTTP1Fast()))

    if err := m.Serve(); !strings.Contains(err.Error(), "use of closed network connection") {
        panic(err)
    }
}
