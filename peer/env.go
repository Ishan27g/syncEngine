package peer

import (
	"fmt"
	"strconv"

	"github.com/joho/godotenv"
)

type engineEnv struct {
	RegistryAddr string `env:"REGISTRY,required"`

	Hostname string `env:"Hostname"`
	Zone     int    `env:"ZoneId,required"`

	HttpPort string `env:"HTTP_PORT,required"` // zone peer discovery
	GrpcPort string `env:"GRPC_PORT,required"` // raft
	UdpPort  string `env:"UDP_PORT,required"`  // gossip & views

	TempName string `env:"TempName"`
	Self     Peer
}

func InitEnv(envFile string) *engineEnv {
	envMap, err := godotenv.Read(envFile)
	if err != nil {
		fmt.Println(err.Error())
		return nil
	}
	z, err := strconv.Atoi(envMap["ZoneId"])
	if err != nil {
		fmt.Println(err.Error())
		return nil
	}
	var e = engineEnv{
		RegistryAddr: envMap["REGISTRY"],
		Hostname:     envMap["Hostname"],
		Zone:         z,
		HttpPort:     envMap["HTTP_PORT"],
		GrpcPort:     envMap["GRPC_PORT"],
		UdpPort:      envMap["UDP_PORT"],
		TempName:     envMap["TempName"],
		Self:         Peer{},
	}
	if e.Hostname == "" {
		e.Hostname = "localhost"
	}
	e.Self = Peer{
		FakeName: e.TempName,
		Zone:     e.Zone,
		HostName: e.Hostname,
		HttpPort: ":" + e.HttpPort,
		GrpcPort: ":" + e.GrpcPort,
		UdpPort:  e.UdpPort,
		Mode:     FOLLOWER,
		RaftTerm: -1,
		SyncTerm: -1,
	}
	return &e
}
