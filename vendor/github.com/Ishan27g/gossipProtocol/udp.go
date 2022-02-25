package gossipProtocol

import (
	"context"
	"fmt"
	"net"
	"strings"

	"github.com/Ishan27g/go-utils/mLogger"

	"github.com/hashicorp/go-hclog"
)

// Client is the UDP Client interface
type client interface {
	send(address string, data []byte) []byte
}

func getClient(logName string) client {
	u := &udpClient{
		processName: logName,
		logger:      mLogger.Get(logName + "-udp-client"),
	}
	if !loggerOn {
		u.logger.SetLevel(hclog.Info)
	}
	return u
}

type udpClient struct {
	processName string
	logger      hclog.Logger
}

const hostname = "http://localhost"

func trimHost(address string) string {
	s := strings.Trim(address, hostname)
	return ":" + s
}
func (u *udpClient) send(address string, data []byte) []byte {
	s, err := net.ResolveUDPAddr("udp4", address)
	if err != nil {
		address = trimHost(address)
		s, err = net.ResolveUDPAddr("udp4", address)
		if err != nil {
			return nil
		}
	}
	c, err := net.DialUDP("udp4", nil, s)
	if err != nil {
		return nil
	}
	defer c.Close()
	_, err = c.Write(data)
	if err != nil {
		return nil
	}
	buffer := make([]byte, 4096)
	readLen, _, err := c.ReadFromUDP(buffer)
	if err != nil {
		u.logger.Trace(err.Error() + "\n for " + c.RemoteAddr().String())
		return nil
	}
	buffer = buffer[:readLen]
	u.logger.Trace("Received startRounds response from - " + c.RemoteAddr().String())
	return buffer
}

// Listen starts the udp server that listens for an incoming view or startRounds from peers.
// Responds with the current view / startRounds as per strategy.
func Listen(ctx context.Context, port string, gossipCb func(Packet, Peer) []byte, viewCb func(View, Peer) []byte) {
	s, err := net.ResolveUDPAddr("udp4", port)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	connection, err := net.ListenUDP("udp4", s)
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	go func() {
		<-ctx.Done()
		connection.Close()
	}()
	fmt.Println("Started on ", port)
	for {
		buffer := make([]byte, 4096)
		readLen, addr, e := connection.ReadFromUDP(buffer)
		if e != nil {
			return
		}
		if string(buffer) == "OKAY" {
			continue
		}
		buffer = buffer[:readLen]
		view, from, err := BytesToView(buffer)
		if from.UdpAddress != "" && err == nil {
			rsp := viewCb(view, from)
			_, err = connection.WriteToUDP(rsp, addr)
			if err != nil {
				fmt.Println("WriteToUDP", err.Error())
			}
		} else {
			rsp := gossipCb(ByteToPacket(buffer))
			_, err = connection.WriteToUDP(rsp, addr)
			if err != nil {
				fmt.Println(err.Error())
			}
		}
	}
}
