package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/Ishan27g/gossipProtocol"
)

func main() {
	var g gossipProtocol.Gossip
	var receive <-chan gossipProtocol.Packet
	if os.Args[1] == "1" {
		var g1 gossipProtocol.Gossip
		g1, receive = gossipProtocol.Config("localhost", "8001", "p1")
		var peers []gossipProtocol.Peer
		peers = append(peers, gossipProtocol.Peer{UdpAddress: "localhost:8002", ProcessIdentifier: "p2"})
		peers = append(peers, gossipProtocol.Peer{UdpAddress: "localhost:8003", ProcessIdentifier: "p3"})
		g1.Join(peers...)
		//g1.Add(gossipProtocol.Peer{UdpAddress: "localhost:9999", ProcessIdentifier: "p3"})
		//g1.Add(gossipProtocol.Peer{UdpAddress: "localhost:9998", ProcessIdentifier: "p4"})

		g = g1
	} else if os.Args[1] == "2" {
		var g1 gossipProtocol.Gossip
		g1, receive = gossipProtocol.Config("localhost", "8002", "p2")
		var peers []gossipProtocol.Peer
		peers = append(peers, gossipProtocol.Peer{UdpAddress: "localhost:8001", ProcessIdentifier: "p1"})
		peers = append(peers, gossipProtocol.Peer{UdpAddress: "localhost:8003", ProcessIdentifier: "p3"})
		g1.Join(peers...)
		g = g1

	} else if os.Args[1] == "3" {
		var g1 gossipProtocol.Gossip
		g1, receive = gossipProtocol.Config("localhost", "8003", "p3")
		var peers []gossipProtocol.Peer
		peers = append(peers, gossipProtocol.Peer{UdpAddress: "localhost:8001", ProcessIdentifier: "p1"})
		peers = append(peers, gossipProtocol.Peer{UdpAddress: "localhost:8002", ProcessIdentifier: "p2"})
		g1.Join(peers...)
		g = g1
	}
	reader := bufio.NewReader(os.Stdin)
	go func() {
		for {
			packet := <-receive
			fmt.Println(packet)
		}
	}()
	for {
		text, _ := reader.ReadString('\n')
		text = strings.Replace(text, "\n", "", -1) // convert CRLF to LF
		if strings.Compare("send", text) == 0 {
			fmt.Println("Enter Data -\n\t$ ")
			text, _ := reader.ReadString('\n')
			text = strings.Replace(text, "\n", "", -1) // convert CRLF to LF
			// println(g.CurrentView())
			g.SendGossip(text)
		}
	}
}
