
# Gossip protocol

Gossip based communication with partial network views & peer-sampling 

#### Various [strategies](http://lpdwww.epfl.ch/upload/documents/publications/neg--1184036295all.pdf) for maintaining a partial view of the network.
 - View-selection
 - View-propagation
 - Peer-selection 
 
 These strategies allow for the construction and maintenance of various dynamic unstructured overlays through gossiping membership information
   (http://lpdwww.epfl.ch/upload/documents/publications/neg--1184036295all.pdf)

#### Partial ordering of events
  - Each `Peer` maintains a vector clock relative to its partial view
  - Data gossipped to the network encapsulates the vector clocks of participating `Peers` 
  - Allows for data sent to the network to be partially ordered based on this.

### Gossip Listener
```go
package gossipProtocol

type Gossip interface {
	// Join with some initial peers
	Join(...Peer)
	// Add peers
	Add(...Peer)
	// CurrentView returned as a String
	CurrentView() string
	// SendGossip  to the network
	SendGossip(data string)
}

```

### Example

```go
package main

import (
	"fmt"

	"github.com/Ishan27gOrg/gossipProtocol"
)



func main() {
	// self
	g, receive := gossipProtocol.Config("localhost", "8001", "p1")
	
	// other peers
	var peers []gossipProtocol.Peer
	peers = append(peers, gossipProtocol.Peer{UdpAddress: "localhost:8002", ProcessIdentifier: "p2"})
	peers = append(peers, gossipProtocol.Peer{UdpAddress: "localhost:8003", ProcessIdentifier: "p3"})
	
	// join and send some data
	g.Join(peers...)
	
	g.SendGossip("some data")
	for{
		gossipPacket := <-receive
		fmt.Println("data - ", gossipPacket.GossipMessage.Data)
		fmt.Println("event clock for this packet - ", gossipPacket.VectorClock)
	}
}
```

- Vector & Event Clocks https://github.com/Ishan27g/vClock
