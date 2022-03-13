package gossipProtocol

import "strconv"

var udpPort = func(start string, numProcesses int) []string {
	var ports []string
	for i := 0; i < numProcesses; i++ {
		if i < 10 {
			ports = append(ports, start+"0"+strconv.Itoa(i))
		} else {
			ports = append(ports, start+strconv.Itoa(i))
		}
	}
	return ports
}

var network = func(base string, exclude int, numProcesses int) []Peer {
	var ports []Peer
	for i, s := range udpPort(base, numProcesses) {
		if i == exclude && exclude != -1 {
			continue
		}
		ports = append(ports, Peer{
			UdpAddress:        s,
			ProcessIdentifier: "Pid-" + strconv.Itoa(i),
		})
	}
	return ports
}
