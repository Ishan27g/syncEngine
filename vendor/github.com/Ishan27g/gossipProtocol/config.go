package gossipProtocol

import (
	"crypto/sha1"
	"fmt"
	"time"
)

// default vars
var loggerOn bool
var defaultHashMethod = defaultHash
var defaultStrategy = PeerSamplingStrategy{
	PeerSelectionStrategy:   Random,
	ViewPropagationStrategy: PushPull,
	ViewSelectionStrategy:   Random,
}

const gossipDelay = 10 * time.Millisecond
const rounds = 1
const fanOut = 5

type envConfig struct {
	Hostname          string `env:"HOST_NAME"`
	UdpPort           string `env:"UDP_PORT,required"`
	ProcessIdentifier string
	RoundDelay        time.Duration // timeout between each round for a gossipMessage
	FanOut            int           // num of peers to startRounds a message to
}

func defaultEnv(hostname string, port string, id string) *envConfig {
	return &envConfig{
		Hostname:          hostname,
		UdpPort:           ":" + port,
		ProcessIdentifier: id,
		RoundDelay:        gossipDelay,
		FanOut:            fanOut,
	}
}

func defaultHash(obj string) string {
	h := sha1.New()
	h.Write([]byte(fmt.Sprintf("%v", obj)))
	return fmt.Sprintf("%x", h.Sum(nil))
}
