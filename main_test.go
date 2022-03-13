package main

import (
	"testing"
	"time"
)

var singleRoundNumMessages = 16
var zone1 = envFile + "1.leader.env"

func Test_Round_AtLeader(t *testing.T) {
	runTest(t, zone1, singleRoundNumMessages, atLeaderOnly, randomInt)
}
func Test_Round_AtFollowers(t *testing.T) {
	runTest(t, zone1, singleRoundNumMessages, atFollowerOnly, randomInt)
}
func Test_Round_AtRandom(t *testing.T) {
	runTest(t, zone1, singleRoundNumMessages, atAny, randomInt)
}
func Test_Multiple_Rounds(t *testing.T) {

	var numMessages = 16
	var delay = delay(func() time.Duration {
		return time.Second * 2
	})
	runTest(t, zone1, numMessages, atAny, delay)
}

// go test --v ./... -run Test_Round_AtLeader
// go test --v ./... -run Test_Round_AtFollowers
// go test --v ./... -run Test_Round_AtRandom
// go test --v ./... -run Test_Multiple_Rounds
