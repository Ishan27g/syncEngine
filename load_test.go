package main

// // go:build load
// // +build load

import (
	"testing"
	"time"
)

const bulkMessages = 100

func Test_Gossip_Load_Multiple_Rounds(t *testing.T) {
	runTest(t, zone1, bulkMessages, atAny, func() time.Duration {
		return 1*time.Second + syncDelay
	})
}
func Test_Gossip_Load_Single_Round(t *testing.T) {
	runTest(t, zone1, bulkMessages, atAny, func() time.Duration {
		return time.Millisecond * 100
	})
}
