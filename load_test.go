//go:build load
// +build load

package main

import (
	"context"
	"testing"
	"time"
)

const bulkMessages = 100

//func Test_Gossip_Load_Single_Round(t *testing.T) {
//	ctx, can := context.WithCancel(context.Background())
//	defer can()
//	runTest(t, ctx, zone1, bulkMessages, atAny, func() time.Duration {
//		return time.Millisecond * 100
//	})
//}
func Test_Gossip_Load_Multiple_Rounds(t *testing.T) {
	ctx, can := context.WithCancel(context.Background())
	defer can()
	runTest(t, ctx, zone1, bulkMessages, atAny, func() time.Duration {
		return 1*time.Second + syncDelay
	})
}
