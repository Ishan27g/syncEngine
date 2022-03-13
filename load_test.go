//go:build load
// +build load

package main

import (
	"context"
	"testing"
	"time"

	registry "github.com/Ishan27g/registry/golang/registry/package"
)

const bulkMessages = 10

func Test_Gossip_Load_Single_Round(t *testing.T) {
	ctx, can := context.WithCancel(context.Background())
	defer func() {
		can()
		<-ctx.Done()
		registry.ShutDown()
		<-time.After(1 * time.Second)
	}()

	runTest(t, ctx, zone1, bulkMessages, atAny, func() time.Duration {
		return time.Millisecond * 100
	})
}
func Test_Gossip_Load_Multiple_Rounds(t *testing.T) {
	ctx, can := context.WithCancel(context.Background())
	defer func() {
		can()
		<-ctx.Done()
		registry.ShutDown()
		<-time.After(1 * time.Second)
	}()

	runTest(t, ctx, zone1, bulkMessages, atAny, func() time.Duration {
		return 1*time.Second + syncDelay
	})
}
