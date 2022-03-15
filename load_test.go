//go:build load
// +build load

package main

import (
	"context"
	"testing"
	"time"

	registry "github.com/Ishan27g/registry/golang/registry/package"
)

const bulkMessages = 100

func Test_Gossip_Load_Single_Zone(t *testing.T) {
	ctx, can := context.WithCancel(context.Background())
	defer func() {
		can()
		<-ctx.Done()
		registry.ShutDown()
		<-time.After(2 * time.Second)
	}()

	runTest(t, ctx, zone1, bulkMessages, atAny, func() time.Duration {
		return time.Millisecond * 100
	})
}
func Test_Gossip_Load_Multiple_Zones(t *testing.T) {
	ctx, can := context.WithCancel(context.Background())
	defer func() {
		can()
		<-ctx.Done()
		registry.ShutDown()
		<-time.After(2 * time.Second)
	}()

	// runTest(t, ctx, zone1, bulkMessages, atAny, func() time.Duration {
	// 	return 1 * time.Second
	// })

	runTestZones(t, ctx, singleRoundNumMessages, atAny, func() time.Duration {
		return 500 * time.Millisecond
	}, []string{zone2, zone3}...)
}
