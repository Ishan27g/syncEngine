//go:build load
// +build load

package main

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"
)

func Test_Gossip_Load_Multiple_Rounds(t *testing.T) {

	l := envFile + "1.leader.env"
	ctx, can := context.WithCancel(context.Background())
	defer can()

	var numMessages = 100
	nw := setupNetwork(ctx, l)

	t.Run("Zone-"+l+" messages - "+strconv.Itoa(numMessages), func(t *testing.T) {
		var sentOrder = make(chan string, numMessages)
		var wg sync.WaitGroup
		defer nw.allProcesses[l].removeFiles()

		<-time.After(1 * time.Second)
		for i := 0; i < numMessages; i += 2 {
			wg.Add(2)
			<-time.After(2 * time.Second) // new round
			go func(i int) {
				defer wg.Done()
				go func(i int) {
					defer wg.Done()
					<-time.After(100 + randomInt())
					data := "data " + strconv.Itoa(i)
					nw.allProcesses[l].sendData(true, data)
					sentOrder <- data
				}(i)
				<-time.After(100 + randomInt())
				data := "data " + strconv.Itoa(i+1+1)
				nw.allProcesses[l].sendData(false, data)
				sentOrder <- data
			}(i)
			wg.Wait()
		}
		close(sentOrder)
		<-time.After(10 * time.Second)
		var so []string
		for s := range sentOrder {
			so = append(so, s)
		}
		nw.allProcesses[l].matchSnapshot(t, so)
	})
}
func Test_Gossip_Load_Single_Round(t *testing.T) {
	l := envFile + "2.leader.env"
	ctx, can := context.WithCancel(context.Background())
	defer can()

	var numMessages = 200
	nw := setupNetwork(ctx, l)

	t.Run("Zone-"+l+" messages - "+strconv.Itoa(numMessages), func(t *testing.T) {
		var sentOrder = make(chan string, numMessages)
		var wg sync.WaitGroup
		defer nw.allProcesses[l].removeFiles()

		<-time.After(1 * time.Second)
		for i := 0; i < numMessages; i += 2 {
			wg.Add(2)
			<-time.After(15 * time.Millisecond) // timeout between consecutive gossip requests at same process
			go func(i int) {
				defer wg.Done()
				go func(i int) {
					defer wg.Done()
					<-time.After(randomInt())
					data := "data " + strconv.Itoa(i)
					nw.allProcesses[l].sendData(true, data)
					sentOrder <- data
				}(i)
				<-time.After(randomInt())
				data := "data " + strconv.Itoa(i+1+1)
				nw.allProcesses[l].sendData(false, data)
				sentOrder <- data
			}(i)
			wg.Wait()
		}
		close(sentOrder)
		<-time.After(10 * time.Second)
		var so []string
		for s := range sentOrder {
			so = append(so, s)
		}
		nw.allProcesses[l].matchSnapshot(t, so)
	})

}
