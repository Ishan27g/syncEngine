package vClock

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewEventVector(t *testing.T) {
	leader := NewEventVector()

	// event from p1
	e1 := newEvent("1", EventClock{
		"p1": 1,
		"p2": 1,
		"p3": 0,
	})
	// event 2 clock > event 1 clock
	e2 := newEvent("2", EventClock{
		"p1": 1,
		"p2": 2,
		"p3": 0,
	})
	// event 3 is the latest message
	e3 := newEvent("3", EventClock{
		"p1": 3,
		"p2": 3,
		"p3": 3,
	})
	e4 := newEvent("11", EventClock{
		"p1": 4,
		"p2": 4,
		"p3": 4,
	})
	// receive in wrong order
	leader.MergeEvent(Event{EventId: "3", EventClock: e3.EventClock})
	fmt.Println(leader.GetEventsOrder())
	leader.MergeEvent(Event{EventId: "1", EventClock: e1.EventClock})
	fmt.Println(leader.GetEventsOrder())
	leader.MergeEvent(Event{EventId: "2", EventClock: e2.EventClock})
	fmt.Println(leader.GetEventsOrder())
	leader.MergeEvent(Event{EventId: "11", EventClock: e4.EventClock})

	order := leader.GetEventsOrder()
	assert.Equal(t, "1", order[0].EventId)
	assert.Equal(t, "2", order[1].EventId)
	assert.Equal(t, "3", order[2].EventId)
	assert.Equal(t, "11", order[3].EventId)

	fmt.Println(leader.GetEventsOrder())
}

func TestNewEventVectorSameClock(t *testing.T) {
	leader := NewEventVector()

	// event from p1
	e1 := newEvent("1", EventClock{
		"localhost:3101": 1,
		"localhost:3102": 3,
		//"p2": 1,
		//"p3": 1,
	})
	// event 2 clock > event 1 clock
	e2 := newEvent("2", EventClock{
		"localhost:3101": 1,
		"localhost:3102": 3,
		//"p2": 1,
		//"p3": 1,
	})
	// event 2 clock > event 1 clock
	e3 := newEvent("12", EventClock{
		"localhost:3101": 1,
		"localhost:3102": 3,
		//"p2": 1,
		//"p3": 1,
	})
	// receive in wrong order
	leader.MergeEvent(Event{EventId: "1", EventClock: e1.EventClock})
	fmt.Println(leader.GetEventsOrder())
	leader.MergeEvent(Event{EventId: "2", EventClock: e2.EventClock})
	fmt.Println(leader.GetEventsOrder())
	leader.MergeEvent(Event{EventId: "12", EventClock: e3.EventClock})
	fmt.Println(leader.GetEventsOrder())

}
