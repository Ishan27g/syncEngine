package data

import (
	"sync"

	"github.com/Ishan27g/vClock"
)

type Event struct {
	lock   sync.Mutex
	events vClock.Events
	clocks map[string]vClock.EventClock
}

func InitEvents() Event {
	ev := Event{
		lock:   sync.Mutex{},
		events: vClock.NewEventVector(),
		clocks: make(map[string]vClock.EventClock)}
	return ev
}
func (ev *Event) Reset() {
	ev.lock.Lock()
	defer ev.lock.Unlock()
	ev.events = vClock.NewEventVector()
	ev.clocks = make(map[string]vClock.EventClock)
}
func (ev *Event) MergeEvents(events ...vClock.Event) {
	for _, event := range events {
		(*ev).MergeEvent(event.EventId, event.EventClock)
	}
}

func (ev *Event) MergeEvent(id string, clock vClock.EventClock) {
	ev.events.MergeEvent(vClock.Event{EventId: id, EventClock: clock})
}
func (ev *Event) GetOrder() []vClock.Event {
	return ev.events.GetEventsOrder()
}

func (ev *Event) GetOrderedIds() []string {
	events := ev.events
	if events == nil {
		return nil
	}
	var orderedIds []string
	for _, event := range events.GetEventsOrder() {
		orderedIds = append(orderedIds, event.EventId)
	}
	return orderedIds
}
