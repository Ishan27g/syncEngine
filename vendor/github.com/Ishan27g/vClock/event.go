package vClock

import (
	"github.com/emirpasic/gods/lists/arraylist"
	"github.com/iancoleman/orderedmap"
)

// Events : provides interface for a process with lease/leader-role
// receives `new-events` from followers with eventId and followers vector clock
type Events interface {
	// MergeEvents merges the current event clocks with received event clocks.
	// New events are added to current list
	MergeEvents(es ...Event)
	// MergeEvent takes an eventId & clock and merges with existing clock
	// New events are added to current list
	MergeEvent(e Event)
	// GetCurrentEvents returns the events currently saved. Not in order
	GetCurrentEvents() []Event
	// GetEventsOrder returns the eventIds ordered according to vector clock for the events
	GetEventsOrder() (orderedEvents []Event)
}

// Event contains value for eventClocks tree
type Event struct {
	EventId    string     `json:"event_id"`
	EventClock EventClock `json:"event_clock"`
}

// all events
type events struct {
	clocks *orderedmap.OrderedMap
}

func (e *events) GetCurrentEvents() []Event {
	var events []Event
	keys := e.clocks.Keys()
	for _, k := range keys {
		clock, _ := e.clocks.Get(k)
		events = append(events, Event{
			EventId:    k,
			EventClock: clock.(Event).EventClock,
		})
	}
	return events
}

func (e *events) MergeEvents(ev ...Event) {
	for _, c := range ev {
		e.MergeEvent(c)
	}
}

// merge entries in v1 with those found in v2
func merge(v1, v2 EventClock) EventClock {
	v := make(EventClock)
	for s, i := range v1 {
		if v2[s] == 0 && i != 0 { // in v1 and not in v2
			v[s] = i
		}
		if v2[s] < i { // in v1 and not in v2
			v[s] = i
		} else {
			v[s] = v2[s]
		}
	}
	return v
}

// MergeClocks merges the current event clock with the provided event clock.
// unique entries from both clocks are kept
func MergeClocks(v1 EventClock, v2 EventClock) *EventClock {
	v := merge(v1, v2)
	v = merge(v2, v)
	return &v
}

func newEvent(eventIdOrHash string, v2 EventClock) Event {
	return Event{
		EventId:    eventIdOrHash,
		EventClock: v2,
	}
}

func (e *events) MergeEvent(ev Event) {
	// check if present with another vectorClock
	v1, found := e.clocks.Get(ev.EventId)
	if !found {
		// new entry
		e.clocks.Set(ev.EventId, newEvent(ev.EventId, ev.EventClock))
		return
	}
	// get existing EventClock
	v := v1.(Event)
	c1 := compareClock(v.EventClock, ev.EventClock)
	c2 := compareClock(ev.EventClock, v.EventClock)
	if c1 && c2 { // both are same
		return
	}
	// merge v1 and v2
	v.EventClock = *v.EventClock.mergeWith(ev.EventClock)
	// update eventClocks
	e.clocks.Set(ev.EventId, v)
}

func (e *events) GetEventsOrder() []Event {
	a := arraylist.New()
	keys := e.clocks.Keys()
	for _, k := range keys {
		clock, _ := e.clocks.Get(k)
		a.Add(clock.(Event))
	}
	a.Sort(eventComparator)
	var events []Event
	a.Each(func(_ int, value interface{}) {
		ec := value.(Event)
		events = append(events, ec)
	})
	return events
}

var eventComparator = func(a, b interface{}) int {
	v1 := a.(Event)
	v2 := b.(Event)
	c1 := compareClock(v1.EventClock, v2.EventClock)
	c2 := compareClock(v2.EventClock, v1.EventClock)
	if c1 && c2 { // both are same
		return 0
	} else if c1 && !c2 { // e1 happened before
		return -1
	} else {
		return 1
	}
}

func NewEventVector() Events {
	e := events{
		clocks: orderedmap.New(),
	}
	return &e
}

// compareClock returns true if v1 is before or concurrent to v2
func compareClock(v1, v2 EventClock) bool {
	v1IsBefore := true

	v1 = *v1.mergeWith(v2)

	for addr, v1Clock := range v1 {
		v2Clock := v2[addr]
		if v2Clock < v1Clock {
			v1IsBefore = false
		}
	}
	for addr, v2Clock := range v2 {
		v1Clock := v1[addr]
		if v2Clock < v1Clock {
			v1IsBefore = false
		}
	}
	return v1IsBefore
}
