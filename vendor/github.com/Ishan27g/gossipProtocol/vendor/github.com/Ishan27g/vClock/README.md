
### Vector-clock based event ordering

```go
package main

import "github.com/Ishan27gOrg/vClock"

// VectorClock : provides interface to implement vector clock
type VectorClock interface {
	// SendEvent returns the current vector clock after first updating self clock and then
	// updating the individual clocks for corresponding addresses
	SendEvent(id string, addresses []string) vClock.EventClock
	// Get returns the current vector clock
	Get(id string) vClock.EventClock
	// ReceiveEvent updates the current vector clock using element wise maximum with the passed vector clock
	ReceiveEvent(id string, v vClock.EventClock)
	Clear(id string)
	print()
}
// Events : provides interface to merge and order events based on vector clocks
type Events interface {
	// MergeEvents merges the current event clocks with received event clocks, new events are added to current list
	MergeEvents(es ...vClock.Event)
	// MergeEvent takes an eventId & clock and merges with existing clock, new events are added to current list
	MergeEvent(e vClock.Event)
	// GetCurrentEvents returns the events currently saved. Not in order
	GetCurrentEvents() []vClock.Event
	// GetEventsOrder returns the eventIds ordered according to vector clock for the events
	GetEventsOrder() (ids []string)
}

```
