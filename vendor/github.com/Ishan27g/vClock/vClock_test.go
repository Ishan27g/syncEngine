package vClock

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func mockClock(clock int, ids ...string) EventClock {
	e := make(EventClock)
	for _, id := range ids {
		e[id] = clock
	}
	return e
}

func TestCompareMisc(t *testing.T) {

	t.Run("all events are common in the clock", func(t *testing.T) {
		m1 := mockClock(1, "event1", "event2", "event3")
		m2 := mockClock(1, "event1", "event2", "event3")
		m3 := mockClock(1, "event1", "event2", "event3")
		m4 := mockClock(1, "event1", "event2", "event3")

		assert.True(t, compareClock(m1, m2))
		assert.True(t, compareClock(m2, m3))
		assert.True(t, compareClock(m3, m4))
	})
	t.Run("some events are common", func(t *testing.T) {
		m1 := mockClock(1, "event1", "event2", "event3")
		m2 := mockClock(1, "event1", "event2", "event3")
		m3 := mockClock(1, "event1", "event2", "event3")

		delete(m1, "event2")
		delete(m2, "event1")
		delete(m3, "event3")

		assert.True(t, compareClock(m1, m2))
		assert.True(t, compareClock(m2, m3))
	})

	t.Run("compares only common events", func(t *testing.T) {
		m5 := make(EventClock)
		m5["p1"] = 10
		m5["p2"] = 10
		m5["p3"] = 42

		m6 := make(EventClock)
		m6["p1"] = 10
		m6["p2"] = 11

		assert.True(t, compareClock(m5, m6))
		assert.False(t, compareClock(m6, m5))
	})

}

func TestSendEventEvent(t *testing.T) {
	p1 := Init("p1")
	p1.SendEvent("event", []string{"p2"})
	assert.Equal(t, 1, p1.Get("event")["p1"])
	assert.Equal(t, 1, p1.Get("event")["p2"])
	p1.Clear("event")
}
func TestClocksMatch(t *testing.T) {

	p1 := Init("p1")
	p2 := Init("p2")
	for i := 0; i < 100; i++ {
		// p1 sends vClock to p2
		p1.SendEvent("event", []string{"p2"})
		// p2 receives vClock from p1
		p2.ReceiveEvent("event", p1.Get("event"))
	}
	// both clocks match
	assert.Equal(t, p1.Get("event"), p2.Get("event"))
	p1.Clear("event")
	p2.Clear("event")
}

func TestCompareClocks(t *testing.T) {
	var ids = []string{"event1", "event2", "event3", "event4"}
	for i := 0; i < 100; i++ {
		m1 := mockClock(i, ids...)
		m2 := mockClock(i+1, ids...)
		m3 := mockClock(i+1, ids[1:]...)
		m4 := mockClock(i+2, ids[2:]...)
		assert.True(t, compareClock(m1, m2))
		assert.True(t, compareClock(m2, m3))
		assert.True(t, compareClock(m3, m4))
		assert.False(t, compareClock(m4, m1))
	}
}
func TestCompareDuplicateClocks(t *testing.T) {
	var ids = []string{"event1", "event2", "event3", "event4"}
	for i := 1; i < 11; i++ {
		m1 := mockClock(i, ids...)
		m2 := m1
		assert.True(t, compareClock(m1, m2))
	}
}
