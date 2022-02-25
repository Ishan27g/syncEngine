package snapshot

import (
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var fileName = "./file.csv"

func mockTime(tick int) time.Time {
	t := time.Date(2000+tick, time.January, 1, 1, tick, tick, tick*99, time.UTC).Format("2006-01-02T15:04:05.999999999Z07:00")
	tt, _ := time.Parse(time.RFC3339Nano, t)
	return tt
}
func mockEntry(tick int) Entry {
	return Entry{
		CreatedAt: mockTime(tick),
		Data:      "Command/Log - " + strconv.Itoa(tick),
	}
}
func Test_Fops(t *testing.T) {
	const csvFile = "./file.csv"
	var ticks = 10
	var entries []Entry
	for i := 1; i <= ticks; i++ {
		entries = append(entries, mockEntry(i))
	}

	offset := write(csvFile, 0, &entries)
	assert.Equal(t, 490, int(offset))

	entries = nil
	for i := 1; i <= ticks; i++ {
		entries = append(entries, mockEntry(i))
	}

	offsetAgain := write(csvFile, offset, &entries)
	assert.Equal(t, 490*2, int(offsetAgain))

	_, off := read(csvFile)
	assert.Equal(t, off, offsetAgain)
	t.Cleanup(func() {
		_ = os.Remove(csvFile)
	})
}
func TestEmptyApply(t *testing.T) {
	_ = os.Remove(fileName)
	mockEmptySnapshot(t).Save()
	t.Cleanup(func() {
		_ = os.Remove(fileName)
	})
}

func mockEmptySnapshot(t *testing.T) SnapShot {
	var ticks = 10
	var entries []Entry
	for i := 1; i <= ticks; i++ {
		entries = append(entries, mockEntry(i))
	}
	snapshot := Empty(fileName)
	snapshot.Apply(entries...)

	for i := 0; i < ticks; i++ {
		assert.Equal(t, ticks-i, len(snapshot.get(mockTime(i))))
	}

	return snapshot
}

func TestReadApply(t *testing.T) {
	_ = os.Remove(fileName)

	t.Cleanup(func() {
		TestEmptyApply(t)
		assert.FileExists(t, fileName)
		snapshot := FromFile(fileName)
		// _ = os.Remove(dataFile())
		var ticks = 10
		var entries []Entry
		for i := 1; i <= ticks; i++ {
			entries = append(entries, mockEntry(i+100))
		}
		snapshot.Apply(entries...)
		snapshot.Save()
	})
	t.Cleanup(func() {
		_ = os.Remove(fileName)
	})
}

func TestReadRound(t *testing.T) {
	//var fileName = "./Round.json"
	_ = os.Remove(fileName)

	t.Cleanup(func() {
		snap := mockEmptySnapshot(t)
		entries := snap.Get()
		snap.Save()
		snap.Round()
		var ticks = 10
		for i := 1; i <= ticks; i++ {
			nextRoundEntry := mockEntry(i + 100)
			entries = append(entries, nextRoundEntry)
			snap.Apply(nextRoundEntry)
		}
		snap.Save()
		snap.Round()

		assert.FileExists(t, fileName)

		snapshot := FromFile(fileName)
		for i, entry := range snapshot.Get() {
			if i < 10 {
				assert.Equal(t, 0, entry.Round)
			} else {
				assert.Equal(t, 1, entry.Round)
			}
			assert.Equal(t, entries[i].Data, entry.Data)
			assert.Equal(t, entries[i].CreatedAt, entry.CreatedAt)
		}
	})
	t.Cleanup(func() {
		_ = os.Remove(fileName)
	})
}

func TestSnapShot_Sync(t *testing.T) {
	snap := mockEmptySnapshot(t)
	entries := snap.Get()
	snap.Save()
	snap.Round()
	var rounds = 25
	for i := 1; i <= rounds; i++ {
		nextRoundEntry := mockEntry(i + 10)
		snap.Apply(nextRoundEntry)
		entries = append(entries, nextRoundEntry)
		snap.Save()
		snap.Round()
	}
	finalEntries := FromFile(fileName).Get()

	const tmpFile = "newFile.csv"
	newSnap := Empty(tmpFile)
	newSnap.Sync(finalEntries...)

	newEntries := FromFile(tmpFile).Get()
	assert.Equal(t, newEntries, finalEntries)

	t.Cleanup(func() {
		_ = os.Remove(fileName)
		_ = os.Remove(tmpFile)
	})
}
