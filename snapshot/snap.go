package snapshot

import (
	"fmt"
	"os"
	"time"
)

type Manager struct {
	RoundNum int
	Count    int
	SnapShot // interface to persist data to file

	LastSnapshotHash string
}
type SnapShot interface {
	// clear the current snapshot & file
	clear()
	// Get all entries
	Get() []Entry
	// get entries in the snapshot that were added after this timestamp, for testing
	get(after time.Time) []Entry
	// Apply a slice of entries to the snapshot
	Apply(...Entry)
	apply(e Entry)
	// Save the current entries to file and clear entries from memory
	Save()
	Round()
	Sync(...Entry)
}

type Entry struct {
	CreatedAt time.Time
	Data      string
	Round     int
}
type snapShot struct {
	entries   []Entry
	file      string
	readLen   int64
	saveCount int
	roundNum  int
}

func (s *snapShot) Sync(entry ...Entry) {
	s.clear()
	s.roundNum = 0
	for _, e := range entry {
		if e.Round == s.roundNum {
			s.entries = append(s.entries, e)
		} else {
			s.roundNum = e.Round
			s.entries = append(s.entries, e)
		}
	}
	s.Save()
	s.readLen = getSize(s.file)
}

func (s *snapShot) Round() {
	s.readLen = getSize(s.file)
	s.roundNum++
}

func (s *snapShot) Save() {
	if len(s.entries) != s.saveCount {
		s.writeToFile()
		s.clear()
	}
}

func (s *snapShot) Apply(entries ...Entry) {
	for _, e := range entries {
		s.apply(e)
	}
}

// apply this Entry to the local snapshot for current round
func (s *snapShot) apply(e Entry) {
	e.Round = s.roundNum
	s.entries = append(s.entries, e)
}

// writeToFile the current round entries to file
func (s *snapShot) writeToFile() {
	write(s.file, s.readLen, &s.entries)
}
func (s *snapShot) read() {
	e, rl := read(s.file)
	if e != nil && rl > 0 {
		s.entries = *e
		s.readLen = rl
		s.roundNum = s.entries[len(*e)-1].Round
	}
}

// get entries in the snapshot that were added after this timestamp
func (s *snapShot) get(from time.Time) []Entry {
	var entries []Entry
	for i := 0; i < len(s.entries); i++ {
		if s.entries[i].CreatedAt.After(from) {
			entries = append(entries, s.entries[i])
		}
	}
	return entries
}

// Get all entries in the snapshot
func (s *snapShot) Get() []Entry {
	return s.entries
}

//Clear the current snapshot & file
func (s *snapShot) clear() {
	s.entries = nil
	s.saveCount = 0
}
func Empty(fileName string) SnapShot {
	_ = os.Remove(fileName)
	snap := empty(fileName)
	return snap
}
func FromFile(fileName string) SnapShot {
	snap := empty(fileName)
	snap.read()
	return snap
}
func empty(fileName string) *snapShot {
	snap := snapShot{
		entries:   []Entry{},
		file:      fileName,
		saveCount: 0,
	}
	return &snap
}

func NewEntry(createdAt time.Time, data interface{}) Entry {
	return Entry{
		CreatedAt: createdAt,
		Data:      fmt.Sprintf("%v", data),
	}
}

func now() time.Time {
	t, _ := time.Parse(time.RFC3339Nano, time.Now().Format("2006-01-02T15:04:05.999999999Z07:00"))
	return t
}
