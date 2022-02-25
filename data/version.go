package data

import "sync"

type VersionAbleI interface {
	// GetVersion returns the version (>= 1) for this identifier or -1 if not found
	GetVersion(id string) int
	// UpdateVersion increments the version for this identifier, starting from 1. Creates a new entry if not found
	UpdateVersion(id ...string)
	// Delete the unused ids
	Delete(ids ...string)
}

type versionAbleS struct {
	mutex sync.Mutex
	v     map[string]*int
}

func (v *versionAbleS) Delete(ids ...string) {
	v.mutex.Lock()
	defer v.mutex.Unlock()
	for _, id := range ids {
		delete(v.v, id)
	}
}
func (v *versionAbleS) GetVersion(id string) int {
	v.mutex.Lock()
	version := v.v[id]
	defer v.mutex.Unlock()
	if version != nil {
		return *version
	}
	return -1
}

func (v *versionAbleS) UpdateVersion(ids ...string) {
	v.mutex.Lock()
	defer v.mutex.Unlock()
	for _, id := range ids {
		v.update(id)
	}
}

func (v *versionAbleS) update(id string) {
	if v.v[id] != nil {
		*v.v[id] = *v.v[id] + 1
	} else {
		version := 1
		v.v[id] = &version
	}
}

func VersionMap() VersionAbleI {
	return &versionAbleS{
		mutex: sync.Mutex{},
		v:     make(map[string]*int),
	}
}
