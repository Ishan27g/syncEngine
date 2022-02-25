package data

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

var data = []string{"a", "b", "c"}

func TestVersions(t *testing.T) {
	v := VersionMap()
	v.UpdateVersion(data...)
	for _, datum := range data {
		assert.Equal(t, 1, v.GetVersion(datum))
	}
	v.UpdateVersion(data...)
	for _, datum := range data {
		assert.Equal(t, 2, v.GetVersion(datum))
	}
	v.Delete(data...)
	for _, datum := range data {
		assert.Equal(t, -1, v.GetVersion(datum))
	}
}
