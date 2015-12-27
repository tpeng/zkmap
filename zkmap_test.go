package zkmap

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestZKmap(t *testing.T) {
	m, err := New("127.0.0.1:2181", "/zkmap1")
	defer m.Delete()

	assert.NoError(t, err)
	assert.NotNil(t, m)

	err = m.Set("key1", "hello")
	assert.NoError(t, err)

	v, err := m.Get("key1")
	assert.NoError(t, err)
	assert.Equal(t, "hello", v)
}

func TestZKmapWithInt(t *testing.T) {
	m, err := New("127.0.0.1:2181", "/zkmap1")
	defer m.Delete()

	assert.NoError(t, err)
	assert.NotNil(t, m)

	err = m.Set("key1", 1)
	assert.NoError(t, err)

	v, err := m.Get("key1")
	assert.NoError(t, err)
	assert.Equal(t, 1, v)
}

func TestZKmapWithBool(t *testing.T) {
	m, err := New("127.0.0.1:2181", "/zkmap1")
	defer m.Delete()

	assert.NoError(t, err)
	assert.NotNil(t, m)

	err = m.Set("key1", true)
	assert.NoError(t, err)

	v, err := m.Get("key1")
	assert.NoError(t, err)
	assert.Equal(t, true, v)

	err = m.Set("key2", false)
	assert.NoError(t, err)

	v, err = m.Get("key2")
	assert.NoError(t, err)
	assert.Equal(t, false, v)
}
