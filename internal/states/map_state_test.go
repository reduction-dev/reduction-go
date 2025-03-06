package states_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"reduction.dev/reduction-go/internal"
	"reduction.dev/reduction-go/internal/states"
)

func TestMapState_Name(t *testing.T) {
	state := states.NewMapState("test-name", codec)
	assert.Equal(t, "test-name", state.Name())
}

func TestMapState_PutMutation(t *testing.T) {
	state := states.NewMapState("id", codec)
	state.Set("k1", "v1")

	mutations, err := state.Mutations()
	assert.NoError(t, err, "getting mutations should not error")
	assert.Len(t, mutations, 1, "should have exactly one mutation")
	assert.Equal(t, &internal.PutMutation{
		Key:   []byte("k1"),
		Value: []byte("v1"),
	}, mutations[0].(*internal.PutMutation))
}

func TestMapState_DeleteMutation(t *testing.T) {
	state := states.NewMapState("id", codec)
	err := state.Load([]internal.StateEntry{{
		Key:   []byte("k1"),
		Value: []byte("v1"),
	}})
	assert.NoError(t, err, "loading initial state should not error")
	state.Delete("k1")

	mutations, err := state.Mutations()
	assert.NoError(t, err, "getting mutations should not error")
	assert.Len(t, mutations, 1, "should have exactly one mutation")
	assert.Equal(t, &internal.DeleteMutation{
		Key: []byte("k1"),
	}, mutations[0].(*internal.DeleteMutation))
}

func TestMapState_All(t *testing.T) {
	state := states.NewMapState("id", codec)
	err := state.Load([]internal.StateEntry{{
		Key: []byte("unchanged"), Value: []byte("unchanged"),
	}, {
		Key: []byte("modified"), Value: []byte("to-be-modified"),
	}, {
		Key: []byte("deleted"), Value: []byte("to-be-deleted"),
	}})
	assert.NoError(t, err, "loading initial state should not error")

	state.Set("added", "added")
	state.Set("modified", "modified")
	state.Delete("deleted")

	got := make(map[string]string)
	for k, v := range state.All() {
		got[k] = v
	}
	assert.Equal(t, map[string]string{
		"unchanged": "unchanged",
		"modified":  "modified",
		"added":     "added",
	}, got)
}

func TestMapState_Size(t *testing.T) {
	state := states.NewMapState("test", codec)

	// Test empty map
	assert.Equal(t, 0, state.Size(), "empty map should have size 0")

	// Test after adding items
	state.Set("k1", "v1")
	state.Set("k2", "v2")
	assert.Equal(t, 2, state.Size(), "map should have size 2 after adding two items")

	// Test after loading items
	state = states.NewMapState("test", codec)
	err := state.Load([]internal.StateEntry{
		{Key: []byte("k1"), Value: []byte("v1")},
		{Key: []byte("k2"), Value: []byte("v2")},
	})
	assert.NoError(t, err, "loading initial state should not error")
	assert.Equal(t, 2, state.Size(), "map should have size 2 after loading two items")

	// Test after deleting items
	state.Delete("k1")
	assert.Equal(t, 1, state.Size(), "map should have size 1 after deleting one item")

	// Test updating existing items
	state = states.NewMapState("test", codec)
	state.Set("k1", "v1")
	state.Set("k1", "v2") // update same key
	assert.Equal(t, 1, state.Size(), "map should have size 1 after updating same key")

	// Test delete then add same key
	state = states.NewMapState("test", codec)
	err = state.Load([]internal.StateEntry{
		{Key: []byte("k1"), Value: []byte("v1")},
	})
	assert.NoError(t, err, "loading second state should not error")
	state.Delete("k1")
	state.Set("k1", "v2")
	assert.Equal(t, 1, state.Size(), "map should have size 1 after delete-then-add of same key")
}

// A Codec for a map[string]string
type MapCodec struct{}

func (m MapCodec) EncodeKey(key string) ([]byte, error) {
	return []byte(key), nil
}

func (m MapCodec) DecodeKey(data []byte) (string, error) {
	return string(data), nil
}

func (m MapCodec) EncodeValue(value string) ([]byte, error) {
	return []byte(value), nil
}

func (m MapCodec) DecodeValue(data []byte) (string, error) {
	return string(data), nil
}

var codec states.MapCodec[string, string] = MapCodec{}
