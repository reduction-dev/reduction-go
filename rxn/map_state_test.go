package rxn_test

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"reduction.dev/reduction-go/rxn"
)

func TestMapState_PutMutation(t *testing.T) {
	state := rxn.NewMapState("id", rxn.WithCodec(codec))
	state.Set("k1", "v1")

	mutations, err := state.Mutations()
	if err != nil {
		t.Fatalf("err getting mutations: %v", err)
	}
	if len(mutations) != 1 {
		t.Fatalf("want 1 mutation, got %d", len(mutations))
	}

	got := mutations[0].(*rxn.PutMutation)
	want := &rxn.PutMutation{
		Key:   []byte("k1"),
		Value: []byte("v1"),
	}
	if !reflect.DeepEqual(want, got) {
		t.Errorf("want: %+v; got: %+v", want, got)
	}
}

func TestMapState_DeleteMutation(t *testing.T) {
	state := rxn.NewMapState("id", rxn.WithCodec(codec))
	state.Load([]rxn.StateEntry{{
		Key:   []byte("k1"),
		Value: []byte("v1"),
	}})
	state.Delete("k1")

	mutations, err := state.Mutations()
	if err != nil {
		t.Fatalf("err getting mutations: %v", err)
	}
	if len(mutations) != 1 {
		t.Fatalf("want 1 mutation, got %d", len(mutations))
	}

	got := mutations[0].(*rxn.DeleteMutation)
	want := &rxn.DeleteMutation{
		Key: []byte("k1"),
	}
	if !reflect.DeepEqual(want, got) {
		t.Errorf("want: %+v; got: %+v", want, got)
	}
}

func TestMapState_All(t *testing.T) {
	state := rxn.NewMapState("id", rxn.WithCodec(codec))
	state.Load([]rxn.StateEntry{{
		Key: []byte("unchanged"), Value: []byte("unchanged"),
	}, {
		Key: []byte("modified"), Value: []byte("to-be-modified"),
	}, {
		Key: []byte("deleted"), Value: []byte("to-be-deleted"),
	}})
	state.Set("added", "added")
	state.Set("modified", "modified")
	state.Delete("deleted")

	got := make(map[string]string)
	for k, v := range state.All() {
		got[k] = v
	}
	want := make(map[string]string)
	want["unchanged"] = "unchanged"
	want["modified"] = "modified"
	want["added"] = "added"
	if !reflect.DeepEqual(want, got) {
		t.Errorf("want: %+v; got: %+v", want, got)
	}
}

func TestMapState_Size(t *testing.T) {
	state := rxn.NewMapState("test", rxn.WithCodec(codec))

	// Test empty map
	assert.Equal(t, 0, state.Size(), "empty map should have size 0")

	// Test after adding items
	state.Set("k1", "v1")
	state.Set("k2", "v2")
	assert.Equal(t, 2, state.Size(), "map should have size 2 after adding two items")

	// Test after loading items
	state = rxn.NewMapState("test", rxn.WithCodec(codec))
	state.Load([]rxn.StateEntry{
		{Key: []byte("k1"), Value: []byte("v1")},
		{Key: []byte("k2"), Value: []byte("v2")},
	})
	assert.Equal(t, 2, state.Size(), "map should have size 2 after loading two items")

	// Test after deleting items
	state.Delete("k1")
	assert.Equal(t, 1, state.Size(), "map should have size 1 after deleting one item")

	// Test updating existing items
	state = rxn.NewMapState("test", rxn.WithCodec(codec))
	state.Set("k1", "v1")
	state.Set("k1", "v2") // update same key
	assert.Equal(t, 1, state.Size(), "map should have size 1 after updating same key")

	// Test delete then add same key
	state = rxn.NewMapState("test", rxn.WithCodec(codec))
	state.Load([]rxn.StateEntry{
		{Key: []byte("k1"), Value: []byte("v1")},
	})
	state.Delete("k1")
	state.Set("k1", "v2")
	assert.Equal(t, 1, state.Size(), "map should have size 1 after delete-then-add of same key")
}

// A Codec for a map[string]string
type MapStateCodec struct{}

func (m MapStateCodec) EncodeKey(key string) ([]byte, error) {
	return []byte(key), nil
}

func (m MapStateCodec) DecodeKey(data []byte) (string, error) {
	return string(data), nil
}

func (m MapStateCodec) EncodeValue(value string) ([]byte, error) {
	return []byte(value), nil
}

func (m MapStateCodec) DecodeValue(data []byte) (string, error) {
	return string(data), nil
}

var codec rxn.MapStateCodec[string, string] = MapStateCodec{}
