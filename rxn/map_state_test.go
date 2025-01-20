package rxn_test

import (
	"reflect"
	"testing"

	"reduction.dev/reduction-go/rxn"
)

func TestMapState_PutMutation(t *testing.T) {
	state := rxn.NewMapState("id", codec)
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
	state := rxn.NewMapState("id", codec)
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
	state := rxn.NewMapState("id", codec)
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
