package rxn_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"reduction.dev/reduction-go/internal"
	"reduction.dev/reduction-go/rxn"
)

func TestValueState(t *testing.T) {
	// Test integer types
	testValueStateRoundTrip(t, "test-int32", int32(42))
	testValueStateRoundTrip(t, "test-int64", int64(42))
	testValueStateRoundTrip(t, "test-int", int(42))

	// Test unsigned integer types
	testValueStateRoundTrip(t, "test-uint32", uint32(42))
	testValueStateRoundTrip(t, "test-uint64", uint64(42))
	testValueStateRoundTrip(t, "test-uint", uint(42))

	// Test floating point types
	testValueStateRoundTrip(t, "test-float32", float32(3.14))
	testValueStateRoundTrip(t, "test-float64", float64(3.14159))

	// Test other scalar types
	testValueStateRoundTrip(t, "test-string", "hello world")
	testValueStateRoundTrip(t, "test-bool", true)

	// Test time values
	now := time.Now().UTC().Truncate(time.Microsecond)
	testValueStateRoundTrip(t, "test-time", now)
	testValueStateRoundTrip(t, "test-time-zero", time.Time{})
}

func TestValueState_Name(t *testing.T) {
	v := rxn.NewValueState("test-name", rxn.ScalarCodec[int]{})
	assert.Equal(t, "test-name", v.Name(), "name should match the value provided to NewValueState")
}

func TestValueState_Drop(t *testing.T) {
	// Initialize value state with a value
	v := rxn.NewValueState("test-drop", rxn.ScalarCodec[int]{})
	encoded, err := rxn.ScalarCodec[int]{}.EncodeValue(42)
	assert.NoError(t, err, "encoding initial value should not error")

	err = v.Load([]rxn.StateEntry{{Value: encoded}})
	assert.NoError(t, err, "loading initial value should not error")
	assert.Equal(t, 42, v.Value(), "initial value should be set")

	// Drop the value
	v.Drop()

	// Verify mutations contain a delete mutation
	mutations, err := v.Mutations()
	assert.NoError(t, err, "getting mutations should not error")
	assert.Len(t, mutations, 1, "should have exactly one mutation")

	deleteMutation, ok := mutations[0].(*rxn.DeleteMutation)
	assert.True(t, ok, "mutation should be a DeleteMutation")
	assert.Equal(t, []byte("test-drop"), deleteMutation.Key, "delete mutation key should match state name")

	// Verify value is zeroed
	assert.Equal(t, 0, v.Value(), "value should be zeroed after drop")
}

func TestValueState_IncrementMultipleEvents(t *testing.T) {
	// Initialize state
	v := rxn.NewValueState("test-counter", rxn.ScalarCodec[int]{})
	err := v.Load([]rxn.StateEntry{})
	assert.NoError(t, err, "loading empty state should not error")

	// First event - increment from 0 to 1
	v.Set(v.Value() + 1)
	assert.Equal(t, 1, v.Value(), "value should be 1 after first increment")

	// Second event - increment from 1 to 2
	v.Set(v.Value() + 1)
	assert.Equal(t, 2, v.Value(), "value should be 2 after second increment")

	// Verify mutations reflect the final value
	mutations, err := v.Mutations()
	assert.NoError(t, err, "getting mutations should not error")
	assert.Len(t, mutations, 1, "should have exactly one mutation")

	putMutation := mutations[0].(*rxn.PutMutation)
	decodedValue, err := rxn.ScalarCodec[int]{}.DecodeValue(putMutation.Value)
	assert.NoError(t, err, "decoding mutation value should not error")
	assert.Equal(t, 2, decodedValue, "mutation should contain final value of 2")
}

// testValueStateRoundTrip is a helper function that tests the complete round-trip of a ValueState:
// 1. Initialize with empty state
// 2. Set a value and get mutations
// 3. Initialize a new instance with those mutations
// 4. Verify the value matches
func testValueStateRoundTrip[T internal.ProtoScalar](t *testing.T, name string, testValue T) {
	t.Helper()

	// Initialize first value with empty state
	v1 := rxn.NewValueState(name, rxn.ScalarCodec[T]{})
	err := v1.Load([]rxn.StateEntry{})
	assert.NoError(t, err, "loading empty state should not error")

	// Set value and get mutations
	v1.Set(testValue)
	mutations, err := v1.Mutations()
	assert.NoError(t, err, "getting mutations should not error")
	assert.Len(t, mutations, 1, "should have exactly one mutation")

	putMutation := mutations[0].(*rxn.PutMutation)

	// Initialize second value with mutation data
	v2 := rxn.NewValueState(name, rxn.ScalarCodec[T]{})
	err = v2.Load([]rxn.StateEntry{{Value: putMutation.Value}})
	assert.NoError(t, err, "loading mutation value should not error")

	// Verify values match
	assert.Equal(t, testValue, v2.Value(), "round-trip value should match original")
}
