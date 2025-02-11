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
