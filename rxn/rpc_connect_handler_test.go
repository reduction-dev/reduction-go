package rxn

import (
	"bytes"
	"context"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
	"reduction.dev/reduction-protocol/handlerpb"

	"connectrpc.com/connect"
)

func TestProcessEventBatch_ProcessKeyedEvent(t *testing.T) {
	now := time.Now().UTC()
	rpcHandler := &rpcConnectHandler{
		rxnHandler: &rxnHandler{
			onEventFunc: func(ctx context.Context, subject *Subject, rawEvent []byte) error {
				subject.SetTimer(now.Add(time.Hour))
				subject.AddSinkRequest("test-sink", []byte("test-output"))
				return nil
			},
		},
	}

	got, err := rpcHandler.ProcessEventBatch(context.Background(), connect.NewRequest(&handlerpb.ProcessEventBatchRequest{
		Events: []*handlerpb.Event{{
			Event: &handlerpb.Event_KeyedEvent{
				KeyedEvent: &handlerpb.KeyedEvent{
					Key:       []byte("test-key"),
					Timestamp: timestamppb.New(now),
					Value:     []byte("test-input"),
				},
			},
		}},
	}))
	if err != nil {
		t.Fatalf("ProcessEventBatch() unexpected error: %v", err)
	}

	want := &handlerpb.ProcessEventBatchResponse{
		KeyResults: []*handlerpb.KeyResult{{
			Key: []byte("test-key"),
			NewTimers: []*timestamppb.Timestamp{
				timestamppb.New(now.Add(time.Hour)),
			},
			StateMutationNamespaces: []*handlerpb.StateMutationNamespace{},
		}},
		SinkRequests: []*handlerpb.SinkRequest{{
			Id:    "test-sink",
			Value: []byte("test-output"),
		}},
	}
	assertResponseEqual(t, want, got.Msg)
}

func TestProcessEventBatch_ProcessTimerExpired(t *testing.T) {
	now := time.Now().UTC()
	rpcHandler := &rpcConnectHandler{
		rxnHandler: &rxnHandler{
			onTimerExpiredFunc: func(ctx context.Context, subject *Subject, timer time.Time) error {
				subject.AddSinkRequest("timer-sink", []byte("timer-output"))
				return nil
			},
		},
	}

	got, err := rpcHandler.ProcessEventBatch(context.Background(), connect.NewRequest(&handlerpb.ProcessEventBatchRequest{
		Events: []*handlerpb.Event{{
			Event: &handlerpb.Event_TimerExpired{
				TimerExpired: &handlerpb.TimerExpired{
					Key:       []byte("test-key"),
					Timestamp: timestamppb.New(now),
				},
			},
		}},
	}))
	if err != nil {
		t.Fatalf("ProcessEventBatch() unexpected error: %v", err)
	}

	want := &handlerpb.ProcessEventBatchResponse{
		KeyResults: []*handlerpb.KeyResult{{
			Key:                     []byte("test-key"),
			StateMutationNamespaces: []*handlerpb.StateMutationNamespace{},
		}},
		SinkRequests: []*handlerpb.SinkRequest{{
			Id:    "timer-sink",
			Value: []byte("timer-output"),
		}},
	}
	assertResponseEqual(t, want, got.Msg)
}

func TestProcessEventBatch_ProcessStateMutations(t *testing.T) {
	now := time.Now().UTC()
	rpcHandler := &rpcConnectHandler{
		rxnHandler: &rxnHandler{
			onEventFunc: func(ctx context.Context, subject *Subject, rawEvent []byte) error {
				state := NewMapState("test-state", MapStringIntCodec{})
				if err := subject.LoadState(state); err != nil {
					return err
				}
				state.Set("count", 42)
				return subject.UpdateState(state)
			},
		},
	}

	got, err := rpcHandler.ProcessEventBatch(context.Background(), connect.NewRequest(&handlerpb.ProcessEventBatchRequest{
		Events: []*handlerpb.Event{{
			Event: &handlerpb.Event_KeyedEvent{
				KeyedEvent: &handlerpb.KeyedEvent{
					Key:       []byte("test-key"),
					Timestamp: timestamppb.New(now),
					Value:     []byte("test-input"),
				},
			},
		}},
		KeyStates: []*handlerpb.KeyState{{
			Key: []byte("test-key"),
			StateEntryNamespaces: []*handlerpb.StateEntryNamespace{{
				Namespace: "test-state",
				Entries: []*handlerpb.StateEntry{{
					Key:   []byte("count"),
					Value: []byte{0}, // Initial count of 0
				}},
			}},
		}},
	}))
	if err != nil {
		t.Fatalf("ProcessEventBatch() unexpected error: %v", err)
	}

	want := &handlerpb.ProcessEventBatchResponse{
		KeyResults: []*handlerpb.KeyResult{{
			Key: []byte("test-key"),
			StateMutationNamespaces: []*handlerpb.StateMutationNamespace{{
				Namespace: "test-state",
				Mutations: []*handlerpb.StateMutation{{
					Mutation: &handlerpb.StateMutation_Put{
						Put: &handlerpb.PutMutation{
							Key:   []byte("count"),
							Value: []byte{42},
						},
					},
				}},
			}},
		}},
	}
	assertResponseEqual(t, want, got.Msg)
}

func TestProcessEventBatch_ProcessMultipleEventsWithState(t *testing.T) {
	now := time.Now().UTC()
	rpcHandler := &rpcConnectHandler{
		rxnHandler: &rxnHandler{
			onEventFunc: func(ctx context.Context, subject *Subject, rawEvent []byte) error {
				state := NewMapState("test-state", MapStringIntCodec{})
				if err := subject.LoadState(state); err != nil {
					return err
				}

				// Read counter from state or start at 0
				counter, _ := state.Get("counter")
				counter++
				state.Set("counter", counter)
				return subject.UpdateState(state)
			},
		},
	}

	got, err := rpcHandler.ProcessEventBatch(context.Background(), connect.NewRequest(&handlerpb.ProcessEventBatchRequest{
		Events: []*handlerpb.Event{{
			Event: &handlerpb.Event_KeyedEvent{
				KeyedEvent: &handlerpb.KeyedEvent{
					Key:       []byte("test-key"),
					Timestamp: timestamppb.New(now),
					Value:     []byte("first"),
				},
			},
		}, {
			Event: &handlerpb.Event_KeyedEvent{
				KeyedEvent: &handlerpb.KeyedEvent{
					Key:       []byte("test-key"),
					Timestamp: timestamppb.New(now),
					Value:     []byte("second"),
				},
			},
		}},
		KeyStates: []*handlerpb.KeyState{{
			Key: []byte("test-key"),
			StateEntryNamespaces: []*handlerpb.StateEntryNamespace{{
				Namespace: "test-state",
				Entries: []*handlerpb.StateEntry{{
					Key:   []byte("counter"),
					Value: []byte{0},
				}},
			}},
		}},
	}))
	if err != nil {
		t.Fatalf("ProcessEventBatch() unexpected error: %v", err)
	}

	// Expect two KeyResults, one for each event
	want := &handlerpb.ProcessEventBatchResponse{
		KeyResults: []*handlerpb.KeyResult{{
			Key: []byte("test-key"),
			StateMutationNamespaces: []*handlerpb.StateMutationNamespace{{
				Namespace: "test-state",
				Mutations: []*handlerpb.StateMutation{{
					Mutation: &handlerpb.StateMutation_Put{
						Put: &handlerpb.PutMutation{
							Key:   []byte("counter"),
							Value: []byte{2},
						},
					},
				}},
			}},
		}},
	}
	assertResponseEqual(t, want, got.Msg)
}

func TestProcessEventBatch_EmptyRequest(t *testing.T) {
	rpcHandler := &rpcConnectHandler{rxnHandler: &rxnHandler{}}

	got, err := rpcHandler.ProcessEventBatch(context.Background(), connect.NewRequest(&handlerpb.ProcessEventBatchRequest{}))
	if err != nil {
		t.Fatalf("ProcessEventBatch() unexpected error: %v", err)
	}

	want := &handlerpb.ProcessEventBatchResponse{}
	assertResponseEqual(t, want, got.Msg)
}

func TestProcessEventBatch_MixedEventTypes(t *testing.T) {
	now := time.Now().UTC()
	rpcHandler := &rpcConnectHandler{
		rxnHandler: &rxnHandler{
			onEventFunc: func(ctx context.Context, subject *Subject, rawEvent []byte) error {
				subject.AddSinkRequest("keyed-sink", []byte("keyed-output"))
				return nil
			},
			onTimerExpiredFunc: func(ctx context.Context, subject *Subject, timer time.Time) error {
				subject.AddSinkRequest("timer-sink", []byte("timer-output"))
				return nil
			},
		},
	}

	got, err := rpcHandler.ProcessEventBatch(context.Background(), connect.NewRequest(&handlerpb.ProcessEventBatchRequest{
		Events: []*handlerpb.Event{{
			Event: &handlerpb.Event_KeyedEvent{
				KeyedEvent: &handlerpb.KeyedEvent{
					Key:       []byte("test-key"),
					Timestamp: timestamppb.New(now),
					Value:     []byte("test-input"),
				},
			},
		}, {
			Event: &handlerpb.Event_TimerExpired{
				TimerExpired: &handlerpb.TimerExpired{
					Key:       []byte("test-key"),
					Timestamp: timestamppb.New(now),
				},
			},
		}},
	}))
	if err != nil {
		t.Fatalf("ProcessEventBatch() unexpected error: %v", err)
	}

	want := &handlerpb.ProcessEventBatchResponse{
		KeyResults: []*handlerpb.KeyResult{{
			Key:                     []byte("test-key"),
			StateMutationNamespaces: []*handlerpb.StateMutationNamespace{},
		}},
		SinkRequests: []*handlerpb.SinkRequest{{
			Id:    "keyed-sink",
			Value: []byte("keyed-output"),
		}, {
			Id:    "timer-sink",
			Value: []byte("timer-output"),
		}},
	}
	assertResponseEqual(t, want, got.Msg)
}

func TestProcessEventBatch_MultipleKeys(t *testing.T) {
	now := time.Now().UTC()
	rpcHandler := &rpcConnectHandler{
		rxnHandler: &rxnHandler{
			onEventFunc: func(ctx context.Context, subject *Subject, rawEvent []byte) error {
				state := NewMapState("test-state", MapStringIntCodec{})
				if err := subject.LoadState(state); err != nil {
					return err
				}
				count, _ := state.Get("count")
				state.Set("count", count+1)
				return subject.UpdateState(state)
			},
		},
	}

	got, err := rpcHandler.ProcessEventBatch(context.Background(), connect.NewRequest(&handlerpb.ProcessEventBatchRequest{
		Events: []*handlerpb.Event{{
			Event: &handlerpb.Event_KeyedEvent{
				KeyedEvent: &handlerpb.KeyedEvent{
					Key:       []byte("key-1"),
					Timestamp: timestamppb.New(now),
				},
			},
		}, {
			Event: &handlerpb.Event_KeyedEvent{
				KeyedEvent: &handlerpb.KeyedEvent{
					Key:       []byte("key-2"),
					Timestamp: timestamppb.New(now),
				},
			},
		}},
		KeyStates: []*handlerpb.KeyState{{
			Key: []byte("key-1"),
			StateEntryNamespaces: []*handlerpb.StateEntryNamespace{{
				Namespace: "test-state",
				Entries: []*handlerpb.StateEntry{{
					Key:   []byte("count"),
					Value: []byte{1},
				}},
			}},
		}, {
			Key: []byte("key-2"),
			StateEntryNamespaces: []*handlerpb.StateEntryNamespace{{
				Namespace: "test-state",
				Entries: []*handlerpb.StateEntry{{
					Key:   []byte("count"),
					Value: []byte{2},
				}},
			}},
		}},
	}))
	if err != nil {
		t.Fatalf("ProcessEventBatch() unexpected error: %v", err)
	}

	want := &handlerpb.ProcessEventBatchResponse{
		KeyResults: []*handlerpb.KeyResult{{
			Key: []byte("key-1"),
			StateMutationNamespaces: []*handlerpb.StateMutationNamespace{{
				Namespace: "test-state",
				Mutations: []*handlerpb.StateMutation{{
					Mutation: &handlerpb.StateMutation_Put{
						Put: &handlerpb.PutMutation{
							Key:   []byte("count"),
							Value: []byte{2},
						},
					},
				}},
			}},
		}, {
			Key: []byte("key-2"),
			StateMutationNamespaces: []*handlerpb.StateMutationNamespace{{
				Namespace: "test-state",
				Mutations: []*handlerpb.StateMutation{{
					Mutation: &handlerpb.StateMutation_Put{
						Put: &handlerpb.PutMutation{
							Key:   []byte("count"),
							Value: []byte{3},
						},
					},
				}},
			}},
		}},
	}
	assertResponseEqual(t, want, got.Msg)
}

// Helper functions for comparing responses
func assertResponseEqual(t *testing.T, want, got *handlerpb.ProcessEventBatchResponse) {
	t.Helper()
	if !reflect.DeepEqual(want.SinkRequests, got.SinkRequests) {
		t.Errorf("SinkRequests want: %v, got: %v", want.SinkRequests, got.SinkRequests)
	}

	if len(want.KeyResults) != len(got.KeyResults) {
		t.Fatalf("KeyResults length want: %d, got %d", len(want.KeyResults), len(got.KeyResults))
	}

	// Sort KeyResults by Key before comparison
	slices.SortFunc(want.KeyResults, func(a, b *handlerpb.KeyResult) int {
		return bytes.Compare(a.Key, b.Key)
	})
	slices.SortFunc(got.KeyResults, func(a, b *handlerpb.KeyResult) int {
		return bytes.Compare(a.Key, b.Key)
	})

	for i := range want.KeyResults {
		assertKeyResultEqual(t, want.KeyResults[i], got.KeyResults[i])
	}
}

func assertKeyResultEqual(t *testing.T, want, got *handlerpb.KeyResult) {
	t.Helper()
	if !reflect.DeepEqual(want.Key, got.Key) {
		t.Errorf("Key want: %s, got: %s", string(want.Key), string(got.Key))
	}

	if len(want.NewTimers) > 0 {
		if len(want.NewTimers) != len(got.NewTimers) {
			t.Errorf("NewTimers length want: %d, got: %d", len(want.NewTimers), len(got.NewTimers))
			return
		}

		for j := range want.NewTimers {
			wantTime := want.NewTimers[j].AsTime()
			gotTime := got.NewTimers[j].AsTime()
			if !wantTime.Equal(gotTime) {
				t.Errorf("NewTimer[%d] want: %v, got: %v", j, wantTime, gotTime)
			}
		}
	}

	// Sort namespaces and their mutations before comparison
	sortStateMutationNamespaces(want.StateMutationNamespaces)
	sortStateMutationNamespaces(got.StateMutationNamespaces)

	if !reflect.DeepEqual(want.StateMutationNamespaces, got.StateMutationNamespaces) {
		t.Errorf("StateMutationNamespaces want: %v, got: %v", want.StateMutationNamespaces, got.StateMutationNamespaces)
	}
}

func sortStateMutationNamespaces(namespaces []*handlerpb.StateMutationNamespace) {
	slices.SortFunc(namespaces, func(a, b *handlerpb.StateMutationNamespace) int {
		return strings.Compare(a.Namespace, b.Namespace)
	})

	for _, ns := range namespaces {
		slices.SortFunc(ns.Mutations, func(a, b *handlerpb.StateMutation) int {
			keyA := getStateMutationKey(a)
			keyB := getStateMutationKey(b)
			return bytes.Compare(keyA, keyB)
		})
	}
}

func getStateMutationKey(mutation *handlerpb.StateMutation) []byte {
	switch m := mutation.Mutation.(type) {
	case *handlerpb.StateMutation_Put:
		return m.Put.Key
	case *handlerpb.StateMutation_Delete:
		return m.Delete.Key
	default:
		return nil
	}
}

type rxnHandler struct {
	onEventFunc        func(ctx context.Context, subject *Subject, rawEvent []byte) error
	onTimerExpiredFunc func(ctx context.Context, subject *Subject, timer time.Time) error
	keyEventFunc       func(ctx context.Context, rawEvent []byte) ([]KeyedEvent, error)
}

func (m *rxnHandler) OnEvent(ctx context.Context, subject *Subject, rawEvent []byte) error {
	if m.onEventFunc != nil {
		return m.onEventFunc(ctx, subject, rawEvent)
	}
	return nil
}

func (m *rxnHandler) OnTimerExpired(ctx context.Context, subject *Subject, timer time.Time) error {
	if m.onTimerExpiredFunc != nil {
		return m.onTimerExpiredFunc(ctx, subject, timer)
	}
	return nil
}

func (m *rxnHandler) KeyEvent(ctx context.Context, rawEvent []byte) ([]KeyedEvent, error) {
	if m.keyEventFunc != nil {
		return m.keyEventFunc(ctx, rawEvent)
	}
	return nil, nil
}

// A Codec for a map[string]int
type MapStringIntCodec struct{}

func (m MapStringIntCodec) EncodeKey(key string) ([]byte, error) {
	return []byte(key), nil
}

func (m MapStringIntCodec) DecodeKey(data []byte) (string, error) {
	return string(data), nil
}

func (m MapStringIntCodec) EncodeValue(value int) ([]byte, error) {
	return []byte{byte(value)}, nil
}

func (m MapStringIntCodec) DecodeValue(data []byte) (int, error) {
	return int(data[0]), nil
}

var _ MapStateCodec[string, int] = MapStringIntCodec{}
