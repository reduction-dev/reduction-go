package rxn_test

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
	"reduction.dev/reduction-go/connectors/embedded"
	"reduction.dev/reduction-go/connectors/stdio"
	"reduction.dev/reduction-go/internal/rpc"
	"reduction.dev/reduction-go/internal/types"
	"reduction.dev/reduction-go/jobs"
	"reduction.dev/reduction-go/rxn"
	"reduction.dev/reduction-protocol/handlerpb"
	"reduction.dev/reduction-protocol/handlerpb/handlerpbconnect"
)

func TestProcessEventBatch_ProcessKeyedEvent(t *testing.T) {
	now := time.Now().UTC()
	_, client := setupTestServer(t, func(job *jobs.Job, op *jobs.Operator) types.OperatorHandler {
		sink := stdio.NewSink(job, "test-sink")

		return &rxnHandler{
			onEventFunc: func(ctx context.Context, subject *rxn.Subject, rawEvent []byte) error {
				subject.SetTimer(now.Add(time.Hour))
				sink.Collect(ctx, []byte("test-output"))
				return nil
			},
		}
	})

	got, err := client.ProcessEventBatch(context.Background(), connect.NewRequest(&handlerpb.ProcessEventBatchRequest{
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
	require.NoError(t, err, "ProcessEventBatch should not error")

	want := &handlerpb.ProcessEventBatchResponse{
		KeyResults: []*handlerpb.KeyResult{{
			Key: []byte("test-key"),
			NewTimers: []*timestamppb.Timestamp{
				timestamppb.New(now.Add(time.Hour)),
			},
			StateMutationNamespaces: nil,
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
	_, client := setupTestServer(t, func(job *jobs.Job, op *jobs.Operator) types.OperatorHandler {
		sink := stdio.NewSink(job, "timer-sink")
		return &rxnHandler{
			onTimerExpiredFunc: func(ctx context.Context, subject *rxn.Subject, timer time.Time) error {
				sink.Collect(ctx, []byte("timer-output"))
				return nil
			},
		}
	})

	got, err := client.ProcessEventBatch(context.Background(), connect.NewRequest(&handlerpb.ProcessEventBatchRequest{
		Events: []*handlerpb.Event{{
			Event: &handlerpb.Event_TimerExpired{
				TimerExpired: &handlerpb.TimerExpired{
					Key:       []byte("test-key"),
					Timestamp: timestamppb.New(now),
				},
			},
		}},
	}))
	require.NoError(t, err)

	want := &handlerpb.ProcessEventBatchResponse{
		KeyResults: []*handlerpb.KeyResult{{
			Key:                     []byte("test-key"),
			StateMutationNamespaces: nil,
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
	_, client := setupTestServer(t, func(job *jobs.Job, op *jobs.Operator) types.OperatorHandler {
		stateSpec := rxn.NewMapSpec(op, "test-state", MapStringIntCodec{})
		return &rxnHandler{
			onEventFunc: func(ctx context.Context, subject *rxn.Subject, rawEvent []byte) error {
				state := stateSpec.StateFor(subject)
				state.Set("count", 42)
				return nil
			},
		}
	})

	got, err := client.ProcessEventBatch(context.Background(), connect.NewRequest(&handlerpb.ProcessEventBatchRequest{
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
	require.NoError(t, err, "ProcessEventBatch should not error")

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
	_, client := setupTestServer(t, func(job *jobs.Job, op *jobs.Operator) types.OperatorHandler {
		stateSpec := rxn.NewMapSpec(op, "test-state", MapStringIntCodec{})
		return &rxnHandler{
			onEventFunc: func(ctx context.Context, subject *rxn.Subject, rawEvent []byte) error {
				state := stateSpec.StateFor(subject)
				counter, _ := state.Get("counter")
				counter++
				state.Set("counter", counter)
				return nil
			},
		}
	})

	got, err := client.ProcessEventBatch(context.Background(), connect.NewRequest(&handlerpb.ProcessEventBatchRequest{
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
					Key:   []byte("counter"),
					Value: []byte{1},
				}},
			}},
		}, {
			Key: []byte("key-2"),
			StateEntryNamespaces: []*handlerpb.StateEntryNamespace{{
				Namespace: "test-state",
				Entries: []*handlerpb.StateEntry{{
					Key:   []byte("counter"),
					Value: []byte{2},
				}},
			}},
		}},
	}))
	require.NoError(t, err)

	want := &handlerpb.ProcessEventBatchResponse{
		KeyResults: []*handlerpb.KeyResult{{
			Key: []byte("key-1"),
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
		}, {
			Key: []byte("key-2"),
			StateMutationNamespaces: []*handlerpb.StateMutationNamespace{{
				Namespace: "test-state",
				Mutations: []*handlerpb.StateMutation{{
					Mutation: &handlerpb.StateMutation_Put{
						Put: &handlerpb.PutMutation{
							Key:   []byte("counter"),
							Value: []byte{3},
						},
					},
				}},
			}},
		}},
	}
	assertResponseEqual(t, want, got.Msg)
}

func TestProcessEventBatch_EmptyRequest(t *testing.T) {
	_, client := setupTestServer(t, func(job *jobs.Job, op *jobs.Operator) types.OperatorHandler {
		return &rxnHandler{}
	})

	got, err := client.ProcessEventBatch(context.Background(), connect.NewRequest(&handlerpb.ProcessEventBatchRequest{}))
	require.NoError(t, err)

	want := &handlerpb.ProcessEventBatchResponse{}
	assertResponseEqual(t, want, got.Msg)
}

func TestProcessEventBatch_MixedEventTypes(t *testing.T) {
	now := time.Now().UTC()
	_, client := setupTestServer(t, func(job *jobs.Job, op *jobs.Operator) types.OperatorHandler {
		onEventSink := stdio.NewSink(job, "on-event-sink")
		onTimerSink := stdio.NewSink(job, "on-timer-expired-sink")

		return &rxnHandler{
			onEventFunc: func(ctx context.Context, subject *rxn.Subject, rawEvent []byte) error {
				onEventSink.Collect(ctx, []byte("keyed-output"))
				return nil
			},
			onTimerExpiredFunc: func(ctx context.Context, subject *rxn.Subject, timer time.Time) error {
				onTimerSink.Collect(ctx, []byte("timer-output"))
				return nil
			},
		}
	})

	got, err := client.ProcessEventBatch(context.Background(), connect.NewRequest(&handlerpb.ProcessEventBatchRequest{
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
	require.NoError(t, err)

	want := &handlerpb.ProcessEventBatchResponse{
		KeyResults: []*handlerpb.KeyResult{{
			Key:                     []byte("test-key"),
			StateMutationNamespaces: nil, // Changed from empty slice to nil
		}},
		SinkRequests: []*handlerpb.SinkRequest{{
			Id:    "on-event-sink",
			Value: []byte("keyed-output"),
		}, {
			Id:    "on-timer-expired-sink",
			Value: []byte("timer-output"),
		}},
	}
	assertResponseEqual(t, want, got.Msg)
}

func TestProcessEventBatch_DropValueState(t *testing.T) {
	now := time.Now().UTC()
	_, client := setupTestServer(t, func(job *jobs.Job, op *jobs.Operator) types.OperatorHandler {
		stateSpec := rxn.NewValueSpec(op, "test-value", rxn.ScalarCodec[int]{})
		return &rxnHandler{
			onEventFunc: func(ctx context.Context, subject *rxn.Subject, rawEvent []byte) error {
				state := stateSpec.StateFor(subject)
				state.Drop()
				return nil
			},
		}
	})

	// Properly encode the initial value
	initialValue, err := rxn.ScalarCodec[int]{}.EncodeValue(42)
	require.NoError(t, err, "encoding initial value should not error")

	got, err := client.ProcessEventBatch(context.Background(), connect.NewRequest(&handlerpb.ProcessEventBatchRequest{
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
				Namespace: "test-value",
				Entries: []*handlerpb.StateEntry{{
					Value: initialValue,
				}},
			}},
		}},
	}))
	require.NoError(t, err)

	want := &handlerpb.ProcessEventBatchResponse{
		KeyResults: []*handlerpb.KeyResult{{
			Key: []byte("test-key"),
			StateMutationNamespaces: []*handlerpb.StateMutationNamespace{{
				Namespace: "test-value",
				Mutations: []*handlerpb.StateMutation{{
					Mutation: &handlerpb.StateMutation_Delete{
						Delete: &handlerpb.DeleteMutation{
							Key: []byte("test-value"),
						},
					},
				}},
			}},
		}},
	}
	assertResponseEqual(t, want, got.Msg)
}

func TestProcessEventBatch_IncrementValueState(t *testing.T) {
	_, client := setupTestServer(t, func(job *jobs.Job, op *jobs.Operator) types.OperatorHandler {
		stateSpec := rxn.NewValueSpec(op, "counter-state", rxn.ScalarCodec[int]{})
		return &rxnHandler{
			onEventFunc: func(ctx context.Context, subject *rxn.Subject, rawEvent []byte) error {
				state := stateSpec.StateFor(subject)
				state.Set(state.Value() + 1)
				return nil
			},
		}
	})

	got, err := client.ProcessEventBatch(context.Background(), connect.NewRequest(&handlerpb.ProcessEventBatchRequest{
		Events: []*handlerpb.Event{{
			Event: &handlerpb.Event_KeyedEvent{
				KeyedEvent: &handlerpb.KeyedEvent{
					Key: []byte("test-key"),
				},
			},
		}, {
			Event: &handlerpb.Event_KeyedEvent{
				KeyedEvent: &handlerpb.KeyedEvent{
					Key: []byte("test-key"),
				},
			},
		}},
	}))
	require.NoError(t, err)

	encodedValue, err := rxn.ScalarCodec[int]{}.EncodeValue(2)
	require.NoError(t, err)

	want := &handlerpb.ProcessEventBatchResponse{
		KeyResults: []*handlerpb.KeyResult{{
			Key: []byte("test-key"),
			StateMutationNamespaces: []*handlerpb.StateMutationNamespace{{
				Namespace: "counter-state",
				Mutations: []*handlerpb.StateMutation{{
					Mutation: &handlerpb.StateMutation_Put{
						Put: &handlerpb.PutMutation{
							Key:   []byte("counter-state"),
							Value: encodedValue,
						},
					},
				}},
			}},
		}},
	}
	assertResponseEqual(t, want, got.Msg)
}

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

var _ rxn.MapStateCodec[string, int] = MapStringIntCodec{}

// Helper functions for comparing responses
func assertResponseEqual(t *testing.T, want, got *handlerpb.ProcessEventBatchResponse) {
	t.Helper()

	// Sink request order should be preserved
	assert.EqualExportedValues(t, want.SinkRequests, got.SinkRequests)

	// Key results represent a map by key so the order doesn't matter for assertions. Sort KeyResults by key before comparing
	sortKeyResults := func(kr []*handlerpb.KeyResult) {
		slices.SortFunc(kr, func(a, b *handlerpb.KeyResult) int {
			return bytes.Compare(a.Key, b.Key)
		})
	}
	sortKeyResults(want.KeyResults)
	sortKeyResults(got.KeyResults)
	assert.EqualExportedValues(t, want.KeyResults, got.KeyResults)
}

type rxnHandler struct {
	onEventFunc        func(ctx context.Context, subject *rxn.Subject, rawEvent []byte) error
	onTimerExpiredFunc func(ctx context.Context, subject *rxn.Subject, timer time.Time) error
	keyEventFunc       func(ctx context.Context, rawEvent []byte) ([]rxn.KeyedEvent, error)
}

func (m *rxnHandler) OnEvent(ctx context.Context, subject *rxn.Subject, rawEvent []byte) error {
	if m.onEventFunc != nil {
		return m.onEventFunc(ctx, subject, rawEvent)
	}
	return nil
}

func (m *rxnHandler) OnTimerExpired(ctx context.Context, subject *rxn.Subject, timer time.Time) error {
	if m.onTimerExpiredFunc != nil {
		return m.onTimerExpiredFunc(ctx, subject, timer)
	}
	return nil
}

func (m *rxnHandler) KeyEvent(ctx context.Context, rawEvent []byte) ([]rxn.KeyedEvent, error) {
	if m.keyEventFunc != nil {
		return m.keyEventFunc(ctx, rawEvent)
	}
	return nil, nil
}

func setupTestServer(t *testing.T, factory func(*jobs.Job, *jobs.Operator) types.OperatorHandler) (*httptest.Server, handlerpbconnect.HandlerClient) {
	t.Helper()

	job := &jobs.Job{}
	source := embedded.NewSource(job, "test-source", &embedded.SourceParams{})
	operator := jobs.NewOperator(job, "test-operator", &jobs.OperatorParams{
		Handler: func(op *jobs.Operator) types.OperatorHandler {
			return factory(job, op)
		},
	})
	source.Connect(operator)
	synth, err := job.Synthesize()
	if err != nil {
		t.Fatal(err)
	}

	path, handler := handlerpbconnect.NewHandlerHandler(rpc.NewConnectHandler(synth.Handler))

	mux := http.NewServeMux()
	mux.Handle(path, handler)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	client := handlerpbconnect.NewHandlerClient(
		srv.Client(),
		srv.URL,
	)

	return srv, client
}
