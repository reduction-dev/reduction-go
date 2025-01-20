package rxn

import (
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
	"reduction.dev/reduction-handler/handlerpb"
)

// The scoped API for Reduction
type Subject struct {
	// Timers set during a handler method
	timers []time.Time
	// Keyed state that contains several state items: map[<StateID>]<Value>
	state map[string][]StateEntry
	// Mutations applied in a handler method
	stateMutations map[string][]StateMutation
	// The current keyed scope of the handler
	key []byte
	// The current timestamp scope of the handler
	timestamp time.Time
	// Requests to send to sinks
	sinkRequests []*handlerpb.SinkRequest
}

type StateEntry struct {
	Key   []byte
	Value []byte
}

func newSubjectFromOnEventRequest(req *handlerpb.OnEventRequest) *Subject {
	state := make(map[string][]StateEntry, len(req.GetStateEntryNamespaces()))
	for _, namespace := range req.GetStateEntryNamespaces() {
		entries := make([]StateEntry, len(namespace.Entries))
		for i, entry := range namespace.Entries {
			entries[i] = StateEntry{Key: entry.Key, Value: entry.Value}
		}
		state[namespace.Namespace] = entries
	}

	return &Subject{
		key:            req.Event.Key,
		timestamp:      req.Event.Timestamp.AsTime(),
		state:          state,
		stateMutations: make(map[string][]StateMutation),
	}
}

func newSubjectFromOnTimerExpiredRequest(req *handlerpb.OnTimerExpiredRequest) *Subject {
	state := make(map[string][]StateEntry, len(req.StateEntryNamespaces))
	for _, namespace := range req.StateEntryNamespaces {
		entries := make([]StateEntry, len(namespace.Entries))
		for i, entry := range namespace.Entries {
			entries[i] = StateEntry{Key: entry.Key, Value: entry.Value}
		}
		state[namespace.Namespace] = entries
	}

	return &Subject{
		key:            req.Key,
		timestamp:      req.Timestamp.AsTime(),
		state:          state,
		stateMutations: make(map[string][]StateMutation),
	}
}

// Register a timer that will trigger the OnTimerExpired when the watermark
// passes the timer.
func (s *Subject) SetTimer(timestamp time.Time) {
	s.timers = append(s.timers, timestamp)
}

// Get the current subject's key
func (s *Subject) Key() []byte {
	return s.key
}

// Get the current subject's timestamp
func (s *Subject) Timestamp() time.Time {
	return s.timestamp
}

func (s *Subject) State(stateItem StateItem) error {
	stateEntries := s.state[stateItem.Name()]
	return stateItem.Load(stateEntries)
}

func (s *Subject) UpdateState(stateItem StateItem) error {
	mutations, err := stateItem.Mutations()
	if err != nil {
		return err
	}

	s.stateMutations[stateItem.Name()] = append(s.stateMutations[stateItem.Name()], mutations...)
	return nil
}

func (s *Subject) AddSinkRequest(sinkID string, event []byte) {
	s.sinkRequests = append(s.sinkRequests, &handlerpb.SinkRequest{Id: sinkID, Value: event})
}

func (s *Subject) encode() *handlerpb.HandlerResponse {
	ret := &handlerpb.HandlerResponse{}
	ret.NewTimers = make([]*timestamppb.Timestamp, len(s.timers))
	for i, t := range s.timers {
		ret.NewTimers[i] = timestamppb.New(t)
	}

	ret.StateMutationNamespaces = make([]*handlerpb.StateMutationNamespace, len(s.stateMutations))
	for ns, mutations := range s.stateMutations {
		// Create a protobuf namespace
		pbNamespace := &handlerpb.StateMutationNamespace{
			Namespace: ns,
			Mutations: make([]*handlerpb.StateMutation, len(mutations)),
		}
		for i := range pbNamespace.Mutations {
			pbNamespace.Mutations[i] = &handlerpb.StateMutation{}
		}

		// Assign oneof mutation types
		for i, mutation := range mutations {
			switch typedMutation := mutation.(type) {
			case *PutMutation:
				pbNamespace.Mutations[i].Mutation = &handlerpb.StateMutation_Put{
					Put: &handlerpb.PutMutation{
						Key:   []byte(typedMutation.Key),
						Value: typedMutation.Value,
					},
				}
			case *DeleteMutation:
				pbNamespace.Mutations[i].Mutation = &handlerpb.StateMutation_Delete{
					Delete: &handlerpb.DeleteMutation{
						Key: []byte(typedMutation.Key),
					},
				}
			}
		}
		ret.StateMutationNamespaces = append(ret.StateMutationNamespaces, pbNamespace)
	}

	ret.SinkRequests = s.sinkRequests

	return ret
}

// Interface used to brand a mutation type
type StateMutation interface {
	isStateMutation() bool
}

// A request to delete a state item
type DeleteMutation struct {
	Key []byte
}

func (m *DeleteMutation) isStateMutation() bool {
	return true
}

// A request to replace a state item
type PutMutation struct {
	Key   []byte
	Value []byte
}

func (m *PutMutation) isStateMutation() bool {
	return true
}

// A named state item inside of the state scoped to a handler call.
type StateItem interface {
	Name() string
	Marshal() ([]byte, error)
	Unmarshal([]byte) error
	Load(entries []StateEntry) error
	Mutations() ([]StateMutation, error)
}
