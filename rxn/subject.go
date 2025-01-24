package rxn

import (
	"bytes"
	"slices"
	"sort"
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

type contextKey string

var SubjectContextKey = contextKey("subject")

type StateEntry struct {
	Key   []byte
	Value []byte
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

func (s *Subject) UpdateState(state StateItem) error {
	currentStateEntries := s.state[state.Name()]

	mutations, err := state.Mutations()
	if err != nil {
		return err
	}
	name := state.Name()

	// Create a map of existing entries by key
	entryMap := make(map[string]StateEntry)
	for _, entry := range currentStateEntries {
		entryMap[string(entry.Key)] = entry
	}

	// Apply mutations to the entry map
	for _, mutation := range mutations {
		switch typed := mutation.(type) {
		case *PutMutation:
			entryMap[string(typed.Key)] = StateEntry{
				Key:   typed.Key,
				Value: typed.Value,
			}
		case *DeleteMutation:
			delete(entryMap, string(typed.Key))
		}
	}

	// Convert map back to slice
	newEntries := make([]StateEntry, 0, len(entryMap))
	for _, entry := range entryMap {
		newEntries = append(newEntries, entry)
	}
	slices.SortFunc(newEntries, func(a, b StateEntry) int {
		return bytes.Compare(a.Key, b.Key)
	})

	// Update the state map
	s.state[name] = newEntries
	s.stateMutations[name] = append(s.stateMutations[name], mutations...)
	return nil
}

func (s *Subject) AddSinkRequest(sinkID string, event []byte) {
	s.sinkRequests = append(s.sinkRequests, &handlerpb.SinkRequest{Id: sinkID, Value: event})
}

func (s *Subject) encode() *handlerpb.KeyResult {
	ret := &handlerpb.KeyResult{Key: s.key}
	ret.NewTimers = make([]*timestamppb.Timestamp, len(s.timers))
	for i, t := range s.timers {
		ret.NewTimers[i] = timestamppb.New(t)
	}

	ret.StateMutationNamespaces = make([]*handlerpb.StateMutationNamespace, len(s.stateMutations))
	var idx int
	for ns, mutations := range s.stateMutations {
		// Coalesce mutations by key
		latest := make(map[string]StateMutation)
		for _, m := range mutations {
			switch typed := m.(type) {
			case *PutMutation:
				latest[string(typed.Key)] = m
			case *DeleteMutation:
				latest[string(typed.Key)] = m
			}
		}

		// Convert to sorted slice
		keys := make([]string, 0, len(latest))
		for k := range latest {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		coalesced := make([]StateMutation, 0, len(latest))
		for _, k := range keys {
			coalesced = append(coalesced, latest[k])
		}

		// Create a protobuf namespace
		pbNamespace := &handlerpb.StateMutationNamespace{
			Namespace: ns,
			Mutations: make([]*handlerpb.StateMutation, len(coalesced)),
		}
		for i := range pbNamespace.Mutations {
			pbNamespace.Mutations[i] = &handlerpb.StateMutation{}
		}

		// Assign oneof mutation types
		for i, mutation := range coalesced {
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
		ret.StateMutationNamespaces[idx] = pbNamespace
		idx++
	}

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
	Load(entries []StateEntry) error
	Mutations() ([]StateMutation, error)
}
