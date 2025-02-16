package internal

import (
	"bytes"
	"fmt"
	"slices"
	"sort"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
	"reduction.dev/reduction-protocol/handlerpb"
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
	// Track which states were used during handler execution
	usedStates map[string]LazyMutations
	// Cache of loaded state instances
	loadedStates map[string]any
}

// LoadedState returns a previously loaded state instance for the given ID, or nil if not found
func (s *Subject) LoadedState(id string) any {
	if s.loadedStates == nil {
		return nil
	}
	return s.loadedStates[id]
}

// StoreLoadedState stores a loaded state instance for later reuse
func (s *Subject) StoreLoadedState(id string, state any) {
	if s.loadedStates == nil {
		s.loadedStates = make(map[string]any)
	}
	s.loadedStates[id] = state
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

func (s *Subject) LoadState(stateItem StateItem) error {
	// Get base state entries
	stateEntries := s.state[stateItem.Name()]

	// If we have previous mutations for this state, apply them to our entries
	if mutations, ok := s.usedStates[stateItem.Name()]; ok {
		prevMutations, err := mutations()
		if err != nil {
			return err
		}

		// Create a map of existing entries
		entryMap := make(map[string]StateEntry)
		for _, entry := range stateEntries {
			entryMap[string(entry.Key)] = entry
		}

		// Apply previous mutations
		for _, mutation := range prevMutations {
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

		// Convert back to slice
		stateEntries = make([]StateEntry, 0, len(entryMap))
		for _, entry := range entryMap {
			stateEntries = append(stateEntries, entry)
		}
	}

	return stateItem.Load(stateEntries)
}

func (s *Subject) StateEntries(stateID string) []StateEntry {
	return s.state[stateID]
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

	// Collect mutations from all used states
	allMutations := make(map[string][]StateMutation)
	for stateID, mutations := range s.usedStates {
		mutations, err := mutations()
		if err != nil {
			// Since encode is called internally and errors here would indicate a serious problem,
			// we'll maintain the existing pattern of panicking on state errors
			panic(fmt.Sprintf("failed to get mutations for state %s: %v", stateID, err))
		}
		allMutations[stateID] = mutations
	}

	ret.StateMutationNamespaces = make([]*handlerpb.StateMutationNamespace, len(allMutations))
	var idx int
	for ns, mutations := range allMutations {
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

type LazyMutations = func() ([]StateMutation, error)

// Register a state item that was used during handler execution
func (s *Subject) RegisterStateUse(stateID string, mutations LazyMutations) {
	s.usedStates[stateID] = mutations
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
