package rxn

import (
	"time"

	"reduction.dev/reduction-handler/handlerpb"
)

type lazySubjectBatch struct {
	subjects map[string]*Subject                // <subject-key>:<subject>
	state    map[string]map[string][]StateEntry // <subject-key>:<state-id>:<state-entries>
}

func newLazySubjectBatch(keyStates []*handlerpb.KeyState) *lazySubjectBatch {
	state := make(map[string]map[string][]StateEntry, len(keyStates))
	for _, keyState := range keyStates {
		// Initialize inner map for this key
		state[string(keyState.Key)] = make(map[string][]StateEntry)
		for _, namespace := range keyState.StateEntryNamespaces {
			entries := make([]StateEntry, len(namespace.Entries))
			for i, entry := range namespace.Entries {
				entries[i] = StateEntry{Key: entry.Key, Value: entry.Value}
			}
			state[string(keyState.Key)][namespace.Namespace] = entries
		}
	}

	return &lazySubjectBatch{
		subjects: make(map[string]*Subject),
		state:    state,
	}
}

func (sb *lazySubjectBatch) subjectFor(key []byte, timestamp time.Time) *Subject {
	if subject, ok := sb.subjects[string(key)]; ok {
		subject.timestamp = timestamp
		return subject
	}
	subject := &Subject{
		key:            key,
		timestamp:      timestamp,
		state:          sb.stateForKey(key),
		stateMutations: make(map[string][]StateMutation),
	}
	sb.subjects[string(key)] = subject
	return subject
}

func (sb *lazySubjectBatch) response() *handlerpb.ProcessEventBatchResponse {
	resp := &handlerpb.ProcessEventBatchResponse{}
	for _, subject := range sb.subjects {
		resp.SinkRequests = append(resp.SinkRequests, subject.sinkRequests...)
		resp.KeyResults = append(resp.KeyResults, subject.encode())
	}
	return resp
}

func (sb *lazySubjectBatch) stateForKey(key []byte) map[string][]StateEntry {
	if state, ok := sb.state[string(key)]; ok {
		return state
	}
	state := make(map[string][]StateEntry)
	sb.state[string(key)] = state
	return state
}
