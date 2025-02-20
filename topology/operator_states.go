package topology

type OperatorStates struct {
	states []State
}

func (os *OperatorStates) NewIntState(name string) *State {
	state := &State{
		Name: name,
		Type: "int",
	}
	os.states = append(os.states, *state)
	return state
}

type State struct {
	Name string
	Type string
}
