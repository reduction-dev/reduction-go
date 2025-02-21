package internal

// Construct defines the format for each abstract type
type Construct struct {
	ID     string
	Type   string
	Params map[string]any
}