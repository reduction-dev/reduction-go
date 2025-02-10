module reduction.dev/reduction-go

go 1.23

require (
	connectrpc.com/connect v1.18.1
	google.golang.org/protobuf v1.36.3
	reduction.dev/reduction-protocol v0.0.0-00010101000000-000000000000
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/stretchr/testify v1.9.0
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace (
	reduction.dev/reduction-go => ../reduction-go
	reduction.dev/reduction-protocol => ../reduction-protocol
)
