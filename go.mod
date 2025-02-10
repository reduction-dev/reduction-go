module reduction.dev/reduction-go

go 1.23

require (
	connectrpc.com/connect v1.18.1
	google.golang.org/protobuf v1.36.3
	reduction.dev/reduction-protocol v0.0.0-00010101000000-000000000000
)

replace (
	reduction.dev/reduction-go => ../reduction-go
	reduction.dev/reduction-protocol => ../reduction-protocol
)
