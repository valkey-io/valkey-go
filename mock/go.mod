module github.com/valkey-io/valkey-go/mock

go 1.20

replace github.com/valkey-io/valkey-go => ../

require (
	github.com/valkey-io/valkey-go v1.0.40
	go.uber.org/mock v0.3.0
)

require golang.org/x/sys v0.19.0 // indirect
