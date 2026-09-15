module github.com/valkey-io/valkey-go/valkeylimiter

go 1.25.0

replace github.com/valkey-io/valkey-go => ../

replace github.com/valkey-io/valkey-go/mock => ../mock

require (
	github.com/valkey-io/valkey-go v1.0.78
	github.com/valkey-io/valkey-go/mock v1.0.78
	go.uber.org/mock v0.6.0
)

require golang.org/x/sys v0.47.0 // indirect
