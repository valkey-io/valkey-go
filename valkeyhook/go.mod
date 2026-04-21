module github.com/valkey-io/valkey-go/valkeyhook

go 1.25.0

replace (
	github.com/valkey-io/valkey-go => ../
	github.com/valkey-io/valkey-go/mock => ../mock
)

require (
	github.com/valkey-io/valkey-go v1.0.74
	github.com/valkey-io/valkey-go/mock v1.0.74
	go.uber.org/mock v0.6.0
)

require golang.org/x/sys v0.43.0 // indirect
