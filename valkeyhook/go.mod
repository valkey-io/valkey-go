module github.com/valkey-io/valkey-go/valkeyhook

go 1.20

replace (
	github.com/valkey-io/valkey-go => ../
	github.com/valkey-io/valkey-go/mock => ../mock
)

require (
	github.com/valkey-io/valkey-go v1.0.36
	github.com/valkey-io/valkey-go/mock v1.0.36
	go.uber.org/mock v0.4.0
)

require golang.org/x/sys v0.19.0 // indirect
