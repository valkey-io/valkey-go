module github.com/valkey-io/valkey-go/valkeygcp/examples

go 1.26.0

replace (
	github.com/valkey-io/valkey-go => ../../
	github.com/valkey-io/valkey-go/valkeygcp => ../
)

require (
	github.com/valkey-io/valkey-go v1.0.80
	github.com/valkey-io/valkey-go/valkeygcp v1.0.80
	golang.org/x/oauth2 v0.38.0
)

require (
	cloud.google.com/go/compute/metadata v0.10.0 // indirect
	golang.org/x/sys v0.50.0 // indirect
)
