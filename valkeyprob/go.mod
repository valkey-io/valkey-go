module github.com/valkey-io/valkey-go/valkeyprob

go 1.25.0

replace github.com/valkey-io/valkey-go => ../

require (
	github.com/twmb/murmur3 v1.1.8
	github.com/valkey-io/valkey-go v1.0.74
)

require golang.org/x/sys v0.43.0 // indirect
