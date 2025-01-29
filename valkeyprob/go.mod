module github.com/valkey-io/valkey-go/valkeyprob

go 1.22.0

replace github.com/valkey-io/valkey-go => ../

require (
	github.com/twmb/murmur3 v1.1.8
	github.com/valkey-io/valkey-go v1.0.53
)

require golang.org/x/sys v0.29.0 // indirect
