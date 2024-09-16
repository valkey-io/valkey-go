module github.com/valkey-io/valkey-go/valkeyprob

go 1.21

replace github.com/valkey-io/valkey-go => ../

require (
	github.com/twmb/murmur3 v1.1.8
	github.com/valkey-io/valkey-go v1.0.46
)

require golang.org/x/sys v0.24.0 // indirect
