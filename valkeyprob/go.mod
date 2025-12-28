module github.com/valkey-io/valkey-go/valkeyprob

go 1.24.9

toolchain go1.24.11

replace github.com/valkey-io/valkey-go => ../

require (
	github.com/twmb/murmur3 v1.1.8
	github.com/valkey-io/valkey-go v1.0.69
)

require golang.org/x/sys v0.39.0 // indirect
