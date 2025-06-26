module github.com/valkey-io/valkey-go/valkeyprob

go 1.23.0

toolchain go1.23.4

replace github.com/valkey-io/valkey-go => ../

require (
	github.com/twmb/murmur3 v1.1.8
	github.com/valkey-io/valkey-go v1.0.62
)

require golang.org/x/sys v0.31.0 // indirect
