module github.com/valkey-io/valkey-go/valkeyprob

go 1.20.0

replace github.com/valkey-io/valkey-go => ../

require (
	github.com/valkey-io/valkey-go v1.0.41
	github.com/twmb/murmur3 v1.1.8
)

require golang.org/x/sys v0.19.0 // indirect
