module github.com/valkey-io/valkey-go/valkeyprob

go 1.26.0

replace github.com/valkey-io/valkey-go => ../

require (
	github.com/twmb/murmur3 v1.2.2
	github.com/valkey-io/valkey-go v1.0.79
)

require golang.org/x/sys v0.50.0 // indirect
