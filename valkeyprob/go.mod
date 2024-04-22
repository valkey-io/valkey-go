module github.com/rueian/valkey-go/valkeyprob

go 1.20.0

replace github.com/rueian/valkey-go => ../

require (
	github.com/rueian/valkey-go v1.0.35
	github.com/twmb/murmur3 v1.1.8
)

require golang.org/x/sys v0.19.0 // indirect
