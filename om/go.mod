module github.com/valkey-io/valkey-go/om

go 1.22.0

replace github.com/valkey-io/valkey-go => ../

require (
	github.com/oklog/ulid/v2 v2.1.0
	github.com/valkey-io/valkey-go v1.0.55
)

require golang.org/x/sys v0.30.0 // indirect
