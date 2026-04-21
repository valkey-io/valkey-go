module github.com/valkey-io/valkey-go/om

go 1.25.0

replace github.com/valkey-io/valkey-go => ../

require (
	github.com/oklog/ulid/v2 v2.1.1
	github.com/valkey-io/valkey-go v1.0.74
)

require golang.org/x/sys v0.43.0 // indirect
