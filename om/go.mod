module github.com/valkey-io/valkey-go/om

go 1.20

replace github.com/valkey-io/valkey-go => ../

require (
	github.com/oklog/ulid/v2 v2.1.0
	github.com/valkey-io/valkey-go v1.0.43
)

require golang.org/x/sys v0.19.0 // indirect
