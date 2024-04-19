module github.com/rueian/valkey-go/valkeyaside

go 1.20

replace github.com/rueian/valkey-go => ../

require (
	github.com/oklog/ulid/v2 v2.1.0
	github.com/rueian/valkey-go v1.0.34
)

require golang.org/x/sys v0.19.0 // indirect
