module github.com/rueian/valkey-go/mock

go 1.20

replace github.com/rueian/valkey-go => ../

require (
	github.com/rueian/valkey-go v1.0.34
	go.uber.org/mock v0.3.0
)

require golang.org/x/sys v0.19.0 // indirect
