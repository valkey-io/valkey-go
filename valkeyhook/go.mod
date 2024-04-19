module github.com/rueian/valkey-go/valkeyhook

go 1.20

replace (
	github.com/rueian/valkey-go => ../
	github.com/rueian/valkey-go/mock => ../mock
)

require (
	github.com/rueian/valkey-go v1.0.34
	github.com/rueian/valkey-go/mock v1.0.34
	go.uber.org/mock v0.4.0
)

require golang.org/x/sys v0.19.0 // indirect
