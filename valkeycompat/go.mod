module github.com/valkey-io/valkey-go/valkeycompat

go 1.25.0

replace github.com/valkey-io/valkey-go => ../

replace github.com/valkey-io/valkey-go/mock => ../mock

require (
	github.com/onsi/ginkgo/v2 v2.28.1
	github.com/onsi/gomega v1.39.1
	github.com/valkey-io/valkey-go v1.0.74
	github.com/valkey-io/valkey-go/mock v1.0.74
	go.uber.org/mock v0.6.0
)

require (
	github.com/Masterminds/semver/v3 v3.4.0 // indirect
	github.com/gkampitakis/ciinfo v0.3.4 // indirect
	github.com/gkampitakis/go-snaps v0.5.21 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-task/slim-sprig/v3 v3.0.0 // indirect
	github.com/goccy/go-yaml v1.19.2 // indirect
	github.com/google/go-cmp v0.7.0 // indirect
	github.com/google/pprof v0.0.0-20260402051712-545e8a4df936 // indirect
	github.com/maruel/natural v1.3.0 // indirect
	github.com/stretchr/testify v1.11.1 // indirect
	github.com/tidwall/match v1.2.0 // indirect
	go.yaml.in/yaml/v3 v3.0.4 // indirect
	golang.org/x/mod v0.35.0 // indirect
	golang.org/x/net v0.53.0 // indirect
	golang.org/x/sync v0.20.0 // indirect
	golang.org/x/sys v0.43.0 // indirect
	golang.org/x/text v0.36.0 // indirect
	golang.org/x/tools v0.44.0 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)
