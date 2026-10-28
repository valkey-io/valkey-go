package valkeygcp

import (
	"strings"
	"time"

	"github.com/valkey-io/valkey-go"
)

// noReplicasFound is in the error a Memorystore reader endpoint returns, before it closes the connection, when the
// instance has no replicas.
const noReplicasFound = "no replicas found"

// connect creates the client for option. Memorystore keeps the reader endpoint of an instance without replicas
// but refuses its connections, and never sends them to the primary. So if option has a reader endpoint that
// refuses connections because the instance has no replicas, connect creates a client for the primary endpoint only.
func connect(option valkey.ClientOption, readYourWritesWindow time.Duration) (valkey.Client, error) {
	client, err := newClient(option, readYourWritesWindow)
	if err != nil && len(option.Standalone.ReplicaAddress) > 0 && strings.Contains(err.Error(), noReplicasFound) {
		option.Standalone.ReplicaAddress = nil
		option.ForceSingleClient = true // the primary endpoint of a Cluster Mode Disabled instance is not a cluster
		client, err = newClient(option, readYourWritesWindow)
	}
	return client, err
}

// newClient creates the client, wrapped for read-your-writes if readYourWritesWindow is set.
func newClient(option valkey.ClientOption, readYourWritesWindow time.Duration) (valkey.Client, error) {
	if readYourWritesWindow == 0 {
		return valkey.NewClient(option)
	}
	return newReadYourWrites(option, readYourWritesWindow)
}

