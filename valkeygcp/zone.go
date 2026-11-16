package valkeygcp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"strings"
	"time"

	"github.com/valkey-io/valkey-go"
)

// metadataTimeout caps the zone lookup on the metadata server when ctx has no earlier deadline. The GKE metadata
// server can take a few seconds to answer on a new Pod, so the lookup retries until then. Google's metadata client
// times out after 5s too.
const metadataTimeout = 5 * time.Second

var (
	// metadataZoneURL is a variable so that tests can use a fake metadata server.
	metadataZoneURL = "http://169.254.169.254/computeMetadata/v1/instance/zone"
	// metadataClient doesn't use HTTP proxies: the link-local metadata server is only reachable directly.
	metadataClient = &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}
)

// WithZoneAwareReads makes NewClient and ResolveClientOption send the read-only commands of a Cluster Mode Enabled
// instance to replicas in the caller's zone first, then to replicas in other zones, and then to the primary.
// It enables ClientOption.EnableReplicaAZInfo and, unless already set, routes reads as with a reader endpoint
// (ClientOption.SendToReplicas) and uses valkey.AZAffinityNodeSelector with the zone from DetectCurrentGCPZone as
// ClientOption.ReadNodeSelector. If ClientOption.ReplicaSelector is set, it picks the replica instead, and
// ReadNodeSelector stays unset: valkey-go doesn't take both. ClientOption.ReplicaOnly, which sends every command to
// replicas, is an error, and so is a zone DetectCurrentGCPZone can't find.
// With a reader endpoint in ClientOption.Standalone.ReplicaAddress, it changes nothing: the reader endpoint, not the
// client, picks the replica.
// Zone matching needs the servers to report their availability zone; otherwise reads are spread across replicas.
func WithZoneAwareReads() Option {
	return func(c *config) { c.zoneAwareReads = true }
}

// errZoneAwareReadsReplicaOnly is returned for WithZoneAwareReads with ClientOption.ReplicaOnly, which valkey-go
// doesn't take with the SendToReplicas and ReadNodeSelector that WithZoneAwareReads sets.
var errZoneAwareReadsReplicaOnly = errors.New("valkeygcp: ClientOption.ReplicaOnly conflicts with WithZoneAwareReads, which routes only reads to replicas")

// withZoneAwareReads sets up opt for WithZoneAwareReads.
func withZoneAwareReads(ctx context.Context, opt valkey.ClientOption) (valkey.ClientOption, error) {
	if len(opt.Standalone.ReplicaAddress) > 0 {
		return opt, nil // the reader endpoint picks the replica
	}
	opt.EnableReplicaAZInfo = true
	if opt.SendToReplicas == nil {
		opt.SendToReplicas = sendReadsToReplicas
	}
	// valkey-go rejects a ReadNodeSelector next to a ReplicaSelector, which then picks the replica.
	//lint:ignore SA1019 a caller's ReplicaSelector, although deprecated, is still supported by valkey-go
	if opt.ReadNodeSelector == nil && opt.ReplicaSelector == nil {
		zone, err := DetectCurrentGCPZone(ctx)
		if err != nil {
			return valkey.ClientOption{}, err
		}
		opt.ReadNodeSelector = valkey.AZAffinityNodeSelector(zone)
	}
	return opt, nil
}

// DetectCurrentGCPZone returns the GCP zone the caller runs in, such as "us-central1-a".
// It uses the GCP_ZONE or CLOUD_ZONE environment variable if set, and otherwise asks the
// Compute Engine metadata server, retrying for up to 5s, or until ctx ends if sooner.
// A full zone resource path, such as "projects/123456789/zones/us-central1-a", is reduced to the zone name.
// It returns an error if the zone can't be determined, such as outside GCP without GCP_ZONE.
func DetectCurrentGCPZone(ctx context.Context) (string, error) {
	for _, env := range [...]string{"GCP_ZONE", "CLOUD_ZONE"} {
		if zone := zoneName(os.Getenv(env)); zone != "" {
			return zone, nil
		}
	}
	ctx, cancel := context.WithTimeout(ctx, metadataTimeout)
	defer cancel()
	for delay := 100 * time.Millisecond; ; delay = min(2*delay, time.Second) {
		zone, err := metadataZone(ctx)
		if err == nil {
			return zone, nil
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return "", fmt.Errorf("valkeygcp: GCP zone not found in GCP_ZONE, CLOUD_ZONE or the metadata server: %w: %w", ctx.Err(), err)
		case <-timer.C:
		}
	}
}

// metadataZone asks the metadata server once for the zone of the caller.
func metadataZone(ctx context.Context) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, metadataZoneURL, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Metadata-Flavor", "Google")
	resp, err := metadataClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("metadata server: %s", resp.Status)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if zone := zoneName(string(body)); zone != "" {
		return zone, nil
	}
	return "", errors.New("metadata server: empty zone")
}

// zoneName returns the last element of a zone resource path,
// such as "us-central1-a" for "projects/123456789/zones/us-central1-a".
func zoneName(zone string) string {
	if zone = strings.TrimSpace(zone); zone == "" {
		return ""
	}
	return path.Base(zone)
}
