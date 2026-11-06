package gcp

import (
	"errors"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/resource"
)

// fixHostAndProcess makes the generated parts of service.instance.id predictable: the OS
// hostname, PID 42 and the random part 0a1b2c3d.
func fixHostAndProcess(t *testing.T, host string, hostErr error) {
	t.Helper()
	origHost, origPID, origRandom := hostname, processID, randomHex
	hostname = func() (string, error) { return host, hostErr }
	processID = func() int { return 42 }
	randomHex = func() string { return "0a1b2c3d" }
	t.Cleanup(func() { hostname, processID, randomHex = origHost, origPID, origRandom })
}

func resourceWith(kv ...attribute.KeyValue) *resource.Resource { return resource.NewSchemaless(kv...) }

func TestChooseInstanceID(t *testing.T) {
	var (
		envID    = attribute.String("service.instance.id", "from-env")
		cloudRun = attribute.String("faas.instance", "0069c7a988")
		pod      = attribute.String("k8s.pod.name", "checkout-7d9f8-abcde")
		vm       = attribute.String("gcp.gce.instance.name", "vm-1")
	)
	for _, tc := range []struct {
		name     string
		explicit string
		res      *resource.Resource
		host     string
		hostErr  error
		want     string
		warns    bool
	}{
		{name: "Options.ServiceInstanceID wins", explicit: "copy-1", res: resourceWith(envID, cloudRun), want: "copy-1"},
		{name: "then the resource's, from OTEL_RESOURCE_ATTRIBUTES or ResourceAttributes", res: resourceWith(envID, cloudRun), want: "from-env"},
		{name: "Cloud Run instance ID", res: resourceWith(cloudRun, pod, vm), host: "localhost", want: "0069c7a988-42-0a1b2c3d"},
		{name: "pod name", res: resourceWith(pod, vm), host: "node-1", want: "checkout-7d9f8-abcde-42-0a1b2c3d"},
		{name: "Compute Engine VM name", res: resourceWith(vm, attribute.String("host.name", "vm-1.c.p.internal")), host: "other", want: "vm-1-42-0a1b2c3d"},
		{name: "OS hostname, as on GKE or a laptop", res: resourceWith(), host: " my-laptop ", want: "my-laptop-42-0a1b2c3d"},
		{name: "no resource", host: "my-laptop", want: "my-laptop-42-0a1b2c3d"},
		{name: "localhost doesn't count", res: resourceWith(), host: "localhost", want: "unknown-42-0a1b2c3d", warns: true},
		{name: "no hostname", res: resourceWith(), hostErr: errors.New("no hostname"), want: "unknown-42-0a1b2c3d", warns: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixHostAndProcess(t, tc.host, tc.hostErr)
			log := captureErrors(t)
			if got := chooseInstanceID(tc.explicit, tc.res); got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
			if warned := log.contains("couldn't find a host name"); warned != tc.warns {
				t.Errorf("warned %v, want %v", warned, tc.warns)
			}
			if tc.warns && !log.contains(tc.want) {
				t.Errorf("the warning doesn't name the ID %q it uses", tc.want)
			}
		})
	}
}

// TestChooseInstanceID_Unique checks what the two-copies test checks in Google Cloud: copies on
// the same host get different IDs, even with the same PID, as containers that run as PID 1.
func TestChooseInstanceID_Unique(t *testing.T) {
	origHost, origPID := hostname, processID
	hostname = func() (string, error) { return "same-host", nil }
	processID = func() int { return 1 }
	t.Cleanup(func() { hostname, processID = origHost, origPID })

	seen := map[string]bool{}
	for range 100 {
		id := chooseInstanceID("", nil)
		if !strings.HasPrefix(id, "same-host-1-") || len(id) != len("same-host-1-")+8 {
			t.Fatalf("id %q, want same-host-1-<8 hex digits>", id)
		}
		if seen[id] {
			t.Fatalf("id %q was generated twice", id)
		}
		seen[id] = true
	}
}
