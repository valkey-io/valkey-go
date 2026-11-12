package gcp

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/resource"
)

// serviceInstanceIDKey is the resource attribute that Google Cloud turns into the instance
// label of prometheus_target. Copies of an application that share it collide, and Google
// drops their points.
const serviceInstanceIDKey = attribute.Key("service.instance.id")

// The parts of a generated service.instance.id. They are variables so tests can fix them.
var (
	hostname  = os.Hostname
	processID = os.Getpid
	randomHex = func() string {
		var b [4]byte
		_, _ = rand.Read(b[:]) // crypto/rand.Read doesn't fail on supported platforms
		return hex.EncodeToString(b[:])
	}
)

// chooseInstanceID returns the service.instance.id of this process. res describes the process
// without Options (see buildResource). The first value found wins:
//
//  1. Options.ServiceInstanceID;
//  2. service.instance.id in res, from Options.ResourceAttributes or OTEL_RESOURCE_ATTRIBUTES;
//  3. a generated <host>-<pid>-<8 random hex digits>, where host is the first one found of: the
//     Cloud Run (or other serverless) instance ID, k8s.pod.name (from OTEL_RESOURCE_ATTRIBUTES,
//     for example), the Compute Engine VM name, and the OS hostname, which Kubernetes sets to
//     the pod name (on GKE, the detected environment has no VM name, so the pod name is used).
//     Without any of them (or with only "localhost"), host is "unknown" and a warning goes to
//     OpenTelemetry's error handler.
//
// The random part keeps the ID unique when processes share a host name and a PID, as the
// containers of a pod do (each often runs as PID 1). The ID stays the same while the process
// runs, and a restarted process gets a new one.
func chooseInstanceID(explicit string, res *resource.Resource) string {
	if explicit != "" {
		return explicit
	}
	if v := stringAttr(res, serviceInstanceIDKey); v != "" {
		return v
	}
	host := firstNonEmpty(
		stringAttr(res, "faas.instance"),
		stringAttr(res, "k8s.pod.name"),
		stringAttr(res, "gcp.gce.instance.name"),
		osHostname(),
	)
	unknown := host == ""
	if unknown {
		host = "unknown"
	}
	id := fmt.Sprintf("%s-%d-%s", host, processID(), randomHex())
	if unknown {
		otel.Handle(fmt.Errorf("valkeyotel/gcp: couldn't find a host name for service.instance.id; using %q. Set Options.ServiceInstanceID or service.instance.id in OTEL_RESOURCE_ATTRIBUTES to choose one", id))
	}
	return id
}

// osHostname returns the OS hostname, or "" when it is unavailable or "localhost", which
// doesn't tell machines apart.
func osHostname() string {
	h, err := hostname()
	if err != nil {
		return ""
	}
	if h = strings.TrimSpace(h); strings.EqualFold(h, "localhost") {
		return ""
	}
	return h
}

// stringAttr returns the value of key in res, or "".
func stringAttr(res *resource.Resource, key attribute.Key) string {
	if res == nil {
		return ""
	}
	v, _ := res.Set().Value(key)
	return strings.TrimSpace(v.AsString())
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}
