package valkeygcp

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// TLSMode names the instance's in-transit encryption and server CA mode. Today every mode
// except TLSDisabled trusts the CA certificates given in TLSOptions (CAPEM, CAFile or
// Base.RootCAs), whatever the mode says, and Mode only appears in errors: it is there for a
// later version that downloads the CA certificates for the instance's mode from the
// Memorystore API. WithTLS doesn't check that the certificates match the mode.
type TLSMode int

// TLSMode values. Each mode except TLSAuto matches a transit encryption and server CA mode.
const (
	// TLSAuto, the zero value, is TLSCustomCA in WithTLS: the CA certificates in TLSOptions
	// are trusted whatever the instance's server CA mode.
	TLSAuto          TLSMode = iota
	TLSDisabled              // TRANSIT_ENCRYPTION_DISABLED
	TLSPerInstanceCA         // SERVER_AUTHENTICATION + GOOGLE_MANAGED_PER_INSTANCE_CA
	TLSSharedCA              // SERVER_AUTHENTICATION + GOOGLE_MANAGED_SHARED_CA
	TLSCustomCA              // SERVER_AUTHENTICATION + CUSTOMER_MANAGED_CAS_CA, or any caller PEM
)

func (m TLSMode) String() string {
	switch m {
	case TLSAuto:
		return "TLSAuto"
	case TLSDisabled:
		return "TLSDisabled"
	case TLSPerInstanceCA:
		return "TLSPerInstanceCA"
	case TLSSharedCA:
		return "TLSSharedCA"
	case TLSCustomCA:
		return "TLSCustomCA"
	}
	return "TLSMode(" + strconv.Itoa(int(m)) + ")"
}

// TLSOptions configures in-transit encryption.
type TLSOptions struct {
	Mode       TLSMode // the instance's server CA mode; it doesn't change what is trusted (see TLSMode)
	CAPEM      []byte  // server CA certificates (PEM), for any mode
	CAFile     string  // alternative to CAPEM; setting both is an error
	ServerName string  // optional verification override
	// Base is an optional starting point, cloned and never mutated. Only RootCAs,
	// MinVersion (raised to TLS 1.2) and ServerName (when set above) are changed; its other
	// settings are kept as they are, so InsecureSkipVerify still turns verification off.
	Base *tls.Config
}

// buildTLSConfig turns the resolved mode plus CA material into a *tls.Config; nil for
// TLSDisabled. The CA certificates come from CAPEM or CAFile (not both), else from
// discovered, else from Base.RootCAs. discovered holds CA certificates obtained another way,
// such as from the Memorystore API; WithTLS passes none.
func buildTLSConfig(opt TLSOptions, mode TLSMode, discovered []string) (*tls.Config, error) {
	if mode == TLSDisabled {
		return nil, nil
	}
	if len(opt.CAPEM) > 0 && opt.CAFile != "" {
		return nil, errors.New("valkeygcp: set TLSOptions.CAPEM or TLSOptions.CAFile, not both")
	}
	pemBytes := opt.CAPEM
	if opt.CAFile != "" {
		b, err := os.ReadFile(opt.CAFile)
		if err != nil {
			return nil, fmt.Errorf("valkeygcp: read TLSOptions.CAFile: %w", err)
		}
		pemBytes = b
	}
	if len(pemBytes) == 0 && len(discovered) > 0 {
		pemBytes = []byte(strings.Join(discovered, "\n"))
	}
	var pool *x509.CertPool
	switch {
	case len(pemBytes) > 0:
		var err error
		if pool, err = parseCABundle(pemBytes); err != nil {
			return nil, err
		}
	case opt.Base != nil && opt.Base.RootCAs != nil:
		pool = opt.Base.RootCAs // caller-built pool, kept as is
	default:
		return nil, errNeedsCA(mode)
	}
	cfg := &tls.Config{MinVersion: tls.VersionTLS12} // Memorystore supports TLS 1.2+
	if opt.Base != nil {
		cfg = opt.Base.Clone()
		if cfg.MinVersion < tls.VersionTLS12 {
			cfg.MinVersion = tls.VersionTLS12
		}
	}
	cfg.RootCAs = pool
	if opt.ServerName != "" {
		cfg.ServerName = opt.ServerName
	}
	// ServerName stays empty by default, so crypto/tls verifies each node's certificate
	// against the IP address valkey-go dials.
	return cfg, nil
}

// parseCABundle returns a pool with every certificate in pemBytes. Unlike
// x509.CertPool.AppendCertsFromPEM, which skips what it can't parse, it fails on any
// malformed, truncated or non-certificate block: a bundle whose new CA is broken, during a
// server CA rotation, then fails NewClient instead of the handshakes with the nodes that
// moved to that CA.
func parseCABundle(pemBytes []byte) (*x509.CertPool, error) {
	pool := x509.NewCertPool()
	blocks := 0
	for rest := pemBytes; ; {
		var block *pem.Block
		if block, rest = pem.Decode(rest); block == nil {
			break
		}
		blocks++
		if block.Type != "CERTIFICATE" {
			return nil, fmt.Errorf("valkeygcp: CA bundle: PEM block %d is %s, not CERTIFICATE", blocks, block.Type)
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("valkeygcp: CA bundle: certificate %d: %w", blocks, err)
		}
		pool.AddCert(cert)
	}
	// pem.Decode skips a block it can't decode, so compare with the blocks the bundle begins.
	if begun := bytes.Count(pemBytes, []byte("-----BEGIN")); begun != blocks {
		return nil, fmt.Errorf("valkeygcp: CA bundle: %d of its %d PEM blocks are malformed or truncated", begun-blocks, begun)
	}
	if blocks == 0 {
		return nil, errors.New("valkeygcp: no valid PEM certificate in the CA bundle")
	}
	return pool, nil
}

// errNeedsCA reports a TLS mode without CA material.
func errNeedsCA(mode TLSMode) error {
	return fmt.Errorf("valkeygcp: TLS mode %v needs CA certificates: set TLSOptions.CAPEM or TLSOptions.CAFile to the instance's server CA certificates", mode)
}

// errTLSConfigConflict reports a caller-built TLSConfig next to WithTLS.
var errTLSConfigConflict = errors.New("valkeygcp: ClientOption.TLSConfig conflicts with WithTLS, which builds the TLS configuration; to start from your own settings, set TLSOptions.Base")

// WithTLS turns on in-transit encryption for NewClient and ResolveClientOption. Every node's
// certificate is verified against the CA certificates in opt (CAPEM, CAFile or
// Base.RootCAs), with TLS 1.2 or later. All certificates in the bundle are trusted, so a
// bundle holding more than one CA, as during a server CA rotation, verifies nodes signed by
// any of them. The bundle is read once, when NewClient or ResolveClientOption runs.
//
// opt.Mode names the instance's server CA mode and appears in errors; it doesn't change
// which certificates are trusted (see TLSMode). TLSAuto is TLSCustomCA. Without CA
// certificates NewClient fails instead of connecting unverified. TLSDisabled leaves TLS
// off and leaves a ClientOption.TLSConfig as it is; with any other mode, a
// ClientOption.TLSConfig set next to WithTLS is an error.
func WithTLS(opt TLSOptions) Option {
	return func(c *config) { c.tls = &opt }
}

// newTLSConfig checks opt.Mode and builds the TLS configuration WithTLS asks for; nil for
// TLSDisabled.
func newTLSConfig(opt TLSOptions) (*tls.Config, error) {
	mode := opt.Mode
	switch {
	case mode == TLSAuto:
		mode = TLSCustomCA
	case mode < TLSAuto || mode > TLSCustomCA:
		return nil, fmt.Errorf("valkeygcp: unknown TLS mode %v", mode)
	}
	return buildTLSConfig(opt, mode, nil)
}
