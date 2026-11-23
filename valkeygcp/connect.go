package valkeygcp

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/valkey-io/valkey-go"
)

// connect builds the valkey client. valkey-go dials each InitAddress once and returns an
// ERR_IAM_* reply to HELLO as an error without retrying, so connect retries those with
// exponential backoff, as Google recommends for these errors. ctx bounds the retries, which
// also stop after setupTimeout when ctx has no earlier deadline.
//
// Memorystore also answers an invalid username with ERR_IAM_OTHER, and Google recommends
// retrying that error once the username is valid. usernameValid reports that setup knows it
// is (see checkIAMUsername). Otherwise connect looks at the username the handshakes send:
// "default", or none, is always valid; after maxIAMOtherAttempts handshakes answered with
// ERR_IAM_OTHER for another username, connect gives up.
func connect(ctx context.Context, opt valkey.ClientOption, usernameValid bool) (valkey.Client, error) {
	ctx, cancel := context.WithTimeout(ctx, setupTimeout)
	defer cancel()
	var sent func() string // the username the handshakes send, when setup doesn't know it is valid
	if !usernameValid {
		sent = authUsername(&opt)
	}
	var lastErr error
	others := 0 // handshakes answered with ERR_IAM_OTHER
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			if lastErr == nil {
				return nil, err // valkey.NewClient takes no context
			}
			return nil, fmt.Errorf("valkeygcp: %w (gave up after %d attempts: %w)", lastErr, attempt, err)
		}
		client, err := valkey.NewClient(opt)
		if err == nil {
			return client, nil
		}
		if !isIAMBackendError(err) {
			return nil, explainHandshakeError(err)
		}
		if !usernameValid && isIAMOtherError(err) && !alwaysAccepted(sent()) {
			others++
			if others >= maxIAMOtherAttempts {
				return nil, fmt.Errorf("valkeygcp: %w (gave up after %d attempts: Memorystore also answers ERR_IAM_OTHER when the username isn't valid; use %q, no username, or the IAM principal's email)", err, attempt+1, DefaultIAMUsername)
			}
		}
		lastErr = err
		t := time.NewTimer(iamBackoff(attempt))
		select {
		case <-ctx.Done():
			t.Stop()
		case <-t.C:
		}
	}
}

// authUsername returns a function that reports the username opt's handshakes send: the one
// AuthCredentialsFn returned last, which it wraps opt's AuthCredentialsFn to see, or
// ClientOption.Username without one.
func authUsername(opt *valkey.ClientOption) func() string {
	fn := opt.AuthCredentialsFn
	if fn == nil {
		username := opt.Username
		return func() string { return username }
	}
	var last atomic.Pointer[string]
	last.Store(new(string)) // "" until the first AUTH
	opt.AuthCredentialsFn = func(c valkey.AuthCredentialsContext) (valkey.AuthCredentials, error) {
		creds, err := fn(c)
		username := creds.Username
		last.Store(&username)
		return creds, err
	}
	return func() string { return *last.Load() }
}

// alwaysAccepted reports a username Memorystore accepts with any principal's token: none,
// or "default".
func alwaysAccepted(username string) bool {
	return username == "" || username == DefaultIAMUsername
}

// explainHandshakeError adds a hint when an ACL rule refused CLIENT TRACKING, which
// valkey-go sends on every new connection unless ClientOption.DisableCache is set. The hint
// names no user: Memorystore matches ACL rules to the IAM principal that authenticated,
// which isn't the AUTH username "default", and its NOPERM reply, kept in the error, names
// the rule's user.
func explainHandshakeError(err error) error {
	if msg := err.Error(); errors.Is(err, valkey.ErrNoCache) && strings.Contains(msg, "NOPERM") && strings.Contains(msg, "TRACKING") {
		return fmt.Errorf("valkeygcp: the ACL rule must allow CLIENT TRACKING (+client|tracking), or set ClientOption.DisableCache: %w", err)
	}
	return err
}
