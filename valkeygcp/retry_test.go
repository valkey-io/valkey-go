package valkeygcp

import (
	"errors"
	"testing"
	"time"

	"github.com/valkey-io/valkey-go"
)

// Every error, and a nil error, gets the same backoff: at most 50ms at first and never more
// than 2s. A negative attempt count is treated like the first attempt.
func TestRetryDelay_Backoff(t *testing.T) {
	for _, err := range []error{nil, errors.New("i/o timeout"), errors.New("LOADING Valkey is loading the dataset in memory")} {
		for attempt := range maxRetryAttempts {
			d := memorystoreRetryDelay(attempt, valkey.Completed{}, err)
			if d <= 0 || d > retryCap {
				t.Fatalf("%v attempt %d: delay %v outside (0, 2s]", err, attempt, d)
			}
			if attempt == 0 && d > retryBase {
				t.Fatalf("%v: first delay %v above the base %v", err, d, retryBase)
			}
		}
	}
	if d := memorystoreRetryDelay(-3, valkey.Completed{}, nil); d <= 0 || d > retryBase {
		t.Errorf("negative attempts: %v", d)
	}
}

// From maxRetryAttempts on, the delay is negative, which tells valkey-go to stop retrying.
func TestRetryDelay_StopsAfterMaxAttempts(t *testing.T) {
	for _, attempt := range []int{maxRetryAttempts, maxRetryAttempts + 1, 1 << 20} {
		if d := memorystoreRetryDelay(attempt, valkey.Completed{}, errors.New("i/o timeout")); d >= 0 {
			t.Errorf("attempt %d: got %v, want negative (stop)", attempt, d)
		}
	}
}

// Memorystore IAM backend errors (ERR_IAM_EXHAUSTED, ERR_IAM_OTHER), which connect and the
// dial gate back off, are recognized; other errors are not. Their backoff starts at most at
// 250ms and can pass the 2s cap of memorystoreRetryDelay, up to 10s.
func TestIAMBackoff(t *testing.T) {
	for _, msg := range []string{
		"ERR (ERR_IAM_EXHAUSTED) Memorystore IAM authentication backend quota exceeded.",
		"ERR (ERR_IAM_OTHER) Memorystore IAM authentication backend error.",
	} {
		if !isIAMBackendError(errors.New(msg)) {
			t.Errorf("%q not recognized as an IAM backend error", msg)
		}
	}
	for _, err := range []error{nil, errors.New("WRONGPASS invalid username-password pair"), errors.New("i/o timeout")} {
		if isIAMBackendError(err) {
			t.Errorf("%v recognized as an IAM backend error", err)
		}
	}
	var longest time.Duration
	for attempt := range maxRetryAttempts {
		d := iamBackoff(attempt)
		if d <= 0 || d > iamBackoffCap {
			t.Fatalf("attempt %d: delay %v outside (0, 10s]", attempt, d)
		}
		if attempt == 0 && d > iamBackoffBase {
			t.Fatalf("first delay %v above the IAM base %v", d, iamBackoffBase)
		}
	}
	for range 200 {
		longest = max(longest, iamBackoff(10))
	}
	if longest <= retryCap {
		t.Errorf("longest IAM delay %v never exceeded the generic 2s cap", longest)
	}
}
