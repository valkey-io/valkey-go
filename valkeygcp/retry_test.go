package valkeygcp

import (
	"errors"
	"testing"

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
