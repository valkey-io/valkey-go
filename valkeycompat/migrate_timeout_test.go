package valkeycompat

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/valkey-io/valkey-go"
	"github.com/valkey-io/valkey-go/mock"
	"go.uber.org/mock/gomock"
)

// MIGRATE's timeout argument is in milliseconds:
// https://valkey.io/commands/migrate/
//
//	"The timeout specifies the maximum idle time in any moment of the
//	 communication with the destination instance in milliseconds."
//
// Migrate accepts a time.Duration, so the value put on the wire must be
// timeout/time.Millisecond (go-redis does the same: formatMs).
func TestMigrateTimeoutIsInMilliseconds(t *testing.T) {
	for _, tc := range []struct {
		timeout time.Duration
		want    string
	}{
		{30 * time.Second, "30000"},
		{1500 * time.Millisecond, "1500"},
	} {
		t.Run(tc.timeout.String(), func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			client := mock.NewClient(ctrl)
			var got []string
			client.EXPECT().Do(gomock.Any(), gomock.Any()).DoAndReturn(
				func(_ context.Context, cmd valkey.Completed) valkey.ValkeyResult {
					got = append([]string(nil), cmd.Commands()...)
					return mock.Result(mock.ValkeyString("OK"))
				},
			)

			adapter := NewAdapter(client)
			if err := adapter.Migrate(context.Background(), "127.0.0.1", 6380, "key", 0, tc.timeout).Err(); err != nil {
				t.Fatalf("Migrate: %v", err)
			}

			want := []string{"MIGRATE", "127.0.0.1", "6380", "key", "0", tc.want}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("MIGRATE timeout is sent in the wrong unit:\n got: %q\nwant: %q", got, want)
			}
		})
	}
}

// CLIENT PAUSE's timeout argument is in milliseconds as well:
// https://valkey.io/commands/client-pause/
//
//	"CLIENT PAUSE is a connections control command able to suspend all the
//	 Valkey clients for the specified amount of time (in milliseconds)."
//
// The Compat API takes a time.Duration, so a 1500ms pause must be sent as
// "1500" (go-redis sends formatMs too).
func TestClientPauseTimeoutIsInMilliseconds(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	p := newPipeline(mock.NewClient(ctrl))
	p.ClientPause(context.Background(), 1500*time.Millisecond)

	cmds := p.comp.client.(*proxy).cmds
	if len(cmds) != 1 {
		t.Fatalf("expected one command, got %d", len(cmds))
	}
	got := cmds[0].Commands()
	want := []string{"CLIENT", "PAUSE", "1500"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("CLIENT PAUSE timeout is sent in the wrong unit:\n got: %q\nwant: %q", got, want)
	}
}
