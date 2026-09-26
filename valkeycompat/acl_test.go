package valkeycompat

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/valkey-io/valkey-go"
	"github.com/valkey-io/valkey-go/mock"
	"go.uber.org/mock/gomock"
)

func TestParseACLUser(t *testing.T) {
	t.Run("default admin rule", func(t *testing.T) {
		raw := "user default on nopass sanitize-payload ~* &* +@all"
		u, err := ParseACLUser(raw)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if u.Username != "default" {
			t.Errorf("expected username default, got %s", u.Username)
		}
		if !u.Enabled {
			t.Errorf("expected enabled true")
		}
		if !u.NoPass {
			t.Errorf("expected nopass true")
		}
		if !reflect.DeepEqual(u.Keys, []string{"~*"}) {
			t.Errorf("expected keys [~*], got %v", u.Keys)
		}
		if !reflect.DeepEqual(u.Channels, []string{"&*"}) {
			t.Errorf("expected channels [&*], got %v", u.Channels)
		}
		if u.Commands != "+@all" {
			t.Errorf("expected commands +@all, got %s", u.Commands)
		}
		if !reflect.DeepEqual(u.Flags, []string{"sanitize-payload"}) {
			t.Errorf("expected flags [sanitize-payload], got %v", u.Flags)
		}
		if !u.AllDatabases {
			t.Errorf("expected all databases true")
		}
		if len(u.Selectors) != 0 {
			t.Errorf("expected no selectors, got %d", len(u.Selectors))
		}
		if u.Raw != raw {
			t.Errorf("expected raw %q, got %q", raw, u.Raw)
		}
	})

	t.Run("restricted user with password hash, channels, and selector", func(t *testing.T) {
		raw := "user alice on #ea71c25a7a602246b4c39824b855678894a96f43bb9b71319c39700a1e045222 ~cached:* resetchannels &events:* -@all +get +set (~secret:* resetchannels -@all +@read)"
		u, err := ParseACLUser(raw)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if u.Username != "alice" {
			t.Errorf("expected username alice, got %s", u.Username)
		}
		if !u.Enabled {
			t.Errorf("expected enabled true")
		}
		if u.NoPass {
			t.Errorf("expected nopass false")
		}
		if len(u.Passwords) != 1 || u.Passwords[0] != "#ea71c25a7a602246b4c39824b855678894a96f43bb9b71319c39700a1e045222" {
			t.Errorf("unexpected passwords %v", u.Passwords)
		}
		if !reflect.DeepEqual(u.Keys, []string{"~cached:*"}) {
			t.Errorf("expected keys [~cached:*], got %v", u.Keys)
		}
		if !reflect.DeepEqual(u.Channels, []string{"&events:*"}) {
			t.Errorf("expected channels [&events:*], got %v", u.Channels)
		}
		if u.Commands != "-@all +get +set" {
			t.Errorf("expected commands '-@all +get +set', got %q", u.Commands)
		}
		if len(u.Selectors) != 1 {
			t.Fatalf("expected 1 selector, got %d", len(u.Selectors))
		}
		sel := u.Selectors[0]
		if !reflect.DeepEqual(sel.Keys, []string{"~secret:*"}) {
			t.Errorf("expected selector keys [~secret:*], got %v", sel.Keys)
		}
		if sel.Commands != "-@all +@read" {
			t.Errorf("expected selector commands '-@all +@read', got %q", sel.Commands)
		}
		if len(sel.Channels) != 0 {
			t.Errorf("expected empty selector channels after resetchannels, got %v", sel.Channels)
		}
	})

	t.Run("database scoping and reset directives", func(t *testing.T) {
		raw := "user bob off resetpass resetkeys resetdbs db=0,1,2"
		u, err := ParseACLUser(raw)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if u.Username != "bob" {
			t.Errorf("expected username bob, got %s", u.Username)
		}
		if u.Enabled {
			t.Errorf("expected enabled false")
		}
		if u.AllDatabases {
			t.Errorf("expected all databases false")
		}
		if !reflect.DeepEqual(u.Databases, []int{0, 1, 2}) {
			t.Errorf("expected databases [0, 1, 2], got %v", u.Databases)
		}

		// Now reset to alldbs
		rawAllDbs := "user bob on resetdbs alldbs"
		u2, err := ParseACLUser(rawAllDbs)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if !u2.AllDatabases {
			t.Errorf("expected all databases true after alldbs")
		}
	})

	t.Run("allkeys and allchannels keywords", func(t *testing.T) {
		raw := "user eve on allkeys allchannels allcommands"
		u, err := ParseACLUser(raw)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if !reflect.DeepEqual(u.Keys, []string{"~*"}) {
			t.Errorf("expected keys [~*], got %v", u.Keys)
		}
		if !reflect.DeepEqual(u.Channels, []string{"&*"}) {
			t.Errorf("expected channels [&*], got %v", u.Channels)
		}
		if u.Commands != "allcommands" {
			t.Errorf("expected allcommands, got %q", u.Commands)
		}
	})

	t.Run("multiple selector blocks", func(t *testing.T) {
		raw := "user multi on (~keys1:* +@read) (~keys2:* +@write db=0)"
		u, err := ParseACLUser(raw)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if len(u.Selectors) != 2 {
			t.Fatalf("expected 2 selectors, got %d", len(u.Selectors))
		}
		if !reflect.DeepEqual(u.Selectors[0].Keys, []string{"~keys1:*"}) {
			t.Errorf("unexpected selector 0 keys: %v", u.Selectors[0].Keys)
		}
		if u.Selectors[0].Commands != "+@read" {
			t.Errorf("unexpected selector 0 commands: %s", u.Selectors[0].Commands)
		}
		if !reflect.DeepEqual(u.Selectors[1].Keys, []string{"~keys2:*"}) {
			t.Errorf("unexpected selector 1 keys: %v", u.Selectors[1].Keys)
		}
		if u.Selectors[1].Commands != "+@write" {
			t.Errorf("unexpected selector 1 commands: %s", u.Selectors[1].Commands)
		}
		if u.Selectors[1].AllDatabases {
			t.Errorf("expected selector 1 AllDatabases false")
		}
		if !reflect.DeepEqual(u.Selectors[1].Databases, []int{0}) {
			t.Errorf("expected selector 1 databases [0], got %v", u.Selectors[1].Databases)
		}
	})

	t.Run("user reset token", func(t *testing.T) {
		raw := "user resetted on >pass ~* (~sub:* +@read) reset"
		u, err := ParseACLUser(raw)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if u.Enabled {
			t.Errorf("expected user off after reset")
		}
		if len(u.Passwords) != 0 {
			t.Errorf("expected no passwords after reset")
		}
		if len(u.Keys) != 0 {
			t.Errorf("expected no keys after reset")
		}
		if len(u.Selectors) != 0 {
			t.Errorf("expected no selectors after reset, got %d", len(u.Selectors))
		}
		if u.Commands != "-@all" {
			t.Errorf("expected commands -@all after reset, got %q", u.Commands)
		}
	})

	t.Run("clearselectors directive", func(t *testing.T) {
		raw := "user alice on (~sub1:* +get) (~sub2:* +set) clearselectors"
		u, err := ParseACLUser(raw)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if len(u.Selectors) != 0 {
			t.Errorf("expected 0 selectors after clearselectors, got %d", len(u.Selectors))
		}
	})

	t.Run("rule without user prefix", func(t *testing.T) {
		raw := "on nopass ~* +get"
		u, err := ParseACLUser(raw)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if !u.Enabled || !u.NoPass {
			t.Errorf("expected enabled and nopass")
		}
		if !reflect.DeepEqual(u.Keys, []string{"~*"}) {
			t.Errorf("expected keys [~*]")
		}
	})

	t.Run("invalid rules and unclosed selectors", func(t *testing.T) {
		if _, err := ParseACLUser(""); err == nil {
			t.Errorf("expected error on empty string")
		}
		if _, err := ParseACLUser("user"); err == nil {
			t.Errorf("expected error on missing username")
		}
		if u, err := ParseACLUser("user bob (+get"); err == nil {
			t.Errorf("expected error on unclosed selector")
		} else {
			for _, flag := range u.Flags {
				if flag == "(+get" {
					t.Errorf("unclosed selector must not pollute Flags: %v", u.Flags)
				}
			}
		}
		if _, err := ParseACLUser("user bob (+get (~secret:*"); err == nil {
			t.Errorf("expected error on nested unclosed selector")
		}
		if _, err := ParseACLUser("user bob +get )"); err == nil {
			t.Errorf("expected error on stray ')'")
		}
	})

	t.Run("parentheses in passwords keys and channels", func(t *testing.T) {
		raw := "user alice on >pass(word) ~data(1) &events(all) +get"
		u, err := ParseACLUser(raw)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if !reflect.DeepEqual(u.Passwords, []string{">pass(word)"}) {
			t.Errorf("expected passwords [>pass(word)], got %v", u.Passwords)
		}
		if !reflect.DeepEqual(u.Keys, []string{"~data(1)"}) {
			t.Errorf("expected keys [~data(1)], got %v", u.Keys)
		}
		if !reflect.DeepEqual(u.Channels, []string{"&events(all)"}) {
			t.Errorf("expected channels [&events(all)], got %v", u.Channels)
		}
		if len(u.Selectors) != 0 {
			t.Errorf("expected 0 selectors, got %d", len(u.Selectors))
		}
		if u.Commands != "+get" {
			t.Errorf("expected commands '+get', got %q", u.Commands)
		}
	})

	t.Run("parentheses in passwords with selectors", func(t *testing.T) {
		raw := "user alice on >pass(word) (~data(1) +get)"
		u, err := ParseACLUser(raw)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if !reflect.DeepEqual(u.Passwords, []string{">pass(word)"}) {
			t.Errorf("expected passwords [>pass(word)], got %v", u.Passwords)
		}
		if len(u.Selectors) != 1 {
			t.Fatalf("expected 1 selector, got %d", len(u.Selectors))
		}
		if !reflect.DeepEqual(u.Selectors[0].Keys, []string{"~data(1)"}) {
			t.Errorf("expected selector keys [~data(1)], got %v", u.Selectors[0].Keys)
		}
		if u.Selectors[0].Commands != "+get" {
			t.Errorf("expected selector commands '+get', got %q", u.Selectors[0].Commands)
		}
	})
}

func TestParseACLList(t *testing.T) {
	lines := []string{
		"user default on nopass sanitize-payload ~* &* +@all",
		"user alice on #1234 ~cached:* &events:* +get",
	}

	users, err := ParseACLListStrings(lines)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(users) != 2 {
		t.Fatalf("expected 2 users, got %d", len(users))
	}
	if users[0].Username != "default" || users[1].Username != "alice" {
		t.Errorf("unexpected usernames: %s, %s", users[0].Username, users[1].Username)
	}

	// Test with ValkeyResult
	msg := mock.ValkeyArray(
		mock.ValkeyString(lines[0]),
		mock.ValkeyString(lines[1]),
	)
	res := mock.Result(msg)
	usersFromRes, err := ParseACLList(res)
	if err != nil {
		t.Fatalf("unexpected err from ParseACLList: %v", err)
	}
	if len(usersFromRes) != 2 {
		t.Fatalf("expected 2 users from res, got %d", len(usersFromRes))
	}

	// Error in ValkeyResult
	errRes := mock.ErrorResult(errors.New("network error"))
	if _, err := ParseACLList(errRes); err == nil {
		t.Errorf("expected error from errRes")
	}
}

func TestParseClientInfo(t *testing.T) {
	txt := "id=469 addr=127.0.0.1:55958 laddr=127.0.0.1:6379 fd=8 name=myclient age=12 idle=3 flags=SM db=1 sub=2 psub=3 ssub=4 multi=5 watch=6 qbuf=100 qbuf-free=200 argv-mem=18 multi-mem=50 rbs=16384 rbp=16384 obl=1 oll=2 omem=3 tot-mem=17322 events=r cmd=auth user=default redir=-1 resp=3 lib-name=valkey-go lib-ver=1.0 tot-net-in=40 tot-net-out=50 tot-cmds=6"
	info, err := ParseClientInfo(txt)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	if info.ID != 469 {
		t.Errorf("expected ID 469, got %d", info.ID)
	}
	if info.Addr != "127.0.0.1:55958" {
		t.Errorf("expected Addr 127.0.0.1:55958, got %s", info.Addr)
	}
	if info.LAddr != "127.0.0.1:6379" {
		t.Errorf("expected LAddr 127.0.0.1:6379, got %s", info.LAddr)
	}
	if info.FD != 8 {
		t.Errorf("expected FD 8, got %d", info.FD)
	}
	if info.Name != "myclient" {
		t.Errorf("expected Name myclient, got %s", info.Name)
	}
	if info.Age != 12*time.Second {
		t.Errorf("expected Age 12s, got %v", info.Age)
	}
	if info.Idle != 3*time.Second {
		t.Errorf("expected Idle 3s, got %v", info.Idle)
	}
	if info.Flags != (ClientSlave | ClientMaster) {
		t.Errorf("expected Flags ClientSlave|ClientMaster, got %v", info.Flags)
	}
	if info.DB != 1 {
		t.Errorf("expected DB 1, got %d", info.DB)
	}
	if info.Sub != 2 || info.PSub != 3 || info.SSub != 4 {
		t.Errorf("unexpected subscriptions: %d, %d, %d", info.Sub, info.PSub, info.SSub)
	}
	if info.Multi != 5 || info.Watch != 6 {
		t.Errorf("unexpected multi/watch: %d, %d", info.Multi, info.Watch)
	}
	if info.BufferSize != 16384 || info.BufferPeak != 16384 {
		t.Errorf("unexpected buffer size/peak: %d, %d", info.BufferSize, info.BufferPeak)
	}
	if info.LastCmd != "auth" || info.User != "default" {
		t.Errorf("unexpected lastCmd/user: %s, %s", info.LastCmd, info.User)
	}
	if info.Resp != 3 || info.LibName != "valkey-go" || info.LibVer != "1.0" {
		t.Errorf("unexpected resp/lib info: %d, %s, %s", info.Resp, info.LibName, info.LibVer)
	}
	if info.TotalNetIn != 40 || info.TotalNetOut != 50 || info.TotalCmds != 6 {
		t.Errorf("unexpected net/cmds stats: %d, %d, %d", info.TotalNetIn, info.TotalNetOut, info.TotalCmds)
	}

	// Test prefix txt:
	infoWithPrefix, err := ParseClientInfo("txt: id=10 addr=127.0.0.1:1234 flags=N")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if infoWithPrefix.ID != 10 {
		t.Errorf("expected ID 10, got %d", infoWithPrefix.ID)
	}

	// Test forward compatibility for unknown flags (tolerated and skipped)
	cases := []struct {
		name  string
		flags string
		want  ClientFlags
	}{
		{"single unknown", "m", 0},
		{"multiple unknown", "go", 0},
		{"known plus unknown", "tm", ClientTracking},
		{"known combo plus unknown", "Btm", ClientTrackingBCAST | ClientTracking},
		{"no-flag sentinel", "N", 0},
		{"redis/valkey newer flags", "SMCIiE", ClientSlave | ClientMaster},
	}
	for _, tc := range cases {
		t.Run("flags_"+tc.name, func(t *testing.T) {
			info, err := ParseClientInfo("id=1 addr=127.0.0.1:6379 flags=" + tc.flags + " db=0")
			if err != nil {
				t.Fatalf("ParseClientInfo(flags=%q) errored: %v", tc.flags, err)
			}
			if info.Flags&tc.want != tc.want {
				t.Fatalf("flags=%q: Flags=%b, want bits %b set", tc.flags, info.Flags, tc.want)
			}
		})
	}

	// Test malformed kv
	if _, err := ParseClientInfo("not_a_kv"); err == nil {
		t.Errorf("expected error on non-kv token")
	}
}

func TestParseACLLog(t *testing.T) {
	t.Run("RESP3 map format", func(t *testing.T) {
		entryMap := mock.ValkeyMap(map[string]valkey.ValkeyMessage{
			"count":                  mock.ValkeyInt64(1),
			"reason":                 mock.ValkeyString("command"),
			"context":                mock.ValkeyString("toplevel"),
			"object":                 mock.ValkeyString("set"),
			"username":               mock.ValkeyString("testuser"),
			"age-seconds":            mock.ValkeyFloat64(4.5),
			"client-info":            mock.ValkeyString("id=100 addr=127.0.0.1:9999 flags=N"),
			"entry-id":               mock.ValkeyInt64(42),
			"timestamp-created":      mock.ValkeyInt64(1600000000),
			"timestamp-last-updated": mock.ValkeyInt64(1600000001),
		})
		logArr := mock.ValkeyArray(entryMap)
		res := mock.Result(logArr)

		entries, err := ParseACLLog(res)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if len(entries) != 1 {
			t.Fatalf("expected 1 entry, got %d", len(entries))
		}
		e := entries[0]
		if e.Count != 1 {
			t.Errorf("expected count 1, got %d", e.Count)
		}
		if e.Reason != "command" || e.Context != "toplevel" || e.Object != "set" || e.Username != "testuser" {
			t.Errorf("unexpected entry fields: %s, %s, %s, %s", e.Reason, e.Context, e.Object, e.Username)
		}
		if e.AgeSeconds != 4.5 {
			t.Errorf("expected age 4.5, got %f", e.AgeSeconds)
		}
		if e.EntryID != 42 || e.TimestampCreated != 1600000000 || e.TimestampLastUpdated != 1600000001 {
			t.Errorf("unexpected id/timestamps: %d, %d, %d", e.EntryID, e.TimestampCreated, e.TimestampLastUpdated)
		}
		if e.ClientInfo == nil || e.ClientInfo.ID != 100 {
			t.Errorf("expected client-info ID 100")
		}
	})

	t.Run("RESP2 flat array format", func(t *testing.T) {
		entryFlat := mock.ValkeyArray(
			mock.ValkeyString("count"), mock.ValkeyInt64(3),
			mock.ValkeyString("reason"), mock.ValkeyString("key"),
			mock.ValkeyString("object"), mock.ValkeyString("secret_key"),
			mock.ValkeyString("username"), mock.ValkeyString("alice"),
		)
		logArr := mock.ValkeyArray(entryFlat)
		res := mock.Result(logArr)

		entries, err := ParseACLLog(res)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if len(entries) != 1 {
			t.Fatalf("expected 1 entry, got %d", len(entries))
		}
		if entries[0].Reason != "key" || entries[0].Object != "secret_key" || entries[0].Username != "alice" {
			t.Errorf("unexpected entry data: %v", entries[0])
		}
	})

	t.Run("error result", func(t *testing.T) {
		res := mock.ErrorResult(errors.New("err"))
		if _, err := ParseACLLog(res); err == nil {
			t.Errorf("expected error")
		}
	})
}

func TestParseACLDryRun(t *testing.T) {
	tests := []struct {
		name           string
		input          string
		expectedAllow  bool
		expectedReason string
		expectedDenied ACLDeniedReason
	}{
		{
			name:           "allowed OK",
			input:          "OK",
			expectedAllow:  true,
			expectedReason: "OK",
			expectedDenied: DeniedNone,
		},
		{
			name:           "command denial",
			input:          "User alice has no permissions to run the 'set' command",
			expectedAllow:  false,
			expectedReason: "User alice has no permissions to run the 'set' command",
			expectedDenied: DeniedCommand,
		},
		{
			name:           "command denial short",
			input:          "No permissions to access a command",
			expectedAllow:  false,
			expectedReason: "No permissions to access a command",
			expectedDenied: DeniedCommand,
		},
		{
			name:           "key denial",
			input:          "User alice has no permissions to access the 'secret' key",
			expectedAllow:  false,
			expectedReason: "User alice has no permissions to access the 'secret' key",
			expectedDenied: DeniedKey,
		},
		{
			name:           "key denial short",
			input:          "No permissions to access a key",
			expectedAllow:  false,
			expectedReason: "No permissions to access a key",
			expectedDenied: DeniedKey,
		},
		{
			name:           "channel denial",
			input:          "User alice has no permissions to access the 'alerts' channel",
			expectedAllow:  false,
			expectedReason: "User alice has no permissions to access the 'alerts' channel",
			expectedDenied: DeniedChannel,
		},
		{
			name:           "channel denial short",
			input:          "No permissions to access a channel",
			expectedAllow:  false,
			expectedReason: "No permissions to access a channel",
			expectedDenied: DeniedChannel,
		},
		{
			name:           "database denial",
			input:          "User alice has no permissions to access database 2",
			expectedAllow:  false,
			expectedReason: "User alice has no permissions to access database 2",
			expectedDenied: DeniedDatabase,
		},
		{
			name:           "database denial short",
			input:          "No permissions to access database",
			expectedAllow:  false,
			expectedReason: "No permissions to access database",
			expectedDenied: DeniedDatabase,
		},
		{
			name:           "key denial with command substring in key name",
			input:          "User alice has no permissions to access the 'orders_command_queue' key",
			expectedAllow:  false,
			expectedReason: "User alice has no permissions to access the 'orders_command_queue' key",
			expectedDenied: DeniedKey,
		},
		{
			name:           "channel denial with command substring in channel name",
			input:          "User alice has no permissions to access the 'command_notifications' channel",
			expectedAllow:  false,
			expectedReason: "User alice has no permissions to access the 'command_notifications' channel",
			expectedDenied: DeniedChannel,
		},
		{
			name:           "command denial with key substring in command name",
			input:          "User alice has no permissions to run the 'key_command' command",
			expectedAllow:  false,
			expectedReason: "User alice has no permissions to run the 'key_command' command",
			expectedDenied: DeniedCommand,
		},
		{
			name:           "other rejection",
			input:          "User alice access denied for special reason",
			expectedAllow:  false,
			expectedReason: "User alice access denied for special reason",
			expectedDenied: DeniedOther,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res := ParseACLDryRunString(tc.input)
			if res.Allowed != tc.expectedAllow {
				t.Errorf("expected Allowed %v, got %v", tc.expectedAllow, res.Allowed)
			}
			if res.Reason != tc.expectedReason {
				t.Errorf("expected Reason %q, got %q", tc.expectedReason, res.Reason)
			}
			if res.DeniedType != tc.expectedDenied {
				t.Errorf("expected DeniedType %v, got %v", tc.expectedDenied, res.DeniedType)
			}

			// Also test through ParseACLDryRun with ValkeyResult
			vr := mock.Result(mock.ValkeyString(tc.input))
			resFromVR, err := ParseACLDryRun(vr)
			if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
			if resFromVR != res {
				t.Errorf("mismatch between ParseACLDryRunString and ParseACLDryRun: %+v vs %+v", res, resFromVR)
			}
		})
	}

	t.Run("protocol error propagation", func(t *testing.T) {
		protoErr := errors.New("ERR User 'nonexistent' not found")
		vr := mock.ErrorResult(protoErr)
		_, err := ParseACLDryRun(vr)
		if err == nil {
			t.Errorf("expected error from protocol error")
		}
	})

	t.Run("denied reason strings", func(t *testing.T) {
		if DeniedNone.String() != "none" {
			t.Errorf("expected none, got %s", DeniedNone.String())
		}
		if DeniedCommand.String() != "command" {
			t.Errorf("expected command, got %s", DeniedCommand.String())
		}
		if DeniedKey.String() != "key" {
			t.Errorf("expected key, got %s", DeniedKey.String())
		}
		if DeniedChannel.String() != "channel" {
			t.Errorf("expected channel, got %s", DeniedChannel.String())
		}
		if DeniedDatabase.String() != "database" {
			t.Errorf("expected database, got %s", DeniedDatabase.String())
		}
		if DeniedOther.String() != "other" {
			t.Errorf("expected other, got %s", DeniedOther.String())
		}
		if ACLDeniedReason(999).String() != "other" {
			t.Errorf("expected other for unknown code")
		}
	})
}

func TestACLHelpers(t *testing.T) {
	ctx := context.Background()

	t.Run("ACLDryRun validation", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		client := mock.NewClient(ctrl)

		// Missing command
		_, err := ACLDryRun(ctx, client, "alice")
		if err == nil {
			t.Errorf("expected error for missing command in ACLDryRun")
		}

		// Single command
		client.EXPECT().Do(gomock.Any(), mock.Match("ACL", "DRYRUN", "alice", "get")).
			Return(mock.Result(mock.ValkeyString("OK")))
		res, err := ACLDryRun(ctx, client, "alice", "get")
		if err != nil || !res.Allowed {
			t.Errorf("unexpected dryrun result: %+v, %v", res, err)
		}

		// Command with args
		client.EXPECT().Do(gomock.Any(), mock.Match("ACL", "DRYRUN", "alice", "set", "k", "v")).
			Return(mock.Result(mock.ValkeyString("User alice has no permissions to run the 'set' command")))
		res, err = ACLDryRun(ctx, client, "alice", "set", "k", "v")
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if res.Allowed || res.DeniedType != DeniedCommand {
			t.Errorf("expected Allowed=false, DeniedCommand, got %+v", res)
		}
	})

	t.Run("ACLList helper dispatch", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		client := mock.NewClient(ctrl)

		client.EXPECT().Do(gomock.Any(), mock.Match("ACL", "LIST")).
			Return(mock.Result(mock.ValkeyArray(mock.ValkeyString("user default on nopass ~* &* +@all"))))
		users, err := ACLList(ctx, client)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if len(users) != 1 || users[0].Username != "default" {
			t.Errorf("unexpected users: %v", users)
		}
	})

	t.Run("ACLLog helper dispatch", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		client := mock.NewClient(ctrl)

		// Count > 0
		client.EXPECT().Do(gomock.Any(), mock.Match("ACL", "LOG", "10")).
			Return(mock.Result(mock.ValkeyArray()))
		entries, err := ACLLog(ctx, client, 10)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if len(entries) != 0 {
			t.Errorf("expected 0 entries")
		}

		// Count <= 0
		client.EXPECT().Do(gomock.Any(), mock.Match("ACL", "LOG")).
			Return(mock.Result(mock.ValkeyArray()))
		entries, err = ACLLog(ctx, client, 0)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if len(entries) != 0 {
			t.Errorf("expected 0 entries")
		}
	})
}

func TestACLLiveIntegration(t *testing.T) {
	targets := []struct {
		name string
		addr string
	}{
		{"Valkey_8", "127.0.0.1:6379"},
		{"Redis_7_4", "127.0.0.1:6378"},
		{"Redis_8_6", "127.0.0.1:6382"},
	}

	for _, target := range targets {
		t.Run(target.name, func(t *testing.T) {
			client, err := valkey.NewClient(valkey.ClientOption{
				InitAddress:  []string{target.addr},
				DisableCache: true,
			})
			if err != nil {
				t.Skipf("skipping %s: cannot connect to %s: %v", target.name, target.addr, err)
			}
			defer client.Close()

			ctx := context.Background()
			if err := client.Do(ctx, client.B().Ping().Build()).Error(); err != nil {
				t.Skipf("skipping %s: server not reachable: %v", target.name, err)
			}

			// 1. Live ACLList
			users, err := ACLList(ctx, client)
			if err != nil {
				t.Fatalf("[%s] live ACLList failed: %v", target.name, err)
			}
			if len(users) == 0 {
				t.Fatalf("[%s] expected at least 1 user in ACLList", target.name)
			}
			var defaultUser *ACLUser
			for i := range users {
				if users[i].Username == "default" {
					defaultUser = &users[i]
					break
				}
			}
			if defaultUser == nil {
				t.Fatalf("[%s] default user not found in ACLList", target.name)
			}
			if !defaultUser.Enabled {
				t.Errorf("[%s] default user expected enabled", target.name)
			}

			// 2. Setup isolated user for ACL DRYRUN verification
			testUser := "test_dryrun_user"
			cleanupCmd := client.B().AclDeluser().Username(testUser).Build()
			client.Do(ctx, cleanupCmd)
			defer client.Do(ctx, client.B().AclDeluser().Username(testUser).Build())

			// User with permission to GET only key:permitted:* and no SET permission
			setupCmd := client.B().AclSetuser().Username(testUser).
				Rule("on", "nopass", "~key:permitted:*", "+get", "-@write").
				Build()
			if err := client.Do(ctx, setupCmd).Error(); err != nil {
				t.Fatalf("[%s] failed to set up test ACL user: %v", target.name, err)
			}

			// 3. Test allowed command on permitted key -> OK
			res, err := ACLDryRun(ctx, client, testUser, "get", "key:permitted:one")
			if err != nil {
				t.Fatalf("[%s] live ACLDryRun failed: %v", target.name, err)
			}
			if !res.Allowed || res.DeniedType != DeniedNone {
				t.Errorf("[%s] expected allowed, got %+v", target.name, res)
			}

			// 4. Test forbidden command on permitted key -> DeniedCommand
			res, err = ACLDryRun(ctx, client, testUser, "set", "key:permitted:one", "val")
			if err != nil {
				t.Fatalf("[%s] live ACLDryRun failed: %v", target.name, err)
			}
			if res.Allowed {
				t.Errorf("[%s] expected denial for 'set', got allowed!", target.name)
			}
			if res.DeniedType != DeniedCommand {
				t.Errorf("[%s] expected DeniedCommand, got %v (%s)", target.name, res.DeniedType, res.Reason)
			}

			// 5. Test forbidden command with key substring in command name -> must remain DeniedCommand
			res, err = ACLDryRun(ctx, client, testUser, "hset", "key:permitted:one", "f", "v")
			if err != nil {
				t.Fatalf("[%s] live ACLDryRun failed: %v", target.name, err)
			}
			if res.Allowed {
				t.Errorf("[%s] expected denial for 'hset', got allowed!", target.name)
			}
			if res.DeniedType != DeniedCommand {
				t.Errorf("[%s] expected DeniedCommand, got %v (%s)", target.name, res.DeniedType, res.Reason)
			}

			// 6. Test permitted command on forbidden key -> DeniedKey
			res, err = ACLDryRun(ctx, client, testUser, "get", "forbidden:one")
			if err != nil {
				t.Fatalf("[%s] live ACLDryRun failed: %v", target.name, err)
			}
			if res.Allowed {
				t.Errorf("[%s] expected denial for forbidden key, got allowed!", target.name)
			}
			if res.DeniedType != DeniedKey {
				t.Errorf("[%s] expected DeniedKey, got %v (%s)", target.name, res.DeniedType, res.Reason)
			}

			// 7. Test forbidden key with substring 'command' in key name -> must remain DeniedKey
			res, err = ACLDryRun(ctx, client, testUser, "get", "command_forbidden_key")
			if err != nil {
				t.Fatalf("[%s] live ACLDryRun failed: %v", target.name, err)
			}
			if res.Allowed {
				t.Errorf("[%s] expected denial for key with command substring, got allowed!", target.name)
			}
			if res.DeniedType != DeniedKey {
				t.Errorf("[%s] expected DeniedKey (not DeniedCommand) for key with command substring, got %v (%s)", target.name, res.DeniedType, res.Reason)
			}

			// 8. Attach selector and verify structured parsing
			attachSelectorCmd := client.B().AclSetuser().Username(testUser).Rule("(~secret:* +get)").Build()
			if err := client.Do(ctx, attachSelectorCmd).Error(); err == nil {
				// Server supports selectors (Redis 7+ / Valkey)
				uList, err := ACLList(ctx, client)
				if err != nil {
					t.Fatalf("[%s] ACLList after selector attach failed: %v", target.name, err)
				}
				var found *ACLUser
				for i := range uList {
					if uList[i].Username == testUser {
						found = &uList[i]
						break
					}
				}
				if found == nil || len(found.Selectors) == 0 {
					t.Errorf("[%s] expected selector attached for user %s, got %+v", target.name, testUser, found)
				} else {
					if !reflect.DeepEqual(found.Selectors[0].Keys, []string{"~secret:*"}) {
						t.Errorf("[%s] unexpected selector keys: %v", target.name, found.Selectors[0].Keys)
					}
				}

				// 9. Clear selectors
				clearCmd := client.B().AclSetuser().Username(testUser).Rule("clearselectors").Build()
				if err := client.Do(ctx, clearCmd).Error(); err == nil {
					uListAfterClear, _ := ACLList(ctx, client)
					for i := range uListAfterClear {
						if uListAfterClear[i].Username == testUser {
							if len(uListAfterClear[i].Selectors) != 0 {
								t.Errorf("[%s] expected 0 selectors after clearselectors, got %d", target.name, len(uListAfterClear[i].Selectors))
							}
							break
						}
					}
				}
			}

			// 10. Live ACLLog check with count > 0 and count == 0
			logs10, err := ACLLog(ctx, client, 10)
			if err != nil {
				t.Fatalf("[%s] live ACLLog(10) failed: %v", target.name, err)
			}
			t.Logf("[%s] retrieved %d ACL log entries (count=10)", target.name, len(logs10))

			logs0, err := ACLLog(ctx, client, 0)
			if err != nil {
				t.Fatalf("[%s] live ACLLog(0) failed: %v", target.name, err)
			}
			t.Logf("[%s] retrieved %d ACL log entries (count=0)", target.name, len(logs0))
		})
	}
}

