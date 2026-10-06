package valkeycompat

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/valkey-io/valkey-go"
	"github.com/valkey-io/valkey-go/mock"
)




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

		entries, err := parseACLLog(res)
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

		entries, err := parseACLLog(res)
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
		if _, err := parseACLLog(res); err == nil {
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
			res := parseACLDryRunString(tc.input)
			if res.Allowed != tc.expectedAllow {
				t.Errorf("expected Allowed %v, got %v", tc.expectedAllow, res.Allowed)
			}
			if res.Reason != tc.expectedReason {
				t.Errorf("expected Reason %q, got %q", tc.expectedReason, res.Reason)
			}
			if res.DeniedType != tc.expectedDenied {
				t.Errorf("expected DeniedType %v, got %v", tc.expectedDenied, res.DeniedType)
			}

			// Also test through parseACLDryRun with ValkeyResult
			vr := mock.Result(mock.ValkeyString(tc.input))
			resFromVR, err := parseACLDryRun(vr)
			if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
			if resFromVR != res {
				t.Errorf("mismatch between parseACLDryRunString and parseACLDryRun: %+v vs %+v", res, resFromVR)
			}
		})
	}

	t.Run("protocol error propagation", func(t *testing.T) {
		protoErr := errors.New("ERR User 'nonexistent' not found")
		vr := mock.ErrorResult(protoErr)
		_, err := parseACLDryRun(vr)
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

			adapter := NewAdapter(client)
			ctx := context.Background()
			if err := client.Do(ctx, client.B().Ping().Build()).Error(); err != nil {
				t.Skipf("skipping %s: server not reachable: %v", target.name, err)
			}

			// 1. Live ACLList
			users, err := adapter.ACLList(ctx).Result()
			if err != nil {
				t.Fatalf("[%s] live ACLList failed: %v", target.name, err)
			}
			if len(users) == 0 {
				t.Fatalf("[%s] expected at least 1 user in ACLList", target.name)
			}
			foundDefault := false
			for _, line := range users {
				if strings.Contains(line, "default") {
					foundDefault = true
					break
				}
			}
			if !foundDefault {
				t.Fatalf("[%s] default user not found in ACLList", target.name)
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
			res, err := adapter.ACLDryRun(ctx, testUser, "get", "key:permitted:one").ACLDryRunResult()
			if err != nil {
				t.Fatalf("[%s] live ACLDryRun failed: %v", target.name, err)
			}
			if !res.Allowed || res.DeniedType != DeniedNone {
				t.Errorf("[%s] expected allowed, got %+v", target.name, res)
			}

			// 4. Test forbidden command on permitted key -> DeniedCommand
			res, err = adapter.ACLDryRun(ctx, testUser, "set", "key:permitted:one", "val").ACLDryRunResult()
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
			res, err = adapter.ACLDryRun(ctx, testUser, "hset", "key:permitted:one", "f", "v").ACLDryRunResult()
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
			res, err = adapter.ACLDryRun(ctx, testUser, "get", "forbidden:one").ACLDryRunResult()
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
			res, err = adapter.ACLDryRun(ctx, testUser, "get", "command_forbidden_key").ACLDryRunResult()
			if err != nil {
				t.Fatalf("[%s] live ACLDryRun failed: %v", target.name, err)
			}
			if res.Allowed {
				t.Errorf("[%s] expected denial for key with command substring, got allowed!", target.name)
			}
			if res.DeniedType != DeniedKey {
				t.Errorf("[%s] expected DeniedKey (not DeniedCommand) for key with command substring, got %v (%s)", target.name, res.DeniedType, res.Reason)
			}

			// 8. Attach selector and verify ACLList
			attachSelectorCmd := client.B().AclSetuser().Username(testUser).Rule("(~secret:* +get)").Build()
			if err := client.Do(ctx, attachSelectorCmd).Error(); err == nil {
				// Server supports selectors (Redis 7+ / Valkey)
				uList, err := adapter.ACLList(ctx).Result()
				if err != nil {
					t.Fatalf("[%s] ACLList after selector attach failed: %v", target.name, err)
				}
				found := false
				for _, line := range uList {
					if strings.Contains(line, testUser) && strings.Contains(line, "~secret:*") {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("[%s] expected selector in ACLList for user %s, got %v", target.name, testUser, uList)
				}

				// 9. Clear selectors
				clearCmd := client.B().AclSetuser().Username(testUser).Rule("clearselectors").Build()
				if err := client.Do(ctx, clearCmd).Error(); err == nil {
					uListAfterClear, _ := adapter.ACLList(ctx).Result()
					for _, line := range uListAfterClear {
						if strings.Contains(line, testUser) {
							if strings.Contains(line, "(~secret:*") {
								t.Errorf("[%s] expected selector removed after clearselectors, got %s", target.name, line)
							}
							break
						}
					}
				}
			}

			// 10. Live ACLLog check with count > 0 and count == 0
			logs10, err := adapter.ACLLog(ctx, 10).Result()
			if err != nil {
				t.Fatalf("[%s] live ACLLog(10) failed: %v", target.name, err)
			}
			t.Logf("[%s] retrieved %d ACL log entries (count=10)", target.name, len(logs10))

			logs0, err := adapter.ACLLog(ctx, 0).Result()
			if err != nil {
				t.Fatalf("[%s] live ACLLog(0) failed: %v", target.name, err)
			}
			t.Logf("[%s] retrieved %d ACL log entries (count=0)", target.name, len(logs0))
		})
	}
}

