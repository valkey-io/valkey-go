package valkey

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
)

func TestSentinelClient_ReadNodeSelector(t *testing.T) {
	defer ShouldNotLeak(SetupLeakDetection())

	t.Run("conflicts with ReplicaOnly", func(t *testing.T) {
		_, err := newSentinelClient(&ClientOption{
			InitAddress: []string{"127.0.0.1:26379"},
			ReplicaOnly: true,
			ReadNodeSelector: func(slot uint16, nodes []NodeInfo) int {
				return 1
			},
		}, nil, nil)
		if !errors.Is(err, ErrReplicaOnlyConflictWithReadNodeSelector) {
			t.Fatalf("expected ErrReplicaOnlyConflictWithReadNodeSelector, got %v", err)
		}
	})

	t.Run("conflicts with ReplicaSelector", func(t *testing.T) {
		_, err := newSentinelClient(&ClientOption{
			InitAddress: []string{"127.0.0.1:26379"},
			ReplicaSelector: func(slot uint16, replicas []NodeInfo) int {
				return 0
			},
			ReadNodeSelector: func(slot uint16, nodes []NodeInfo) int {
				return 1
			},
		}, nil, nil)
		if !errors.Is(err, ErrReplicaSelectorConflictWithReadNodeSelector) {
			t.Fatalf("expected ErrReplicaSelectorConflictWithReadNodeSelector, got %v", err)
		}
	})

	t.Run("selects specific replica and can switch to primary", func(t *testing.T) {
		var selectedIdx atomic.Int32
		selectedIdx.Store(2) // start by picking replica2

		s0 := &mockConn{
			DoFn: func(cmd Completed) ValkeyResult { return ValkeyResult{} },
			DoMultiFn: func(multi ...Completed) *valkeyresults {
				return &valkeyresults{s: []ValkeyResult{
					{val: slicemsg('*', []ValkeyMessage{})},
					{val: slicemsg('*', []ValkeyMessage{strmsg('+', "10.0.0.1"), strmsg('+', "6379")})},
					{val: slicemsg('*', []ValkeyMessage{
						slicemsg('%', []ValkeyMessage{strmsg('+', "ip"), strmsg('+', "10.0.0.2"), strmsg('+', "port"), strmsg('+', "6379")}),
						slicemsg('%', []ValkeyMessage{strmsg('+', "ip"), strmsg('+', "10.0.0.3"), strmsg('+', "port"), strmsg('+', "6379")}),
						slicemsg('%', []ValkeyMessage{strmsg('+', "ip"), strmsg('+', "10.0.0.4"), strmsg('+', "port"), strmsg('+', "6379"), strmsg('+', "s-down-time"), strmsg('+', "100")}),
					})},
				}}
			},
		}
		mConn := &mockConn{
			DoFn: func(cmd Completed) ValkeyResult {
				if cmd.Commands()[0] == "ROLE" {
					return ValkeyResult{val: slicemsg('*', []ValkeyMessage{strmsg('+', "master")})}
				}
				return ValkeyResult{val: strmsg('+', "from-master")}
			},
		}
		r1Conn := &mockConn{
			DoFn: func(cmd Completed) ValkeyResult {
				if cmd.Commands()[0] == "ROLE" {
					return ValkeyResult{val: slicemsg('*', []ValkeyMessage{strmsg('+', "slave")})}
				}
				return ValkeyResult{val: strmsg('+', "from-replica1")}
			},
		}
		r2Conn := &mockConn{
			DoFn: func(cmd Completed) ValkeyResult {
				if cmd.Commands()[0] == "ROLE" {
					return ValkeyResult{val: slicemsg('*', []ValkeyMessage{strmsg('+', "slave")})}
				}
				return ValkeyResult{val: strmsg('+', "from-replica2")}
			},
		}

		var observedNodes []NodeInfo
		client, err := newSentinelClient(&ClientOption{
			InitAddress:    []string{"s0"},
			SendToReplicas: func(cmd Completed) bool { return cmd.IsReadOnly() },
			ReadNodeSelector: func(slot uint16, nodes []NodeInfo) int {
				observedNodes = append([]NodeInfo(nil), nodes...)
				return int(selectedIdx.Load())
			},
		}, func(dst string, opt *ClientOption) conn {
			switch dst {
			case "s0":
				return s0
			case "10.0.0.1:6379":
				return mConn
			case "10.0.0.2:6379":
				return r1Conn
			case "10.0.0.3:6379":
				return r2Conn
			default:
				t.Fatalf("unexpected dial target: %s", dst)
				return nil
			}
		}, newRetryer(defaultRetryDelayFn))
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		defer client.Close()

		// Verify nodes passed to selector: [master, replica1, replica2] (s-down replica3 excluded).
		if len(observedNodes) != 3 ||
			observedNodes[0].Addr != "10.0.0.1:6379" ||
			observedNodes[1].Addr != "10.0.0.2:6379" ||
			observedNodes[2].Addr != "10.0.0.3:6379" {
			t.Fatalf("unexpected nodes passed to ReadNodeSelector: %+v", observedNodes)
		}

		ctx := context.Background()
		// Read goes to replica2 (index 2).
		val, err := client.Do(ctx, client.B().Get().Key("k").Build()).ToString()
		if err != nil || val != "from-replica2" {
			t.Fatalf("expected from-replica2, got %q (err=%v)", val, err)
		}
		// Write goes to master.
		val, err = client.Do(ctx, client.B().Set().Key("k").Value("v").Build()).ToString()
		if err != nil || val != "from-master" {
			t.Fatalf("expected from-master, got %q (err=%v)", val, err)
		}

		// Switch selector to primary (index 0) and refresh.
		selectedIdx.Store(0)
		if err := client.refresh(); err != nil {
			t.Fatalf("refresh failed: %v", err)
		}
		val, err = client.Do(ctx, client.B().Get().Key("k").Build()).ToString()
		if err != nil || val != "from-master" {
			t.Fatalf("expected from-master after selecting primary, got %q (err=%v)", val, err)
		}

		// Switch selector to replica1 (index 1) and refresh.
		selectedIdx.Store(1)
		if err := client.refresh(); err != nil {
			t.Fatalf("refresh failed: %v", err)
		}
		val, err = client.Do(ctx, client.B().Get().Key("k").Build()).ToString()
		if err != nil || val != "from-replica1" {
			t.Fatalf("expected from-replica1, got %q (err=%v)", val, err)
		}
	})

	t.Run("out-of-range index falls back to primary even with no eligible replicas", func(t *testing.T) {
		s0 := &mockConn{
			DoFn: func(cmd Completed) ValkeyResult { return ValkeyResult{} },
			DoMultiFn: func(multi ...Completed) *valkeyresults {
				return &valkeyresults{s: []ValkeyResult{
					{val: slicemsg('*', []ValkeyMessage{})},
					{val: slicemsg('*', []ValkeyMessage{strmsg('+', "10.0.0.1"), strmsg('+', "6379")})},
					{val: slicemsg('*', []ValkeyMessage{})}, // no replicas
				}}
			},
		}
		mConn := &mockConn{
			DoFn: func(cmd Completed) ValkeyResult {
				if cmd.Commands()[0] == "ROLE" {
					return ValkeyResult{val: slicemsg('*', []ValkeyMessage{strmsg('+', "master")})}
				}
				return ValkeyResult{val: strmsg('+', "from-master")}
			},
		}

		client, err := newSentinelClient(&ClientOption{
			InitAddress:    []string{"s0"},
			SendToReplicas: func(cmd Completed) bool { return cmd.IsReadOnly() },
			ReadNodeSelector: func(slot uint16, nodes []NodeInfo) int {
				return -1
			},
		}, func(dst string, opt *ClientOption) conn {
			if dst == "s0" {
				return s0
			}
			return mConn
		}, newRetryer(defaultRetryDelayFn))
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		defer client.Close()

		val, err := client.Do(context.Background(), client.B().Get().Key("k").Build()).ToString()
		if err != nil || val != "from-master" {
			t.Fatalf("expected from-master, got %q (err=%v)", val, err)
		}
	})
}
