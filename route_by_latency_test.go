package valkey

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/valkey-io/valkey-go/internal/cmds"
)

func TestPickLowestLatencyReplica(t *testing.T) {
	var lc net.ListenConfig
	ctx := context.Background()

	l1, err := lc.Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen l1: %v", err)
	}
	defer l1.Close()

	l2, err := lc.Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen l2: %v", err)
	}
	defer l2.Close()

	l3, err := lc.Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen l3: %v", err)
	}
	defer l3.Close()

	var wg sync.WaitGroup
	stopCh := make(chan struct{})

	serveMock := func(l net.Listener, delay time.Duration) {
		defer wg.Done()
		for {
			conn, err := l.Accept()
			if err != nil {
				select {
				case <-stopCh:
					return
				default:
					return
				}
			}
			go func(c net.Conn) {
				defer c.Close()
				var buf [32]byte
				_, _ = c.Read(buf[:])
				if delay > 0 {
					time.Sleep(delay)
				}
				_, _ = c.Write([]byte("+PONG\r\n"))
			}(conn)
		}
	}

	wg.Add(3)
	go serveMock(l1, 60*time.Millisecond)
	go serveMock(l2, 0) // fast node (simulates local node)
	go serveMock(l3, 30*time.Millisecond)

	defer func() {
		close(stopCh)
		l1.Close()
		l2.Close()
		l3.Close()
		wg.Wait()
	}()

	host1, port1, _ := net.SplitHostPort(l1.Addr().String())
	host2, port2, _ := net.SplitHostPort(l2.Addr().String())
	host3, port3, _ := net.SplitHostPort(l3.Addr().String())

	eligible := []map[string]string{
		{"ip": host1, "port": port1},
		{"ip": host2, "port": port2},
		{"ip": host3, "port": port3},
	}

	client := &sentinelClient{
		mOpt: &ClientOption{
			RouteByLatency: true,
			Dialer: net.Dialer{
				Timeout: 200 * time.Millisecond,
			},
		},
	}

	for i := 0; i < 5; i++ {
		best, err := client.pickLowestLatencyReplica(eligible, "")
		if err != nil {
			t.Fatalf("iteration %d: pickLowestLatencyReplica failed: %v", i, err)
		}

		expectedAddr := fmt.Sprintf("%s:%s", host2, port2)
		if best != expectedAddr {
			t.Fatalf("iteration %d: expected lowest latency node %s, got %s", i, expectedAddr, best)
		}
	}
}

func TestPickLowestLatencyReplica_FailbackToFasterNode(t *testing.T) {
	var lc net.ListenConfig
	ctx := context.Background()

	// l1 is slow remote node (currently in use)
	// l2 is fast local node that just recovered
	l1, err := lc.Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen l1: %v", err)
	}
	defer l1.Close()

	l2, err := lc.Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen l2: %v", err)
	}
	defer l2.Close()

	var wg sync.WaitGroup
	stopCh := make(chan struct{})

	serveMock := func(l net.Listener, delay time.Duration) {
		defer wg.Done()
		for {
			conn, err := l.Accept()
			if err != nil {
				select {
				case <-stopCh:
					return
				default:
					return
				}
			}
			go func(c net.Conn) {
				defer c.Close()
				var buf [32]byte
				_, _ = c.Read(buf[:])
				if delay > 0 {
					time.Sleep(delay)
				}
				_, _ = c.Write([]byte("+PONG\r\n"))
			}(conn)
		}
	}

	wg.Add(2)
	go serveMock(l1, 50*time.Millisecond) // currently in use, but slow
	go serveMock(l2, 0)                   // recovered fast node

	defer func() {
		close(stopCh)
		l1.Close()
		l2.Close()
		wg.Wait()
	}()

	host1, port1, _ := net.SplitHostPort(l1.Addr().String())
	host2, port2, _ := net.SplitHostPort(l2.Addr().String())

	eligible := []map[string]string{
		{"ip": host1, "port": port1},
		{"ip": host2, "port": port2},
	}

	client := &sentinelClient{
		mOpt: &ClientOption{
			RouteByLatency: true,
			Dialer: net.Dialer{
				Timeout: 200 * time.Millisecond,
			},
		},
	}

	currentSlowAddr := fmt.Sprintf("%s:%s", host1, port1)
	expectedFastAddr := fmt.Sprintf("%s:%s", host2, port2)

	// Even though current was set to slow node, client should failback to faster recovered node
	best, err := client.pickLowestLatencyReplica(eligible, currentSlowAddr)
	if err != nil {
		t.Fatalf("pickLowestLatencyReplica failed: %v", err)
	}

	if best != expectedFastAddr {
		t.Fatalf("expected failback to %s, got %s", expectedFastAddr, best)
	}
}

func TestPickLowestLatencyReplica_WithUnreachableNodes(t *testing.T) {
	var lc net.ListenConfig
	ctx := context.Background()

	l, err := lc.Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer l.Close()

	unreach1, err := lc.Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen unreach1: %v", err)
	}
	unreachPort1 := strconv.Itoa(unreach1.Addr().(*net.TCPAddr).Port)
	_ = unreach1.Close()

	unreach2, err := lc.Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen unreach2: %v", err)
	}
	unreachPort2 := strconv.Itoa(unreach2.Addr().(*net.TCPAddr).Port)
	_ = unreach2.Close()

	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			var buf [32]byte
			_, _ = conn.Read(buf[:])
			_, _ = conn.Write([]byte("+PONG\r\n"))
			_ = conn.Close()
		}
	}()

	validHost, validPort, _ := net.SplitHostPort(l.Addr().String())

	eligible := []map[string]string{
		{"ip": "127.0.0.1", "port": unreachPort1}, // unreachable
		{"ip": validHost, "port": validPort},      // reachable
		{"ip": "127.0.0.1", "port": unreachPort2}, // unreachable
	}

	client := &sentinelClient{
		mOpt: &ClientOption{
			RouteByLatency: true,
			Dialer: net.Dialer{
				Timeout: 100 * time.Millisecond,
			},
		},
	}

	best, err := client.pickLowestLatencyReplica(eligible, "")
	if err != nil {
		t.Fatalf("pickLowestLatencyReplica failed with unreachable nodes: %v", err)
	}

	expected := fmt.Sprintf("%s:%s", validHost, validPort)
	if best != expected {
		t.Fatalf("expected reachable node %s, got %s", expected, best)
	}
}

func TestSentinelAutoEnableSendToReplicasAndRefreshInterval(t *testing.T) {
	opt := &ClientOption{
		InitAddress:    []string{"127.0.0.1:0"},
		Sentinel:       SentinelOption{MasterSet: "mymaster"},
		RouteByLatency: true,
	}

	sentinelMock := &mockConn{
		DoMultiFn: func(cmd ...Completed) *valkeyresults {
			return &valkeyresults{
				s: []ValkeyResult{
					{
						val: slicemsg('*', []ValkeyMessage{
							slicemsg('%', []ValkeyMessage{
								strmsg('+', "ip"), strmsg('+', "127.0.0.1"),
								strmsg('+', "port"), strmsg('+', "0"),
							}),
						}),
					},
					{
						val: slicemsg('*', []ValkeyMessage{
							strmsg('+', "127.0.1.0"),
							strmsg('+', "10"),
						}),
					},
					{
						val: slicemsg('*', []ValkeyMessage{
							slicemsg('%', []ValkeyMessage{
								strmsg('+', "ip"), strmsg('+', "127.0.1.1"),
								strmsg('+', "port"), strmsg('+', "11"),
							}),
						}),
					},
				},
			}
		},
	}

	client, err := newSentinelClient(
		opt,
		func(dst string, opt *ClientOption) conn {
			if dst == "127.0.0.1:0" {
				return sentinelMock
			}
			if dst == "127.0.1.0:10" {
				return &mockConn{
					DoFn: func(cmd Completed) ValkeyResult {
						return ValkeyResult{val: slicemsg('*', []ValkeyMessage{strmsg('+', "master")})}
					},
				}
			}
			if dst == "127.0.1.1:11" {
				return &mockConn{
					DoFn: func(cmd Completed) ValkeyResult {
						return ValkeyResult{val: slicemsg('*', []ValkeyMessage{strmsg('+', "slave")})}
					},
				}
			}
			return &mockConn{}
		},
		newRetryer(defaultRetryDelayFn),
	)
	if err != nil {
		t.Fatalf("unexpected error creating sentinel client: %v", err)
	}
	defer client.Close()

	if opt.SendToReplicas == nil {
		t.Fatal("expected SendToReplicas to be auto-configured")
	}

	b := cmds.NewBuilder(cmds.NoSlot)
	readCmd := b.Get().Key("key").Build()
	writeCmd := b.Set().Key("key").Value("value").Build()

	if !opt.SendToReplicas(readCmd) {
		t.Fatal("expected SendToReplicas to return true for read commands")
	}

	if opt.SendToReplicas(writeCmd) {
		t.Fatal("expected SendToReplicas to return false for write commands")
	}

	if opt.Sentinel.TopologyRefreshInterval != 10*time.Second {
		t.Fatalf("expected TopologyRefreshInterval 10s, got %v", opt.Sentinel.TopologyRefreshInterval)
	}
}

