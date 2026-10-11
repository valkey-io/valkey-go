package valkey

import (
	"bufio"
	"context"
	"net"
	"testing"
	"time"
)

func setupLifetimeNoBg(t *testing.T, lifetime time.Duration) (*pipe, func()) {
	t.Helper()
	return setupLifetimePipe(t, lifetime, true, false)
}

func setupLifetimePipe(t *testing.T, lifetime time.Duration, noBg, peerEOF bool) (*pipe, func()) {
	t.Helper()
	client, server := net.Pipe()
	mock := &valkeyMock{t: t, buf: bufio.NewReader(server), conn: server}
	go func() {
		mock.Expect("HELLO", "3").Reply(slicemsg('%', []ValkeyMessage{
			strmsg('+', "proto"), {typ: ':', intlen: 3},
		}))
		if peerEOF {
			server.Close()
		} else if !noBg {
			mock.Expect("PING").ReplyString("OK")
		}
	}()
	constructor := newPipe
	if noBg {
		constructor = newPipeNoBg
	}
	p, err := constructor(context.Background(), func(context.Context) (net.Conn, error) {
		return client, nil
	}, &ClientOption{ConnLifetime: lifetime, DisableCache: true, ClientSetInfo: []string{}, AlwaysPipelining: !noBg})
	if err != nil {
		client.Close()
		server.Close()
		t.Fatal(err)
	}
	return p, func() { client.Close(); server.Close() }
}

func TestLifetimeTimerStopsOnClose(t *testing.T) {
	for _, peerClosed := range []bool{false, true} {
		name := "explicit close"
		if peerClosed {
			name = "peer EOF"
		}
		t.Run(name, func(t *testing.T) {
			p, mock, closePipe, closeConn := setup(t, ClientOption{ConnLifetime: time.Hour})
			defer closeConn()
			defer p.lftmTimer.Load().Stop()
			if peerClosed {
				p.background()
				mock.Close()
				waitPipeClosed(t, p)
			} else {
				closePipe()
			}
			if p.lftmTimer.Load().Stop() {
				t.Fatal("closed pipe still has a pending lifetime timer")
			}
			p.ResetTimer()
			if p.lftmTimer.Load().Stop() {
				t.Fatal("ResetTimer rearmed a closed pipe")
			}
		})
	}
}

func TestLifetimeTimerNoBgClose(t *testing.T) {
	p, closeConn := setupLifetimeNoBg(t, time.Hour)
	defer closeConn()
	defer p.lftmTimer.Load().Stop()
	p.Close()
	if p.lftmTimer.Load().Stop() {
		t.Fatal("closed no-background pipe still has a pending lifetime timer")
	}
}

func TestLifetimeTimerTinyLifetime(t *testing.T) {
	for range 200 {
		p, closeConn := setupLifetimeNoBg(t, time.Nanosecond)
		deadline := time.Now().Add(time.Second)
		for p.Error() == nil && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		if p.Error() != errConnExpired {
			closeConn()
			t.Fatalf("tiny lifetime did not expire: %v", p.Error())
		}
		p.Close()
		closeConn()
	}
}

func TestLifetimeTimerPoolLifecycle(t *testing.T) {
	var closeConn func()
	pool := newPool(1, deadFn(), 0, 0, func(context.Context) wire {
		p, closeFn := setupLifetimeNoBg(t, time.Hour)
		closeConn = closeFn
		return p
	})
	p := pool.Acquire(context.Background()).(*pipe)
	defer closeConn()
	defer pool.Close()
	if p.lftmTimer.Load().Stop() {
		t.Fatal("checked-out pipe still has a pending lifetime timer")
	}
	pool.Store(p)
	if !p.lftmTimer.Load().Reset(time.Hour) {
		t.Fatal("stored pipe has no pending lifetime timer")
	}
	if next := pool.Acquire(context.Background()); next != p {
		t.Fatal("pool did not reuse the pipe")
	}
	if p.lftmTimer.Load().Stop() {
		t.Fatal("reacquired pipe still has a pending lifetime timer")
	}
	pool.Store(p)
	pool.Close()
	if p.lftmTimer.Load().Stop() {
		t.Fatal("pool close left a pending lifetime timer")
	}
}

func TestLifetimeTimerZeroLifetimeClose(t *testing.T) {
	p, closeConn := setupLifetimeNoBg(t, 0)
	defer closeConn()
	if p.lftmTimer.Load() != nil {
		t.Fatal("zero lifetime created a timer")
	}
	p.Close()
	p.Close()
	if p.Error() != ErrClosing {
		t.Fatalf("unexpected close error: %v", p.Error())
	}
}

func TestLifetimeTimerConcurrentResetClose(t *testing.T) {
	for range 100 {
		p, closeConn := setupLifetimeNoBg(t, time.Hour)
		done := make(chan struct{})
		go func() { p.ResetTimer(); close(done) }()
		p.Close()
		<-done
		closeConn()
		if p.lftmTimer.Load().Stop() {
			t.Fatal("concurrent ResetTimer rearmed a closed pipe")
		}
	}
}

func TestLifetimeTimerTinyBackgroundLifetime(t *testing.T) {
	for range 50 {
		p, closeConn := setupLifetimePipe(t, time.Nanosecond, false, false)
		waitPipeClosed(t, p)
		closeConn()
		if p.Error() != errConnExpired {
			t.Fatalf("unexpected expiry error: %v", p.Error())
		}
		if p.lftmTimer.Load().Stop() {
			t.Fatal("expired pipe has a pending timer")
		}
	}
}

func TestLifetimeTimerConstructorPeerEOF(t *testing.T) {
	for range 100 {
		p, closeConn := setupLifetimePipe(t, time.Hour, false, true)
		waitPipeClosed(t, p)
		closeConn()
		if p.Error() == nil {
			t.Fatal("peer EOF did not fail pipe")
		}
		if p.lftmTimer.Load().Stop() {
			t.Fatal("constructor left a timer on a failed pipe")
		}
	}
}

func TestLifetimeTimerPoolExpiry(t *testing.T) {
	var closeConn func()
	pool := newPool(1, deadFn(), 0, 0, func(context.Context) wire {
		p, closeFn := setupLifetimeNoBg(t, 100*time.Millisecond)
		closeConn = closeFn
		return p
	})
	p := pool.Acquire(context.Background()).(*pipe)
	defer closeConn()
	defer pool.Close()
	time.Sleep(150 * time.Millisecond)
	if p.Error() != nil {
		t.Fatalf("checked-out pipe expired: %v", p.Error())
	}
	pool.Store(p)
	deadline := time.Now().Add(2 * time.Second)
	for p.Error() == nil && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if p.Error() != errConnExpired {
		t.Fatalf("stored pipe did not expire: %v", p.Error())
	}
}
