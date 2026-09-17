package valkey

import (
	"context"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/valkey-io/valkey-go/internal/cmds"
)

//gocyclo:ignore
func TestRing(t *testing.T) {
	defer ShouldNotLeak(SetupLeakDetection())
	t.Run("PutOne", func(t *testing.T) {
		ring := newRing(DefaultRingScale)
		size := 5000
		fixture := make(map[string]struct{}, size)
		for i := range size {
			fixture[strconv.Itoa(i)] = struct{}{}
		}

		for cmd := range fixture {
			go ring.PutOne(context.Background(), cmds.NewCompleted([]string{cmd}))
		}

		for len(fixture) != 0 {
			cmd1, _, _ := ring.NextWriteCmd()
			if cmd1.IsEmpty() {
				runtime.Gosched()
				continue
			}
			cmd2, _, ch, _ := ring.NextResultCh()
			ring.FinishResult()
			if cmd1.Commands()[0] != cmd2.Commands()[0] {
				t.Fatalf("cmds read by NextWriteCmd and NextResultCh is not the same one")
			}
			if ch == nil || len(ch) != 0 {
				t.Fatalf("channel from NextResultCh is broken")
			}
			delete(fixture, cmd1.Commands()[0])
		}
	})

	t.Run("PutMulti", func(t *testing.T) {
		ring := newRing(DefaultRingScale)
		size := 5000
		fixture := make(map[string]struct{}, size)
		for i := range size {
			fixture[strconv.Itoa(i)] = struct{}{}
		}

		base := [][]string{{"a"}, {"b"}, {"c"}, {"d"}}
		for cmd := range fixture {
			go ring.PutMulti(context.Background(), cmds.NewMultiCompleted(append([][]string{{cmd}}, base...)), nil)
		}

		for len(fixture) != 0 {
			_, cmd1, _ := ring.NextWriteCmd()
			if cmd1 == nil {
				runtime.Gosched()
				continue
			}
			_, cmd2, ch, _ := ring.NextResultCh()
			ring.FinishResult()
			for j := range cmd1 {
				if cmd1[j].Commands()[0] != cmd2[j].Commands()[0] {
					t.Fatalf("cmds read by NextWriteCmd and NextResultCh is not the same one")
				}
			}
			if ch == nil || len(ch) != 0 {
				t.Fatalf("channel from NextResultCh is broken")
			}
			delete(fixture, cmd1[0].Commands()[0])
		}
	})

	t.Run("NextWriteCmd & NextResultCh", func(t *testing.T) {
		ring := newRing(DefaultRingScale)
		if one, multi, _ := ring.NextWriteCmd(); !one.IsEmpty() || multi != nil {
			t.Fatalf("NextWriteCmd should returns nil if empty")
		}
		if one, multi, ch, _ := ring.NextResultCh(); !one.IsEmpty() || multi != nil || ch != nil {
			t.Fatalf("NextResultCh should returns nil if not NextWriteCmd yet")
		} else {
			ring.FinishResult()
		}

		ring.PutOne(context.Background(), cmds.NewCompleted([]string{"0"}))
		if one, _, _ := ring.NextWriteCmd(); len(one.Commands()) == 0 || one.Commands()[0] != "0" {
			t.Fatalf("NextWriteCmd should returns next cmd")
		}
		if one, _, ch, _ := ring.NextResultCh(); len(one.Commands()) == 0 || one.Commands()[0] != "0" || ch == nil {
			t.Fatalf("NextResultCh should returns next cmd after NextWriteCmd")
		} else {
			ring.FinishResult()
		}

		ring.PutMulti(context.Background(), cmds.NewMultiCompleted([][]string{{"0"}}), nil)
		if _, multi, _ := ring.NextWriteCmd(); len(multi) == 0 || multi[0].Commands()[0] != "0" {
			t.Fatalf("NextWriteCmd should returns next cmd")
		}
		if _, multi, ch, _ := ring.NextResultCh(); len(multi) == 0 || multi[0].Commands()[0] != "0" || ch == nil {
			t.Fatalf("NextResultCh should returns next cmd after NextWriteCmd")
		} else {
			ring.FinishResult()
		}
	})

	t.Run("PutOne Wakeup WaitForWrite", func(t *testing.T) {
		ring := newRing(DefaultRingScale)
		if one, _, ch := ring.NextWriteCmd(); ch == nil {
			go func() {
				time.Sleep(time.Millisecond * 100)
				ring.PutOne(context.Background(), cmds.PingCmd)
			}()
			if one, _, ch = ring.WaitForWrite(); ch != nil && one.Commands()[0] == cmds.PingCmd.Commands()[0] {
				return
			}
		}
		t.Fatal("Should sleep")
	})

	t.Run("PutMulti Wakeup WaitForWrite", func(t *testing.T) {
		ring := newRing(DefaultRingScale)
		if _, _, ch := ring.NextWriteCmd(); ch == nil {
			go func() {
				time.Sleep(time.Millisecond * 100)
				ring.PutMulti(context.Background(), []Completed{cmds.PingCmd}, nil)
			}()
			if _, multi, ch := ring.WaitForWrite(); ch != nil && multi[0].Commands()[0] == cmds.PingCmd.Commands()[0] {
				return
			}
		}
		t.Fatal("Should sleep")
	})
}

// Benchmark_Ring_Multiplexer_Contention measures concurrent goroutine contention on the ring buffer multiplexer.
func Benchmark_Ring_Multiplexer_Contention(b *testing.B) {
	r := newRing(DefaultRingScale)
	cmd := cmds.NewCompleted([]string{"PING"})
	ctx := context.Background()

	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			default:
				one, multi, _ := r.NextWriteCmd()
				if !one.IsEmpty() || len(multi) > 0 {
					_, _, _, _ = r.NextResultCh()
					r.FinishResult()
				} else {
					runtime.Gosched()
				}
			}
		}
	}()

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = r.PutOne(ctx, cmd)
		}
	})
	b.StopTimer()
	close(done)
	wg.Wait()
}

// BenchmarkRingBufferMultiplexer_Contention is an alias for CI naming compatibility.
func BenchmarkRingBufferMultiplexer_Contention(b *testing.B) {
	Benchmark_Ring_Multiplexer_Contention(b)
}
