package valkey

import (
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestSubs_Publish(t *testing.T) {
	defer ShouldNotLeak(SetupLeakDetection())
	t.Run("without subs", func(t *testing.T) {
		s := newSubs()
		s.Publish("aa", PubSubMessage{}) // just no block
	})

	t.Run("with multiple subs", func(t *testing.T) {
		s := newSubs()
		counts := map[string]int{
			"a": 0,
			"b": 0,
		}
		subFn := func(s PubSubSubscription) {
			counts[s.Channel]++
		}

		ch1, cancel1 := s.Subscribe([]string{"a"}, subFn)
		ch2, cancel2 := s.Subscribe([]string{"a"}, subFn)
		ch3, cancel3 := s.Subscribe([]string{"b"}, subFn)
		s.Confirm(PubSubSubscription{Channel: "a"})
		s.Confirm(PubSubSubscription{Channel: "b"})

		if counts["a"] != 2 || counts["b"] != 1 {
			t.Fatalf("unexpected counts %v", counts)
		}

		m1 := PubSubMessage{Pattern: "1", Channel: "2", Message: "3"}
		m2 := PubSubMessage{Pattern: "11", Channel: "22", Message: "33"}
		go func() {
			s.Publish("a", m1)
			s.Publish("b", m2)
		}()
		for m := range ch1 {
			if m != m1 {
				t.Fatalf("unexpected msg %v", m)
			}
			cancel1()
		}
		for m := range ch2 {
			if m != m1 {
				t.Fatalf("unexpected msg %v", m)
			}
			cancel2()
		}
		for m := range ch3 {
			if m != m2 {
				t.Fatalf("unexpected msg %v", m)
			}
			cancel3()
		}
	})

	t.Run("drain ch", func(t *testing.T) {
		s := newSubs()
		ch, cancel := s.Subscribe([]string{"a"}, nil)
		s.Publish("a", PubSubMessage{})
		if len(ch) != 1 {
			t.Fatalf("unexpected ch len %v", len(ch))
		}
		cancel()
		for ; len(ch) != 0; time.Sleep(time.Millisecond * 100) {
			t.Log("wait ch to be drain")
		}
	})
}

func TestSubs_Unsubscribe(t *testing.T) {
	defer ShouldNotLeak(SetupLeakDetection())
	s := newSubs()
	counts := map[string]int{"1": 0, "2": 0}
	subFn := func(s PubSubSubscription) {
		counts[s.Channel]++
	}
	ch, _ := s.Subscribe([]string{"1", "2"}, subFn)
	go func() {
		s.Publish("1", PubSubMessage{})
	}()
	_, ok := <-ch
	if !ok {
		t.Fatalf("unexpected ch closed")
	}
	s.Unsubscribe(PubSubSubscription{Channel: "1"})
	if counts["1"] != 1 {
		t.Fatalf("unexpected counts %v", counts)
	}
	_, ok = <-ch
	if ok {
		t.Fatalf("unexpected ch unclosed")
	}
}

var (
	pubsubChannels1000     []string
	pubsubPayloadsGradient = map[string]string{
		"64B":  strings.Repeat("a", 64),
		"1KB":  strings.Repeat("b", 1024),
		"64KB": strings.Repeat("c", 65536),
	}
)

func init() {
	pubsubChannels1000 = make([]string, 1000)
	for i := 0; i < 1000; i++ {
		pubsubChannels1000[i] = "chan_" + strconv.Itoa(i)
	}
}

// Benchmark_PubSub_Receive measures blocking loop message delivery with dynamic channels and payload gradient.
func Benchmark_PubSub_Receive(b *testing.B) {
	for _, size := range []string{"64B", "1KB", "64KB"} {
		payload := pubsubPayloadsGradient[size]
		b.Run("Payload="+size, func(b *testing.B) {
			s := newSubs()
			ch, cancel := s.Subscribe(pubsubChannels1000, nil)
			defer cancel()
			for _, chName := range pubsubChannels1000 {
				s.Confirm(PubSubSubscription{Channel: chName})
			}

			messages := make([]PubSubMessage, len(pubsubChannels1000))
			for i, chName := range pubsubChannels1000 {
				messages[i] = PubSubMessage{Channel: chName, Message: payload}
			}

			ready := make(chan struct{})
			done := make(chan struct{})
			var wg sync.WaitGroup
			wg.Add(1)

			go func() {
				defer wg.Done()
				idx := 0
				for {
					select {
					case <-done:
						return
					case <-ready:
						s.Publish(pubsubChannels1000[idx], messages[idx])
						idx = (idx + 1) % len(pubsubChannels1000)
					}
				}
			}()

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ready <- struct{}{}
				<-ch
			}
			b.StopTimer()
			close(done)
			wg.Wait()
		})
	}
}

func Benchmark_PubSub_Dispatch_Loop(b *testing.B) {
	Benchmark_PubSub_Receive(b) // Alias to the existing Benchmark_PubSub_Receive which does exactly this
}
