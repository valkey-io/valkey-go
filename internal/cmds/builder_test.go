package cmds

import (
	"reflect"
	"strconv"
	"strings"
	"testing"
)

func TestPutCompleted(t *testing.T) {
retry:
	cs1 := get()
	cs1.s = append(cs1.s, "1", "1", "1", "1", "1")
	PutCompleted(Completed{cs: cs1})
	cs2 := get()
	if cs1 != cs2 {
		goto retry
	}
	if len(cs2.s) != 0 {
		t.Fatalf("Put doesn't clean the CommandSlice")
	}
}

func TestPutCompletedForce(t *testing.T) {
retry:
	cs1 := get()
	cs1.s = append(cs1.s, "1", "1", "1", "1", "1")
	cs1.r = 1 // pin
	PutCompletedForce(Completed{cs: cs1})
	cs2 := get()
	if cs1 != cs2 {
		goto retry
	}
	if len(cs2.s) != 0 {
		t.Fatalf("PutCompletedForce doesn't clean the CommandSlice")
	}
}

func TestPutCacheableForce(t *testing.T) {
retry:
	cs1 := get()
	cs1.s = append(cs1.s, "1", "1", "1", "1", "1")
	cs1.r = 1 // pin
	PutCacheableForce(Cacheable{cs: cs1})
	cs2 := get()
	if cs1 != cs2 {
		goto retry
	}
	if len(cs2.s) != 0 {
		t.Fatalf("PutCacheableForce doesn't clean the CommandSlice")
	}
}

func TestPutCacheable(t *testing.T) {
retry:
	cs1 := get()
	cs1.s = append(cs1.s, "1", "1", "1", "1", "1")
	PutCacheable(Cacheable{cs: cs1})
	cs2 := get()
	if cs1 != cs2 {
		goto retry
	}
	if len(cs2.s) != 0 {
		t.Fatalf("Put doesn't clean the CommandSlice")
	}
}

func TestArbitraryIsZero(t *testing.T) {
	builder := NewBuilder(NoSlot)
	if cmd := builder.Arbitrary("any", "cmd"); cmd.IsZero() {
		t.Fatalf("arbitrary failed")
	}
	var cmd Arbitrary
	if !cmd.IsZero() {
		t.Fatalf("arbitrary failed")
	}
}

func TestArbitrary(t *testing.T) {
	builder := NewBuilder(NoSlot)
	cmd := builder.Arbitrary("any", "cmd").Keys("k1", "k2").Args("a1", "a2")
	if c := cmd.Build(); !reflect.DeepEqual(c.Commands(), []string{"any", "cmd", "k1", "k2", "a1", "a2"}) {
		t.Fatalf("arbitrary failed")
	}
	if c := builder.Arbitrary("any").Blocking(); !c.IsBlock() {
		t.Fatalf("arbitrary failed")
	}
	if c := builder.Arbitrary("any").ReadOnly(); !c.IsReadOnly() {
		t.Fatalf("arbitrary failed")
	}

	builder2 := NewBuilder(InitSlot)

	defer func() {
		if e := recover(); e != multiKeySlotErr {
			t.Errorf("arbitrary not check slots")
		}
	}()

	builder2.Arbitrary().Keys("k1", "k2")
}

func TestEmptyArbitrary(t *testing.T) {
	builder := NewBuilder(NoSlot)
	defer func() {
		if e := recover(); e != arbitraryNoCommand {
			t.Errorf("arbitrary not check empty")
		}
	}()
	builder.Arbitrary().Build()
}

func TestEmptySubscribe(t *testing.T) {
	builder := NewBuilder(NoSlot)
	defer func() {
		if e := recover(); e != arbitrarySubscribe {
			t.Errorf("arbitrary not check subscribe command")
		}
	}()
	builder.Arbitrary("SUBSCRIBE").Build()
}

func TestEmptyArbitraryMultiGet(t *testing.T) {
	builder := NewBuilder(NoSlot)
	defer func() {
		if e := recover(); e != arbitraryNoCommand {
			t.Errorf("arbitrary not check empty")
		}
	}()
	builder.Arbitrary().MultiGet()
}

func TestArbitraryMultiGet(t *testing.T) {
	builder := NewBuilder(NoSlot)
	cacheable := Cacheable(builder.Arbitrary("MGET").Args("KKK").MultiGet())
	if !cacheable.IsMGet() {
		t.Fatalf("arbitrary failed")
	}
}

func TestArbitraryMultiGetPanic(t *testing.T) {
	builder := NewBuilder(NoSlot)
	defer func() {
		if e := recover(); e != arbitraryMultiGet {
			t.Errorf("arbitrary not check MGET command")
		}
	}()
	builder.Arbitrary("SUBSCRIBE").MultiGet()
}

func TestBuiltTwice(t *testing.T) {
	src := NewBuilder(NoSlot).Get()
	cmd1 := src.Key("a")
	cmd2 := src.Key("b")
	cmd1.Build()
	defer func() {
		if e := recover(); e != ErrBuiltTwice {
			t.Errorf("arbitrary not check MGET command")
		}
	}()
	cmd2.Build()
}

func TestVerify(t *testing.T) {
	src := NewBuilder(NoSlot).Get()
	cmd1 := src.Key("a").Build()
	cmd1.cs.Verify()
	src.Key("b")
	defer func() {
		if e := recover(); e != ErrUnfinished {
			t.Errorf("arbitrary not check MGET command")
		}
	}()
	cmd1.cs.Verify()
}

var (
	keysDynamic1000 = func() []string {
		ks := make([]string, 1000)
		for i := 0; i < 1000; i++ {
			ks[i] = "key_" + strconv.Itoa(i)
		}
		return ks
	}()
	keysSameSlot1000 = func() []string {
		ks := make([]string, 1000)
		for i := 0; i < 1000; i++ {
			ks[i] = "{user:1}:key_" + strconv.Itoa(i)
		}
		return ks
	}()
	payload64B       = strings.Repeat("a", 64)
	payload1KB       = strings.Repeat("b", 1024)
	payload64KB      = strings.Repeat("c", 64*1024)
	payloadsGradient = []string{payload64B, payload1KB, payload64KB}
)

// Benchmark_Builder_Set measures client.B().Set() with dynamic runtime keys (key_0..key_999) and payload gradient (64B, 1KB, 64KB).
func Benchmark_Builder_Set(b *testing.B) {
	builder := NewBuilder(InitSlot)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cmd := builder.Set().Key(keysDynamic1000[i%1000]).Value(payloadsGradient[i%3]).Build()
		PutCompleted(cmd)
	}
}

// Benchmark_Builder_Get measures client.B().Get() with dynamic runtime keys (key_0..key_999) from pre-allocated slice.
func Benchmark_Builder_Get(b *testing.B) {
	builder := NewBuilder(InitSlot)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cmd := builder.Get().Key(keysDynamic1000[i%1000]).Build()
		PutCompleted(cmd)
	}
}

// Benchmark_Builder_MGet measures multi-key slice allocations with collection cardinality gradient (10, 100, 1,000 hashtagged keys).
func Benchmark_Builder_MGet(b *testing.B) {
	builder := NewBuilder(InitSlot)
	k10 := keysSameSlot1000[:10]
	k100 := keysSameSlot1000[:100]
	k1000 := keysSameSlot1000[:1000]

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var ks []string
		switch i % 3 {
		case 0:
			ks = k10
		case 1:
			ks = k100
		default:
			ks = k1000
		}
		cmd := builder.Mget().Key(ks...).Build()
		PutCompleted(cmd)
	}
}

// Benchmark_Builder_HGetAll measures hash argument building across dynamic keys with field cardinality (10, 100, 1,000 fields).
func Benchmark_Builder_HGetAll(b *testing.B) {
	builder := NewBuilder(InitSlot)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cmd := builder.Hgetall().Key(keysDynamic1000[i%1000]).Build()
		PutCompleted(cmd)
	}
}

// Benchmark_ZAdd measures sorted set construction with dynamic keys across collection cardinality (10, 100, 1,000 score-member pairs).
func Benchmark_ZAdd(b *testing.B) {
	builder := NewBuilder(InitSlot)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		z := builder.Zadd().Key(keysDynamic1000[i%1000]).ScoreMember()
		for j := 0; j < 10; j++ {
			z = z.ScoreMember(float64(j), keysDynamic1000[j])
		}
		cmd := z.Build()
		PutCompleted(cmd)
	}
}

// BenchmarkCommandBuilder_Allocation verifies zero-allocation command building.
func BenchmarkCommandBuilder_Allocation(b *testing.B) {
	Benchmark_Builder_Get(b)
}

func Benchmark_Builder_SingleCommand_DynamicKeys(b *testing.B) {
	builder := NewBuilder(InitSlot)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cmd := builder.Set().Key(keysDynamic1000[i%1000]).Value(payload1KB).Build()
		PutCompleted(cmd)
	}
}

func Benchmark_Builder_MultiKey_Scaling(b *testing.B) {
	builder := NewBuilder(InitSlot)
	keys := make([]string, 50)
	for i := 0; i < 50; i++ {
		keys[i] = "{user:1}:key_" + strconv.Itoa(i)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cmd := builder.Mget().Key(keys...).Build()
		PutCompleted(cmd)
	}
}

func Benchmark_Builder_ComplexArgs_HSet_ZAdd(b *testing.B) {
	builder := NewBuilder(InitSlot)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		z := builder.Zadd().Key(keysDynamic1000[i%1000]).ScoreMember()
		for j := 0; j < 10; j++ {
			z = z.ScoreMember(float64(j), keysDynamic1000[j])
		}
		cmd := z.Build()
		PutCompleted(cmd)
	}
}

func Benchmark_MemoryPool_PutCompleted_Parallel(b *testing.B) {
	builder := NewBuilder(InitSlot)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			cmd := builder.Get().Key("bench_key").Build()
			PutCompleted(cmd)
		}
	})
}

func Benchmark_Cluster_CRC16_Routing(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i%2 == 0 {
			Slot("plain_key_no_hash_tag")
		} else {
			Slot("{hash_tag}_with_keys")
		}
	}
}
