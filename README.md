# valkey-go

[![Go Reference](https://pkg.go.dev/badge/github.com/valkey-io/valkey-go.svg)](https://pkg.go.dev/github.com/valkey-io/valkey-go)
[![CircleCI](https://dl.circleci.com/status-badge/img/gh/valkey-io/valkey-go/tree/main.svg?style=shield)](https://dl.circleci.com/status-badge/redirect/gh/valkey-io/valkey-go/tree/main)
[![Go Report Card](https://goreportcard.com/badge/github.com/valkey-io/valkey-go)](https://goreportcard.com/report/github.com/valkey-io/valkey-go)
[![codecov](https://codecov.io/gh/valkey-io/valkey-go/branch/main/graph/badge.svg?token=26SfMHyUJ3)](https://codecov.io/gh/valkey-io/valkey-go)

A fast Golang Valkey client that does auto pipelining and supports server-assisted client-side caching.

## Features

* [Auto pipelining for non-blocking valkey commands](#auto-pipelining)
* [Server-assisted client-side caching](#server-assisted-client-side-caching)
* [Generic Object Mapping with client-side caching](./om)
* [Cache-Aside pattern with client-side caching](./valkeyaside)
* [Distributed Locks with client-side caching](./valkeylock)
* [Helpers for writing tests with valkey mock](./mock)
* [OpenTelemetry integration](./valkeyotel)
* [Hooks and other integrations](./valkeyhook)
* [Go-redis like API adapter](./valkeycompat) by [@418Coffee](https://github.com/418Coffee)
* Pub/Sub, Sharded Pub/Sub, Streams
* Valkey Cluster, Sentinel, RedisJSON, RedisBloom, RediSearch, RedisTimeseries, etc.
* [Probabilistic Data Structures without Redis Stack](./valkeyprob)
* [Availability zone affinity routing](#availability-zone-affinity-routing)

---

## Getting Started

```golang
package main

import (
	"context"
	"github.com/valkey-io/valkey-go"
)

func main() {
	client, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{"127.0.0.1:6379"}})
	if err != nil {
		panic(err)
	}
	defer client.Close()

	ctx := context.Background()
	// SET key val NX
	err = client.Do(ctx, client.B().Set().Key("key").Value("val").Nx().Build()).Error()
	// HGETALL hm
	hm, err := client.Do(ctx, client.B().Hgetall().Key("hm").Build()).AsStrMap()
}
```

Check out more examples: [Command Response Cheatsheet](https://github.com/valkey-io/valkey-go#command-response-cheatsheet)

## Developer Friendly Command Builder

`client.B()` is the builder entry point to construct a valkey command:

![Developer friendly command builder](https://user-images.githubusercontent.com/2727535/209358313-39000aee-eaa4-42e1-9748-0d3836c1264f.gif)\
<sub>_Recorded by @FZambia [Improving Centrifugo Redis Engine throughput and allocation efficiency with Rueidis Go library
](https://centrifugal.dev/blog/2022/12/20/improving-redis-engine-performance)_</sub>

Once a command is built, use either `client.Do()` or `client.DoMulti()` to send it to valkey.

**You ❗️SHOULD NOT❗️ reuse the command to another `client.Do()` or `client.DoMulti()` call because it has been recycled to the underlying `sync.Pool` by default.**

To reuse a command, use `Pin()` after `Build()` and it will prevent the command from being recycled.

## [Pipelining](https://redis.io/docs/manual/pipelining/)

### Auto Pipelining

All concurrent non-blocking valkey commands (such as `GET`, `SET`) are automatically pipelined by default,
which reduces the overall round trips and system calls and gets higher throughput. You can easily get the benefit
of [pipelining technique](https://redis.io/docs/manual/pipelining/) by just calling `client.Do()` from multiple goroutines concurrently.
For example:

```go
func BenchmarkPipelining(b *testing.B, client valkey.Client) {
	// the below client.Do() operations will be issued from
	// multiple goroutines and thus will be pipelined automatically.
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			client.Do(context.Background(), client.B().Get().Key("k").Build()).ToString()
		}
	})
}
```

### Benchmark Comparison with go-redis v9

Compared to go-redis, valkey-go has higher throughput across 1, 8, and 64 parallelism settings.

It is even able to achieve **~14x** throughput over go-redis in a local benchmark of MacBook Pro 16" M1 Pro 2021. (see `parallelism(64)-key(16)-value(64)-10`)

![client_test_set](https://github.com/rueian/rueidis-benchmark/blob/master/client_test_set_10.png)

Benchmark source code: https://github.com/rueian/rueidis-benchmark

A benchmark result performed on two GCP n2-highcpu-2 machines also shows that valkey-go can achieve higher throughput with lower latencies: https://github.com/redis/rueidis/pull/93

### Disable Auto Pipelining

While auto pipelining maximizes throughput, it relies on additional goroutines to process requests and responses and may add some latencies due to goroutine scheduling and head of line blocking.

You can avoid this by setting `DisableAutoPipelining` to true, then it will switch to connection pooling approach and serve each request with dedicated connection on the same goroutine.

When `DisableAutoPipelining` is set to true, you can still send commands for auto pipelining with `ToPipe()`:

``` golang
cmd := client.B().Get().Key("key").Build().ToPipe()
client.Do(ctx, cmd)
```

This allows you to use connection pooling approach by default but opt-in auto pipelining for a subset of requests.

### Manual Pipelining

Besides auto pipelining, you can also pipeline commands manually with `DoMulti()`:

``` golang
cmds := make(valkey.Commands, 0, 10)
for i := 0; i < 10; i++ {
    cmds = append(cmds, client.B().Set().Key("key").Value("value").Build())
}
for _, resp := range client.DoMulti(ctx, cmds...) {
    if err := resp.Error(); err != nil {
        panic(err)
    }
}
```

When using `DoMulti()` to send multiple commands, the original commands are recycled after execution by default.
If you need to reference them afterward (e.g. to retrieve the key), use the `Pin()` method to prevent recycling.

```golang
// Create pinned commands to preserve them from being recycled
cmds := make(valkey.Commands, 0, 10)
for i := 0; i < 10; i++ {
	cmds = append(cmds, client.B().Get().Key(strconv.Itoa(i)).Build().Pin())
}

// Execute commands and process responses
for i, resp := range client.DoMulti(context.Background(), cmds...) {
	fmt.Println(resp.ToString()) // this is the result
	fmt.Println(cmds[i].Commands()[1]) // this is the corresponding key
}
```

Alternatively, you can use the `MGet` and `MGetCache` helper functions to easily map keys to their corresponding responses.

```golang
val, err := MGet(client, ctx, []string{"k1", "k2"})
fmt.Println(val["k1"].ToString()) // this is the k1 value
```

## [Server-Assisted Client-Side Caching](https://redis.io/docs/manual/client-side-caching/)

The opt-in mode of [server-assisted client-side caching](https://redis.io/docs/manual/client-side-caching/) is enabled by default and can be used by calling `DoCache()` or `DoMultiCache()` with client-side TTLs specified.

```golang
client.DoCache(ctx, client.B().Hmget().Key("mk").Field("1", "2").Cache(), time.Minute).ToArray()
client.DoMultiCache(ctx,
    valkey.CT(client.B().Get().Key("k1").Cache(), 1*time.Minute),
    valkey.CT(client.B().Get().Key("k2").Cache(), 2*time.Minute))
```

Cached responses, including Valkey Nils, will be invalidated either when being notified by valkey servers or when their client-side TTLs are reached. See https://github.com/redis/rueidis/issues/534 for more details.

### Benchmark

Server-assisted client-side caching can dramatically boost latencies and throughput just like **having a valkey replica right inside your application**. For example:

![client_test_get](https://github.com/rueian/rueidis-benchmark/blob/master/client_test_get_10.png)

Benchmark source code: https://github.com/rueian/rueidis-benchmark

### Client-Side Caching Helpers

Use `CacheTTL()` to check the remaining client-side TTL in seconds:

```golang
client.DoCache(ctx, client.B().Get().Key("k1").Cache(), time.Minute).CacheTTL() == 60
```

Use `IsCacheHit()` to verify if the response came from the client-side memory:

```golang
client.DoCache(ctx, client.B().Get().Key("k1").Cache(), time.Minute).IsCacheHit() == true
```

If the OpenTelemetry is enabled by the `valkeyotel.NewClient(option)`, then there are also two metrics instrumented:

* valkey_do_cache_miss
* valkey_do_cache_hits

### MGET/JSON.MGET Client-Side Caching Helpers

`valkey.MGetCache` and `valkey.JsonMGetCache` are handy helpers fetching multiple keys across different slots through the client-side caching.
They will first group keys by slot to build `MGET` or `JSON.MGET` commands respectively and then send requests with only cache missed keys to valkey nodes.

### Broadcast Mode Client-Side Caching

Although the default is opt-in mode, you can use broadcast mode by specifying your prefixes in `ClientOption.ClientTrackingOptions`:

```go
client, err := valkey.NewClient(valkey.ClientOption{
	InitAddress:           []string{"127.0.0.1:6379"},
	ClientTrackingOptions: []string{"PREFIX", "prefix1:", "PREFIX", "prefix2:", "BCAST"},
})
if err != nil {
	panic(err)
}
client.DoCache(ctx, client.B().Get().Key("prefix1:1").Cache(), time.Minute).IsCacheHit() == false
client.DoCache(ctx, client.B().Get().Key("prefix1:1").Cache(), time.Minute).IsCacheHit() == true
```

Please make sure that commands passed to `DoCache()` and `DoMultiCache()` are covered by your prefixes.
Otherwise, their client-side cache will not be invalidated by valkey.

### Client-Side Caching with Cache Aside Pattern

Cache-Aside is a widely used caching strategy.
[valkeyaside](https://github.com/valkey-io/valkey-go/blob/main/valkeyaside/README.md) can help you cache data into your client-side cache backed by Valkey. For example:

```go
client, err := valkeyaside.NewClient(valkeyaside.ClientOption{
    ClientOption: valkey.ClientOption{InitAddress: []string{"127.0.0.1:6379"}},
})
if err != nil {
    panic(err)
}
val, err := client.Get(context.Background(), time.Minute, "mykey", func(ctx context.Context, key string) (val string, err error) {
    if err = db.QueryRowContext(ctx, "SELECT val FROM mytab WHERE id = ?", key).Scan(&val); err == sql.ErrNoRows {
        val = "_nil_" // cache nil to avoid penetration.
        err = nil     // clear err in case of sql.ErrNoRows.
    }
    return
})
// ...
```

Please refer to the full example at [valkeyaside](https://github.com/valkey-io/valkey-go/blob/main/valkeyaside/README.md).

### Disable Client-Side Caching

Some Valkey providers don't support client-side caching, ex. Google Cloud Memorystore.
You can disable client-side caching by setting `ClientOption.DisableCache` to `true`.
This will also fall back `client.DoCache()` and `client.DoMultiCache()` to `client.Do()` and `client.DoMulti()`.

## Context Cancellation

`client.Do()`, `client.DoMulti()`, `client.DoCache()`, and `client.DoMultiCache()` can return early if the context deadline is reached.

```golang
ctx, cancel := context.WithTimeout(context.Background(), time.Second)
defer cancel()
client.Do(ctx, client.B().Set().Key("key").Value("val").Nx().Build()).Error() == context.DeadlineExceeded
```

Please note that though operations can return early, the command is likely sent already.

### Canceling a Context Before Its Deadline

Manually canceling a context is only work in pipeline mode, as it requires an additional goroutine to monitor the context.
Pipeline mode will be started automatically when there are concurrent requests on the same connection, but you can start it in advance with `ClientOption.AlwaysPipelining`
to make sure manually cancellation is respected, especially for blocking requests which are sent with a dedicated connection where pipeline mode isn't started.

### Disable Auto Retry

All read-only commands are automatically retried on failures by default before their context deadlines exceeded.
You can disable this by setting `DisableRetry` or adjust the number of retries and durations between retries using `RetryDelay` function.

## Pub/Sub

To receive messages from channels, `client.Receive()` should be used. It supports `SUBSCRIBE`, `PSUBSCRIBE`, and Valkey 7.0's `SSUBSCRIBE`:

```golang
err = client.Receive(context.Background(), client.B().Subscribe().Channel("ch1", "ch2").Build(), func(msg valkey.PubSubMessage) {
    // Handle the message. If you need to perform heavy processing or issue
    // additional commands, do that in a separate goroutine to avoid
    // blocking the pipeline, e.g.:
    //   go func() {
    //       // long work or client.Do(...)
    //   }()
})
```

The provided handler will be called with the received message.

It is important to note that `client.Receive()` will keep blocking until returning a value in the following cases:

1. return `nil` when receiving any unsubscribe/punsubscribe message related to the provided `subscribe` command, including `sunsubscribe` messages caused by slot migrations.
2. return `valkey.ErrClosing` when the client is closed manually.
3. return `ctx.Err()` when the `ctx` is done.
4. return non-nil `err` when the provided `subscribe` command fails.

While the `client.Receive()` call is blocking, the `Client` is still able to accept other concurrent requests,
and they are sharing the same TCP connection. If your message handler may take some time to complete, it is recommended
to use the `client.Receive()` inside a `client.Dedicated()` for not blocking other concurrent requests.

#### Subscription confirmations

Use `valkey.WithOnSubscriptionHook` when you need to observe subscribe / unsubscribe confirmations that the server sends during the lifetime of a `client.Receive()`.

The hook can be triggered multiple times because the `client.Receive()` may automatically reconnect and resubscribe.

```go
ctx := valkey.WithOnSubscriptionHook(context.Background(), func(s valkey.PubSubSubscription) {
    // This hook runs in the pipeline goroutine. If you need to perform
    // heavy work or invoke additional commands, do it in another
    // goroutine to avoid blocking the pipeline, for example:
    //   go func() {
    //       // long work or client.Do(...)
    //   }()
    fmt.Printf("%s %s (count %d)\n", s.Kind, s.Channel, s.Count)
})

err := client.Receive(ctx, client.B().Subscribe().Channel("news").Build(), func(m valkey.PubSubMessage) {
    // ...
})
```

### Alternative PubSub Hooks

The `client.Receive()` requires users to provide a subscription command in advance.
There is an alternative `Dedicatedclient.SetPubSubHooks()` that allows users to subscribe/unsubscribe channels later.

```golang
c, cancel := client.Dedicate()
defer cancel()

wait := c.SetPubSubHooks(valkey.PubSubHooks{
	OnMessage: func(m valkey.PubSubMessage) {
		// Handle the message. If you need to perform heavy processing or issue
		// additional commands, do that in a separate goroutine to avoid
		// blocking the pipeline, e.g.:
		//   go func() {
		//       // long work or client.Do(...)
		//   }()
	}
})
c.Do(ctx, c.B().Subscribe().Channel("ch").Build())
err := <-wait // disconnected with err
```

If the hooks are not nil, the above `wait` channel is guaranteed to be closed when the hooks will not be called anymore,
and produce at most one error describing the reason. Users can use this channel to detect disconnection.

## CAS Transaction

To do a [CAS Transaction](https://redis.io/docs/interact/transactions/#optimistic-locking-using-check-and-set) (`WATCH` + `MULTI` + `EXEC`), a dedicated connection should be used because there should be no
unintentional write commands between `WATCH` and `EXEC`. Otherwise, the `EXEC` may not fail as expected.

```golang
client.Dedicated(func(c valkey.DedicatedClient) error {
    // watch keys first
    c.Do(ctx, c.B().Watch().Key("k1", "k2").Build())
    // perform read here
    c.Do(ctx, c.B().Mget().Key("k1", "k2").Build())
    // perform write with MULTI EXEC
    c.DoMulti(
        ctx,
        c.B().Multi().Build(),
        c.B().Set().Key("k1").Value("1").Build(),
        c.B().Set().Key("k2").Value("2").Build(),
        c.B().Exec().Build(),
    )
    return nil
})

```

Or use `Dedicate()` and invoke `cancel()` when finished to put the connection back to the pool.

``` golang
c, cancel := client.Dedicate()
defer cancel()

c.Do(ctx, c.B().Watch().Key("k1", "k2").Build())
// do the rest CAS operations with the `client` who occupies a connection
```

However, occupying a connection is not good in terms of throughput. It is better to use [Lua script](#lua-script) to perform
optimistic locking instead.

## Lua Script

The `NewLuaScript` or `NewLuaScriptReadOnly` will create a script which is safe for concurrent usage.

When calling the `script.Exec`, it will try sending `EVALSHA` first and fall back to `EVAL` if the server returns `NOSCRIPT`.

```golang
script := valkey.NewLuaScript("return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}")
// the script.Exec is safe for concurrent call
list, err := script.Exec(ctx, client, []string{"k1", "k2"}, []string{"a1", "a2"}).ToArray()
```

## Streaming Read

`client.DoStream()` and `client.DoMultiStream()` can be used to send large valkey responses to an `io.Writer`
directly without allocating them to the memory. They work by first sending commands to a dedicated connection acquired from a pool,
then directly copying the response values to the given `io.Writer`, and finally recycling the connection.

```go
s := client.DoMultiStream(ctx, client.B().Get().Key("a{slot1}").Build(), client.B().Get().Key("b{slot1}").Build())
for s.HasNext() {
    n, err := s.WriteTo(io.Discard)
    if valkey.IsValkeyNil(err) {
        // ...
    }
}
```

Note that these two methods will occupy connections until all responses are written to the given `io.Writer`.
This can take a long time and hurt performance. Use the normal `Do()` and `DoMulti()` instead unless you want to avoid allocating memory for a large valkey response.

Also note that these two methods only work with `string`, `integer`, and `float` valkey responses. And `DoMultiStream` currently
does not support pipelining keys across multiple slots when connecting to a valkey cluster.

## Memory Consumption Consideration

Each underlying connection in valkey allocates a ring buffer for pipelining.
Its size is controlled by the `ClientOption.RingScaleEachConn` and the default value is 10 which results into each ring of size 2^10.

If you have many valkey connections, you may find that they occupy quite an amount of memory.
In that case, you may consider reducing `ClientOption.RingScaleEachConn` to 8 or 9 at the cost of potential throughput degradation.

You may also consider setting the value of `ClientOption.PipelineMultiplex` to `-1`, which will let valkey use only 1 connection for pipelining to each valkey node.

## Instantiating a new Valkey Client

You can create a new valkey client using `NewClient` and provide several options.

```golang
// Connect to a single valkey node:
client, err := valkey.NewClient(valkey.ClientOption{
    InitAddress: []string{"127.0.0.1:6379"},
})

// Connect to a standalone valkey with replicas
client, err := valkey.NewClient(valkey.ClientOption{
    InitAddress: []string{"127.0.0.1:6379"},
    Standalone: valkey.StandaloneOption{
        // Note that these addresses must be online and cannot be promoted.
        // An example use case is the reader endpoint provided by cloud vendors.
        ReplicaAddress: []string{"reader_endpoint:port"},
    },
    SendToReplicas: func(cmd valkey.Completed) bool {
        return cmd.IsReadOnly()
    },
})

// Connect to a valkey cluster
client, err := valkey.NewClient(valkey.ClientOption{
    InitAddress: []string{"127.0.0.1:7001", "127.0.0.1:7002", "127.0.0.1:7003"},
    ShuffleInit: true,
})

// Connect to a valkey cluster and use replicas for read operations
client, err := valkey.NewClient(valkey.ClientOption{
    InitAddress: []string{"127.0.0.1:7001", "127.0.0.1:7002", "127.0.0.1:7003"},
    SendToReplicas: func(cmd valkey.Completed) bool {
        return cmd.IsReadOnly()
    },
})

// Connect to sentinels
client, err := valkey.NewClient(valkey.ClientOption{
    InitAddress: []string{"127.0.0.1:26379", "127.0.0.1:26380", "127.0.0.1:26381"},
    Sentinel: valkey.SentinelOption{
        MasterSet: "my_master",
    },
})
```

### Valkey URL

You can use `ParseURL` or `MustParseURL` to construct a `ClientOption`.

The provided URL must be started with either `redis://`, `rediss://` or `unix://`.

Currently supported url parameters are `db`, `dial_timeout`, `write_timeout`, `addr`, `protocol`, `client_cache`, `client_name`, `max_retries`, and `master_set`.

```go
// connect to a valkey cluster
client, err = valkey.NewClient(valkey.MustParseURL("redis://127.0.0.1:7001?addr=127.0.0.1:7002&addr=127.0.0.1:7003"))
// connect to a valkey node
client, err = valkey.NewClient(valkey.MustParseURL("redis://127.0.0.1:6379/0"))
// connect to a valkey sentinel
client, err = valkey.NewClient(valkey.MustParseURL("redis://127.0.0.1:26379/0?master_set=my_master"))
```

### Availability Zone Affinity Routing

Starting from Valkey 8.1, Valkey server provides the `availability-zone` information for clients to know where the server is located.
For using this information to route requests to the replica located in the same availability zone,
set the `EnableReplicaAZInfo` option and your `ReplicaSelector` function. For example:

```go
client, err := valkey.NewClient(valkey.ClientOption{
	InitAddress:         []string{"address.example.com:6379"},
	EnableReplicaAZInfo: true,
	SendToReplicas: func(cmd valkey.Completed) bool {
		return cmd.IsReadOnly()
	},
	ReplicaSelector: func(slot uint16, replicas []valkey.ReplicaInfo) int {
		for i, replica := range replicas {
			if replica.AZ == "us-east-1a" {
				return i // return the index of the replica.
			}
		}
		return -1 // send to the primary.
	},
})
```

## Arbitrary Command

If you want to construct commands that are absent from the command builder, you can use `client.B().Arbitrary()`:

```golang
// This will result in [ANY CMD k1 k2 a1 a2]
client.B().Arbitrary("ANY", "CMD").Keys("k1", "k2").Args("a1", "a2").Build()
```

## Working with JSON, Raw `[]byte`, and Vector Similarity Search

The command builder treats all the parameters as Valkey strings, which are binary safe. This means that users can store `[]byte`
directly into Valkey without conversion. And the `valkey.BinaryString` helper can convert `[]byte` to `string` without copying. For example:

```golang
client.B().Set().Key("b").Value(valkey.BinaryString([]byte{...})).Build()
```

Treating all the parameters as Valkey strings also means that the command builder doesn't do any quoting, conversion automatically for users.

When working with RedisJSON, users frequently need to prepare JSON strings in Valkey strings. And `valkey.JSON` can help:

```golang
client.B().JsonSet().Key("j").Path("$.myStrField").Value(valkey.JSON("str")).Build()
// equivalent to
client.B().JsonSet().Key("j").Path("$.myStrField").Value(`"str"`).Build()
```

When working with vector similarity search, users can use `valkey.VectorString32` and `valkey.VectorString64` to build queries:

```golang
cmd := client.B().FtSearch().Index("idx").Query("*=>[KNN 5 @vec $V]").
    Params().Nargs(2).NameValue().NameValue("V", valkey.VectorString64([]float64{...})).
    Dialect(2).Build()
n, resp, err := client.Do(ctx, cmd).AsFtSearch()
```

## Command Response Cheatsheet

While the command builder is developer-friendly, the response parser is a little unfriendly. Developers must know what type of Valkey response will be returned from the server beforehand and which parser they should use.

Error Handling:
If an incorrect parser function is chosen, an errParse will be returned. Here's an example using ToArray which demonstrates this scenario:

```golang
// Attempt to parse the response. If a parsing error occurs, check if the error is a parse error and handle it.
// Normally, you should fix the code by choosing the correct parser function.
// For instance, use ToString() if the expected response is a string, or ToArray() if the expected response is an array as follows:
if err := client.Do(ctx, client.B().Get().Key("k").Build()).ToArray(); IsParseErr(err) {
    fmt.Println("Parsing error:", err)
}
```

It is hard to remember what type of message will be returned and which parsing to use. So, here are some common examples:

```golang
// GET
client.Do(ctx, client.B().Get().Key("k").Build()).ToString()
client.Do(ctx, client.B().Get().Key("k").Build()).AsInt64()
// MGET
client.Do(ctx, client.B().Mget().Key("k1", "k2").Build()).ToArray()
// SET
client.Do(ctx, client.B().Set().Key("k").Value("v").Build()).Error()
// INCR
client.Do(ctx, client.B().Incr().Key("k").Build()).AsInt64()
// HGET
client.Do(ctx, client.B().Hget().Key("k").Field("f").Build()).ToString()
// HMGET
client.Do(ctx, client.B().Hmget().Key("h").Field("a", "b").Build()).ToArray()
// HGETALL
client.Do(ctx, client.B().Hgetall().Key("h").Build()).AsStrMap()
// EXPIRE
client.Do(ctx, client.B().Expire().Key("k").Seconds(1).Build()).AsInt64()
// HEXPIRE
client.Do(ctx, client.B().Hexpire().Key("h").Seconds(1).Fields().Numfields(2).Field("f1", "f2").Build()).AsIntSlice()
// ZRANGE
client.Do(ctx, client.B().Zrange().Key("k").Min("1").Max("2").Build()).AsStrSlice()
// ZRANK
client.Do(ctx, client.B().Zrank().Key("k").Member("m").Build()).AsInt64()
// ZSCORE
client.Do(ctx, client.B().Zscore().Key("k").Member("m").Build()).AsFloat64()
// ZRANGE
client.Do(ctx, client.B().Zrange().Key("k").Min("0").Max("-1").Build()).AsStrSlice()
client.Do(ctx, client.B().Zrange().Key("k").Min("0").Max("-1").Withscores().Build()).AsZScores()
// ZPOPMIN
client.Do(ctx, client.B().Zpopmin().Key("k").Build()).AsZScore()
client.Do(ctx, client.B().Zpopmin().Key("myzset").Count(2).Build()).AsZScores()
// SCARD
client.Do(ctx, client.B().Scard().Key("k").Build()).AsInt64()
// SMEMBERS
client.Do(ctx, client.B().Smembers().Key("k").Build()).AsStrSlice()
// LINDEX
client.Do(ctx, client.B().Lindex().Key("k").Index(0).Build()).ToString()
// LPOP
client.Do(ctx, client.B().Lpop().Key("k").Build()).ToString()
client.Do(ctx, client.B().Lpop().Key("k").Count(2).Build()).AsStrSlice()
// SCAN
client.Do(ctx, client.B().Scan().Cursor(0).Build()).AsScanEntry()
// FT.SEARCH
client.Do(ctx, client.B().FtSearch().Index("idx").Query("@f:v").Build()).AsFtSearch()
// GEOSEARCH
client.Do(ctx, client.B().Geosearch().Key("k").Fromlonlat(1, 1).Bybox(1).Height(1).Km().Build()).AsGeosearch()
```

## Use DecodeSliceOfJSON to Scan Array Result

DecodeSliceOfJSON is useful when you would like to scan the results of an array into a slice of a specific struct.

```golang
type User struct {
	Name string `json:"name"`
}

// Set some values
if err = client.Do(ctx, client.B().Set().Key("user1").Value(`{"name": "name1"}`).Build()).Error(); err != nil {
	return err
}
if err = client.Do(ctx, client.B().Set().Key("user2").Value(`{"name": "name2"}`).Build()).Error(); err != nil {
	return err
}

// Scan MGET results into []*User
var users []*User // or []User is also scannable
if err := valkey.DecodeSliceOfJSON(client.Do(ctx, client.B().Mget().Key("user1", "user2").Build()), &users); err != nil {
	return err
}

for _, user := range users {
	fmt.Printf("%+v\n", user)
}
/*
&{name:name1}
&{name:name2}
*/
```

### !!!!!! DO NOT DO THIS !!!!!!

Please make sure that all values in the result have the same JSON structures.

```golang
// Set a pure string value
if err = client.Do(ctx, client.B().Set().Key("user1").Value("userName1").Build()).Error(); err != nil {
	return err
}

// Bad
users := make([]*User, 0)
if err := valkey.DecodeSliceOfJSON(client.Do(ctx, client.B().Mget().Key("user1").Build()), &users); err != nil {
	return err
}
// -> Error: invalid character 'u' looking for the beginning of the value
// in this case, use client.Do(ctx, client.B().Mget().Key("user1").Build()).AsStrSlice()
```

---

## Contributing

Contributions are welcome, including [issues](https://github.com/valkey-io/valkey-go/issues), [pull requests](https://github.com/valkey-io/valkey-go/pulls), and [discussions](https://github.com/valkey-io/valkey-go/discussions).
Contributions mean a lot to us and help us improve this library and the community!

Thanks to all the people who already contributed!

<a href="https://github.com/valkey-io/valkey-go/graphs/contributors">
  <img src="https://contributors-img.web.app/image?repo=valkey-io/valkey-go" />
</a>

### Generate Command Builders

Command builders are generated based on the definitions in [./hack/cmds](./hack/cmds) by running:

```sh
go generate
```

### Testing

Please use the [./dockertest.sh](./dockertest.sh) script for running test cases locally.
And please try your best to have 100% test coverage on code changes.
