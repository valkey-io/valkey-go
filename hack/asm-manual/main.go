// Command asm-manual is a small manual test driver for a local Valkey 9.0
// cluster. Run it inside the Docker network so the container IP addresses
// advertised by cluster nodes remain reachable.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	valkey "github.com/valkey-io/valkey-go"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}

	addr := os.Getenv("VALKEY_ADDR")
	if addr == "" {
		addr = "valkey-7001:7001"
	}
	client, err := valkey.NewClientWithAtomicSlotMigrationAwareness(valkey.ClientOption{
		InitAddress:  []string{addr},
		DisableCache: true,
	})
	if err != nil {
		log.Fatalf("create ASM-aware cluster client: %v", err)
	}
	defer client.Close()

	switch os.Args[1] {
	case "seed":
		count := argInt(2, 30000)
		valueBytes := argInt(3, 1024)
		if count <= 0 || valueBytes <= 0 {
			log.Fatal("count and value-bytes must be positive")
		}
		if err := seed(client, count, valueBytes); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Seeded %d keys using hash tag {asm-test}; slot=%d\n", count, keySlot(client))
	case "load":
		workers := argInt(2, 4)
		if workers <= 0 {
			log.Fatal("workers must be positive")
		}
		load(client, workers)
	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: asm-manual seed [key-count] [value-bytes] | load [workers]")
	fmt.Fprintln(os.Stderr, "environment: VALKEY_ADDR (default valkey-7001:7001)")
}

func argInt(index, fallback int) int {
	if len(os.Args) <= index {
		return fallback
	}
	value, err := strconv.Atoi(os.Args[index])
	if err != nil {
		log.Fatalf("invalid integer %q: %v", os.Args[index], err)
	}
	return value
}

func seed(client valkey.Client, count, valueBytes int) error {
	const batchSize = 128
	value := strings.Repeat("x", valueBytes)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	for start := 0; start < count; start += batchSize {
		end := start + batchSize
		if end > count {
			end = count
		}
		commands := make([]valkey.Completed, 0, end-start)
		for i := start; i < end; i++ {
			key := fmt.Sprintf("{asm-test}:key:%d", i)
			commands = append(commands, client.B().Set().Key(key).Value(value).Build())
		}
		results := client.DoMulti(ctx, commands...)
		for i, result := range results {
			if err := result.Error(); err != nil {
				return fmt.Errorf("seed batch starting at %d, command %d: %w", start, i, err)
			}
		}
		if end%5000 == 0 || end == count {
			fmt.Printf("seeded %d/%d keys\n", end, count)
		}
	}
	return nil
}

func keySlot(client valkey.Client) int64 {
	result := client.Do(context.Background(), client.B().Arbitrary("CLUSTER", "KEYSLOT", "{asm-test}:key:0").Build())
	slot, err := result.ToInt64()
	if err != nil {
		log.Fatalf("CLUSTER KEYSLOT: %v", err)
	}
	return slot
}

func load(client valkey.Client, workers int) {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var operations atomic.Uint64
	var failures atomic.Uint64
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for ctx.Err() == nil {
				sequence := operations.Add(1)
				key := fmt.Sprintf("{asm-test}:key:%d", sequence%30000)
				setResult := client.Do(ctx, client.B().Set().Key(key).Value(fmt.Sprintf("value-%d", sequence)).Build())
				if err := setResult.Error(); err != nil && ctx.Err() == nil {
					failures.Add(1)
					log.Printf("worker=%d SET error: %v", workerID, err)
					continue
				}
				getResult := client.Do(ctx, client.B().Get().Key(key).Build())
				if err := getResult.Error(); err != nil && ctx.Err() == nil {
					failures.Add(1)
					log.Printf("worker=%d GET error: %v", workerID, err)
				}
			}
		}(worker)
	}

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	var lastOps, lastFailures uint64
	fmt.Printf("load started with %d workers; press Ctrl+C to stop\n", workers)
	for {
		select {
		case <-ctx.Done():
			wg.Wait()
			fmt.Printf("load stopped: operations=%d failures=%d\n", operations.Load(), failures.Load())
			return
		case <-ticker.C:
			nowOps, nowFailures := operations.Load(), failures.Load()
			fmt.Printf("ops/s=%d failures/s=%d total_ops=%d total_failures=%d\n", nowOps-lastOps, nowFailures-lastFailures, nowOps, nowFailures)
			lastOps, lastFailures = nowOps, nowFailures
		}
	}
}
