package valkeylimiter

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/valkey-io/valkey-go"
)

var (
	ErrInvalidTokens   = errors.New("number of tokens must be non-negative")
	ErrInvalidResponse = errors.New("invalid response from Valkey")
	ErrInvalidLimit    = errors.New("limit must be positive")
	ErrInvalidWindow   = errors.New("window must be positive")
	ErrNilBuilder      = errors.New("client builder is required")
)

type Result struct {
	Allowed   bool
	Remaining int64
	ResetAtMs int64
}

type RateLimiterClient interface {
	Check(ctx context.Context, identifier string, options ...RateLimitOption) (Result, error)
	Allow(ctx context.Context, identifier string, options ...RateLimitOption) (Result, error)
	AllowN(ctx context.Context, identifier string, n int64, options ...RateLimitOption) (Result, error)
	Limit() int
	Close()
}

const (
	PlaceholderPrefix = "valkeylimiter"
	keyDelimOpen      = ":{"
	keyDelimClose     = "}"
)

type rateLimiter struct {
	client           valkey.Client
	keyPrefix        string
	defaultRateLimit RateLimitOption
}

type RateLimiterOption struct {
	ClientBuilder func(option valkey.ClientOption) (valkey.Client, error)
	KeyPrefix     string
	ClientOption  valkey.ClientOption
	Limit         int
	Window        time.Duration
}

func NewRateLimiter(option RateLimiterOption) (RateLimiterClient, error) {
	if option.Window <= 0 {
		return nil, ErrInvalidWindow
	}
	if option.Limit <= 0 {
		return nil, ErrInvalidLimit
	}
	if option.KeyPrefix == "" {
		option.KeyPrefix = PlaceholderPrefix
	}

	rl := &rateLimiter{
		defaultRateLimit: RateLimitOption{
			limit:  int64(option.Limit),
			window: option.Window,
		},
	}

	var err error
	if option.ClientBuilder != nil {
		rl.client, err = option.ClientBuilder(option.ClientOption)
	} else {
		rl.client, err = valkey.NewClient(option.ClientOption)
	}
	if err != nil {
		return nil, err
	}
	rl.keyPrefix = option.KeyPrefix
	return rl, nil
}

func (l *rateLimiter) Limit() int {
	return int(l.defaultRateLimit.limit)
}

func (l *rateLimiter) Check(ctx context.Context, identifier string, options ...RateLimitOption) (Result, error) {
	return l.AllowN(ctx, identifier, 0, options...)
}

func (l *rateLimiter) Allow(ctx context.Context, identifier string, options ...RateLimitOption) (Result, error) {
	return l.AllowN(ctx, identifier, 1, options...)
}

func (l *rateLimiter) AllowN(ctx context.Context, identifier string, n int64, options ...RateLimitOption) (Result, error) {
	if n < 0 {
		return Result{}, ErrInvalidTokens
	}
	rl := l.defaultRateLimit
	if len(options) > 0 {
		rl = options[len(options)-1]
	}

	bufs := rateBuffersPool.Get(0, 128)
	defer rateBuffersPool.Put(bufs)

	now := time.Now().UTC()

	offset := len(bufs.keyBuf)
	bufs.keyBuf = append(bufs.keyBuf, l.keyPrefix...)
	bufs.keyBuf = append(bufs.keyBuf, keyDelimOpen...)
	bufs.keyBuf = append(bufs.keyBuf, identifier...)
	bufs.keyBuf = append(bufs.keyBuf, keyDelimClose...)
	key := valkey.BinaryString(bufs.keyBuf[offset:])

	offset = len(bufs.keyBuf)
	bufs.keyBuf = append(bufs.keyBuf, key...)
	bufs.keyBuf = append(bufs.keyBuf, ":ex"...)
	expiresAtKey := valkey.BinaryString(bufs.keyBuf[offset:])

	offset = len(bufs.keyBuf)
	bufs.keyBuf = strconv.AppendInt(bufs.keyBuf, n, 10)
	arg1 := valkey.BinaryString(bufs.keyBuf[offset:])

	offset = len(bufs.keyBuf)
	bufs.keyBuf = strconv.AppendInt(bufs.keyBuf, now.Add(rl.window).UnixMilli(), 10)
	arg2 := valkey.BinaryString(bufs.keyBuf[offset:])

	offset = len(bufs.keyBuf)
	bufs.keyBuf = strconv.AppendInt(bufs.keyBuf, now.UnixMilli(), 10)
	arg3 := valkey.BinaryString(bufs.keyBuf[offset:])

	resp := rateLimitScript.Exec(ctx, l.client, []string{key, expiresAtKey}, []string{arg1, arg2, arg3})
	if err := resp.Error(); err != nil {
		return Result{}, err
	}

	arr, err := resp.ToArray()
	if err != nil || len(arr) != 2 {
		return Result{}, ErrInvalidResponse
	}

	current, err := arr[0].ToInt64()
	if err != nil {
		return Result{}, ErrInvalidResponse
	}

	resetAt, err := arr[1].ToInt64()
	if err != nil {
		return Result{}, ErrInvalidResponse
	}

	remaining := max(rl.limit-current, 0)
	allowed := current <= rl.limit && (n > 0 || current < rl.limit)

	return Result{
		Allowed:   allowed,
		Remaining: remaining,
		ResetAtMs: resetAt,
	}, nil
}

func (l *rateLimiter) Close() {
	l.client.Close()
}

var rateLimitScript = valkey.NewLuaScript(`
local rate_limit_key = KEYS[1]
local increment_amount = tonumber(ARGV[1])
local next_expires_at = tonumber(ARGV[2])
local current_time = tonumber(ARGV[3])
local expires_at_key = KEYS[2]
local expires_at = tonumber(redis.call("get", expires_at_key))
if not expires_at or expires_at < current_time then
  redis.call("set", rate_limit_key, 0, "pxat", next_expires_at + 1000)
  redis.call("set", expires_at_key, next_expires_at, "pxat", next_expires_at + 1000)
  expires_at = next_expires_at
end
local current = redis.call("incrby", rate_limit_key, increment_amount)
return { current, expires_at }
`)

// GCRAResult contains the raw numeric and timing results returned by the GCRA Lua engine.
type GCRAResult struct {
	Allowed    int64
	Remaining  int64
	RetryAfter time.Duration
	ResetAfter time.Duration
}

// DurFromSecString parses a float-in-seconds string (e.g., "-1", "0.1", "1.5") into a time.Duration.
// A value of -1 returns time.Duration(-1), indicating no retry wait is needed.
func DurFromSecString(s string) (time.Duration, error) {
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, err
	}
	if f == -1 {
		return -1, nil
	}
	if f < 0 {
		return 0, nil
	}
	return time.Duration(f * float64(time.Second)), nil
}

// ParseGCRAResponse parses the 4-element array response from the GCRA Lua scripts.
// The array format is:
// [0] allowed (int64): count of allowed tokens (0 if rejected, or cost if allowed)
// [1] remaining (int64): maximum permitted instantaneous tokens
// [2] retry_after (string): float seconds until next request permitted ("-1" if allowed)
// [3] reset_after (string): float seconds until full rate limit restoration
func ParseGCRAResponse(resp valkey.ValkeyResult) (GCRAResult, error) {
	if err := resp.Error(); err != nil {
		return GCRAResult{}, err
	}

	arr, err := resp.ToArray()
	if err != nil || len(arr) != 4 {
		return GCRAResult{}, ErrInvalidResponse
	}

	allowed, err := arr[0].AsInt64()
	if err != nil {
		return GCRAResult{}, fmt.Errorf("%w: invalid allowed count: %v", ErrInvalidResponse, err)
	}

	remaining, err := arr[1].AsInt64()
	if err != nil {
		return GCRAResult{}, fmt.Errorf("%w: invalid remaining count: %v", ErrInvalidResponse, err)
	}

	retryStr, err := arr[2].ToString()
	if err != nil {
		return GCRAResult{}, fmt.Errorf("%w: invalid retry_after: %v", ErrInvalidResponse, err)
	}
	retryAfter, err := DurFromSecString(retryStr)
	if err != nil {
		return GCRAResult{}, fmt.Errorf("%w: failed to parse retry_after %q: %v", ErrInvalidResponse, retryStr, err)
	}

	resetStr, err := arr[3].ToString()
	if err != nil {
		return GCRAResult{}, fmt.Errorf("%w: invalid reset_after: %v", ErrInvalidResponse, err)
	}
	resetAfter, err := DurFromSecString(resetStr)
	if err != nil {
		return GCRAResult{}, fmt.Errorf("%w: failed to parse reset_after %q: %v", ErrInvalidResponse, resetStr, err)
	}

	return GCRAResult{
		Allowed:    allowed,
		Remaining:  remaining,
		RetryAfter: retryAfter,
		ResetAfter: resetAfter,
	}, nil
}

func ExecGCRAAllowN(ctx context.Context, client valkey.Client, key string, burst int64, rate int64, period time.Duration, cost int64) (GCRAResult, error) {
	resp := gcraAllowNScript.Exec(ctx, client, []string{key}, []string{
		strconv.FormatInt(burst, 10),
		strconv.FormatInt(rate, 10),
		strconv.FormatFloat(period.Seconds(), 'f', -1, 64),
		strconv.FormatInt(cost, 10),
	})
	return ParseGCRAResponse(resp)
}

func ExecGCRAAllowAtMost(ctx context.Context, client valkey.Client, key string, burst int64, rate int64, period time.Duration, cost int64) (GCRAResult, error) {
	resp := gcraAllowAtMostScript.Exec(ctx, client, []string{key}, []string{
		strconv.FormatInt(burst, 10),
		strconv.FormatInt(rate, 10),
		strconv.FormatFloat(period.Seconds(), 'f', -1, 64),
		strconv.FormatInt(cost, 10),
	})
	return ParseGCRAResponse(resp)
}

// gcraAllowNScript implements the atomic GCRA allowN (all-or-nothing) algorithm in Lua.
// KEYS[1]: rate limit key
// ARGV[1]: burst tolerance count
// ARGV[2]: rate (number of operations per period)
// ARGV[3]: period in seconds
// ARGV[4]: cost (tokens requested)
var gcraAllowNScript = valkey.NewLuaScript(`
redis.replicate_commands()

local rate_limit_key = KEYS[1]
local burst = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])
local period = tonumber(ARGV[3])
local cost = tonumber(ARGV[4])

local emission_interval = period / rate
local increment = emission_interval * cost
local burst_offset = emission_interval * burst

local jan_1_2017 = 1483228800
local now = redis.call("TIME")
now = (now[1] - jan_1_2017) + (now[2] / 1000000)

local tat = redis.call("GET", rate_limit_key)

if not tat then
  tat = now
else
  tat = tonumber(tat)
end

tat = math.max(tat, now)

local diff = burst_offset - increment - (tat - now)
local remaining = diff / emission_interval

if remaining < 0 then
  local reset_after = tat - now
  local retry_after = diff * -1
  return {
    0,
    0,
    tostring(retry_after),
    tostring(reset_after),
  }
end

local new_tat = tat + increment
local reset_after = new_tat - now
if reset_after > 0 then
  redis.call("SET", rate_limit_key, new_tat, "EX", math.ceil(reset_after))
end
local retry_after = -1
return {cost, math.floor(remaining + 1e-9), tostring(retry_after), tostring(reset_after)}
`)

// gcraAllowAtMostScript implements the atomic GCRA allowAtMost (partial grant) algorithm in Lua.
// KEYS[1]: rate limit key
// ARGV[1]: burst tolerance count
// ARGV[2]: rate (number of operations per period)
// ARGV[3]: period in seconds
// ARGV[4]: cost (maximum tokens requested)
var gcraAllowAtMostScript = valkey.NewLuaScript(`
redis.replicate_commands()

local rate_limit_key = KEYS[1]
local burst = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])
local period = tonumber(ARGV[3])
local cost = tonumber(ARGV[4])

local emission_interval = period / rate
local burst_offset = emission_interval * burst

local jan_1_2017 = 1483228800
local now = redis.call("TIME")
now = (now[1] - jan_1_2017) + (now[2] / 1000000)

local tat = redis.call("GET", rate_limit_key)

if not tat then
  tat = now
else
  tat = tonumber(tat)
end

tat = math.max(tat, now)

local diff = burst_offset - (tat - now)
local remaining = diff / emission_interval

if remaining < 1 then
  local reset_after = tat - now
  local retry_after = emission_interval - diff
  return {
    0,
    0,
    tostring(retry_after),
    tostring(reset_after),
  }
end

if remaining < cost then
  cost = remaining
  remaining = 0
else
  remaining = remaining - cost
end

local increment = emission_interval * cost
local new_tat = tat + increment

local reset_after = new_tat - now
if reset_after > 0 then
  redis.call("SET", rate_limit_key, new_tat, "EX", math.ceil(reset_after))
end

return {
  cost,
  math.floor(remaining + 1e-9),
  tostring(-1),
  tostring(reset_after),
}
`)

