package mock

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/valkey-io/valkey-go"
)

func Result(val valkey.ValkeyMessage) valkey.ValkeyResult {
	r := result{val: val}
	return *(*valkey.ValkeyResult)(unsafe.Pointer(&r))
}

func ErrorResult(err error) valkey.ValkeyResult {
	r := result{err: err}
	return *(*valkey.ValkeyResult)(unsafe.Pointer(&r))
}

func ValkeyString(v string) valkey.ValkeyMessage {
	m := message{typ: '+', string: v}
	return *(*valkey.ValkeyMessage)(unsafe.Pointer(&m))
}

func ValkeyBlobString(v string) valkey.ValkeyMessage {
	m := message{typ: '$', string: v}
	return *(*valkey.ValkeyMessage)(unsafe.Pointer(&m))
}

func ValkeyError(v string) valkey.ValkeyMessage {
	m := message{typ: '-', string: v}
	return *(*valkey.ValkeyMessage)(unsafe.Pointer(&m))
}

func ValkeyInt64(v int64) valkey.ValkeyMessage {
	m := message{typ: ':', integer: v}
	return *(*valkey.ValkeyMessage)(unsafe.Pointer(&m))
}

func ValkeyFloat64(v float64) valkey.ValkeyMessage {
	m := message{typ: ',', string: strconv.FormatFloat(v, 'f', -1, 64)}
	return *(*valkey.ValkeyMessage)(unsafe.Pointer(&m))
}

func ValkeyBool(v bool) valkey.ValkeyMessage {
	m := message{typ: '#'}
	if v {
		m.integer = 1
	}
	return *(*valkey.ValkeyMessage)(unsafe.Pointer(&m))
}

func ValkeyNil() valkey.ValkeyMessage {
	m := message{typ: '_'}
	return *(*valkey.ValkeyMessage)(unsafe.Pointer(&m))
}

func ValkeyArray(values ...valkey.ValkeyMessage) valkey.ValkeyMessage {
	m := message{typ: '*', values: values}
	return *(*valkey.ValkeyMessage)(unsafe.Pointer(&m))
}

func ValkeyMap(kv map[string]valkey.ValkeyMessage) valkey.ValkeyMessage {
	values := make([]valkey.ValkeyMessage, 0, 2*len(kv))
	for k, v := range kv {
		values = append(values, ValkeyString(k))
		values = append(values, v)
	}
	m := message{typ: '%', values: values}
	return *(*valkey.ValkeyMessage)(unsafe.Pointer(&m))
}

func serialize(m message, buf *bytes.Buffer) {
	switch m.typ {
	case '$', '!', '=':
		buf.WriteString(fmt.Sprintf("%s%d\r\n%s\r\n", string(m.typ), len(m.string), m.string))
	case '+', '-', ',', '(':
		buf.WriteString(fmt.Sprintf("%s%s\r\n", string(m.typ), m.string))
	case ':', '#':
		buf.WriteString(fmt.Sprintf("%s%d\r\n", string(m.typ), m.integer))
	case '_':
		buf.WriteString(fmt.Sprintf("%s\r\n", string(m.typ)))
	case '*':
		buf.WriteString(fmt.Sprintf("%s%d\r\n", string(m.typ), len(m.values)))
		for _, v := range m.values {
			pv := *(*message)(unsafe.Pointer(&v))
			serialize(pv, buf)
		}
	case '%':
		buf.WriteString(fmt.Sprintf("%s%d\r\n", string(m.typ), len(m.values)/2))
		for _, v := range m.values {
			pv := *(*message)(unsafe.Pointer(&v))
			serialize(pv, buf)
		}
	}
}

func ValkeyResultStreamError(err error) valkey.ValkeyResultStream {
	s := stream{e: err}
	return *(*valkey.ValkeyResultStream)(unsafe.Pointer(&s))
}

func ValkeyResultStream(ms ...valkey.ValkeyMessage) valkey.ValkeyResultStream {
	buf := bytes.NewBuffer(nil)
	for _, m := range ms {
		pm := *(*message)(unsafe.Pointer(&m))
		serialize(pm, buf)
	}
	s := stream{n: len(ms), p: &pool{size: 1, cond: sync.NewCond(&sync.Mutex{})}, w: &pipe{r: bufio.NewReader(buf)}}
	return *(*valkey.ValkeyResultStream)(unsafe.Pointer(&s))
}

func MultiValkeyResultStream(ms ...valkey.ValkeyMessage) valkey.MultiValkeyResultStream {
	return ValkeyResultStream(ms...)
}

func MultiValkeyResultStreamError(err error) valkey.ValkeyResultStream {
	return ValkeyResultStreamError(err)
}

type message struct {
	attrs   *valkey.ValkeyMessage
	string  string
	values  []valkey.ValkeyMessage
	integer int64
	typ     byte
	ttl     [7]byte
}

type result struct {
	err error
	val valkey.ValkeyMessage
}

type pool struct {
	dead    any
	cond    *sync.Cond
	timer   *time.Timer
	make    func() any
	list    []any
	cleanup time.Duration
	size    int
	minSize int
	cap     int
	down    bool
	timerOn bool
}

type pipe struct {
	conn            net.Conn
	error           atomic.Pointer[errs]
	clhks           atomic.Value // closed hook, invoked after the conn is closed
	pshks           atomic.Value // pubsub hook, registered by the SetPubSubHooks
	queue           any
	cache           valkey.CacheStore
	r               *bufio.Reader
	w               *bufio.Writer
	close           chan struct{}
	onInvalidations func([]valkey.ValkeyMessage)
	r2psFn          func() (p *pipe, err error) // func to build pipe for resp2 pubsub
	r2pipe          *pipe                       // internal pipe for resp2 pubsub only
	ssubs           *any                        // pubsub smessage subscriptions
	nsubs           *any                        // pubsub  message subscriptions
	psubs           *any                        // pubsub pmessage subscriptions
	pingTimer       *time.Timer
	info            map[string]valkey.ValkeyMessage
	timeout         time.Duration
	pinggap         time.Duration
	maxFlushDelay   time.Duration
	r2mu            sync.Mutex
	version         int32
	_               [10]int32
	blcksig         int32
	state           int32
	waits           int32
	recvs           int32
	bgState         int32
	r2ps            bool // identify this pipe is used for resp2 pubsub or not
	noNoDelay       bool
	optIn           bool
}

type stream struct {
	p *pool
	w *pipe
	e error
	n int
}

type errs struct{ error }
