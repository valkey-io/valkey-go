package valkeyrdma

/*
#cgo LDFLAGS: -libverbs -lrdmacm
#include <errno.h>
#include <stdlib.h>
#include "conn_linux.h"
int rdmaConnect(RdmaContext *ctx, const char *addr, int port, long timeout_msec);
ssize_t rdmaRead(RdmaContext *ctx, char *buf, size_t bufcap, long timeout_msec);
ssize_t rdmaWrite(RdmaContext *ctx, const char *obuf, size_t data_len, long timeout_msec);
void rdmaClose(RdmaContext *ctx);
void rdmaDisconnect(RdmaContext *ctx);
*/
import "C"

import (
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

var _ net.Conn = (*conn)(nil)

func DialContext(ctx context.Context, dst string) (net.Conn, error) {
	host, portstr, err := net.SplitHostPort(dst)
	if err != nil {
		return nil, err
	}
	port, err := strconv.Atoi(portstr)
	if err != nil {
		return nil, err
	}
	c := &conn{
		ctx:   (*C.RdmaContext)(C.malloc(C.sizeof_struct_RdmaContext)),
		timed: -1,
	}
	chost := C.CString(host)
	defer C.free(unsafe.Pointer(chost))

	timeout := 10000
	if dl, ok := ctx.Deadline(); ok {
		timeout = int(time.Since(dl).Milliseconds())
	}

	if ret := C.rdmaConnect(c.ctx, chost, C.int(port), C.long(timeout)); ret != 0 {
		err = fmt.Errorf("%s: %d", C.GoString(&c.ctx.errstr[0]), int(c.ctx.err))
		C.free(unsafe.Pointer(c.ctx))
		return nil, err
	}
	return c, nil
}

type conn struct {
	ctx   *C.RdmaContext
	mu    sync.RWMutex
	timed int64
	once  int32
}

func (c *conn) Read(b []byte) (n int, err error) {
	if len(b) == 0 {
		return 0, nil
	}
	c.mu.RLock()
	if c.ctx == nil {
		c.mu.RUnlock()
		return 0, io.ErrClosedPipe
	}
	var ret C.ssize_t
	var timed = c.timed
	if timed < 0 {
		timed = 100000
	}
	ret = C.rdmaRead(c.ctx, (*C.char)(unsafe.Pointer(&b[0])), C.size_t(len(b)), C.long(timed))
	c.mu.RUnlock()

	if ret < 0 {
		return 0, c.err()
	}
	return int(ret), nil
}

func (c *conn) Write(b []byte) (n int, err error) {
	if len(b) == 0 {
		return 0, nil
	}
	c.mu.RLock()
	if c.ctx == nil {
		c.mu.RUnlock()
		return 0, io.ErrClosedPipe
	}
	var ret C.ssize_t
	var timed = c.timed
	if timed < 0 {
		timed = 100000
	}
	ret = C.rdmaWrite(c.ctx, (*C.char)(unsafe.Pointer(&b[0])), C.size_t(len(b)), C.long(timed))
	c.mu.RUnlock()

	if ret < 0 {
		return 0, c.err()
	}
	return int(ret), nil
}

func (c *conn) Close() error {
	if atomic.CompareAndSwapInt32(&c.once, 0, 1) {
		C.rdmaDisconnect(c.ctx)
	}
	c.mu.Lock()
	if c.ctx != nil {
		C.rdmaClose(c.ctx)
		C.free(unsafe.Pointer(c.ctx))
		c.ctx = nil
	}
	c.mu.Unlock()
	return nil
}

func (c *conn) err() (err error) {
	c.mu.Lock()
	if c.ctx == nil {
		err = io.ErrClosedPipe
	} else {
		err = fmt.Errorf("%s: %d", C.GoString(&c.ctx.errstr[0]), int(c.ctx.err))
	}
	c.mu.Unlock()
	return err
}

func (c *conn) LocalAddr() net.Addr {
	panic("not implemented")
}

func (c *conn) RemoteAddr() net.Addr {
	panic("not implemented")
}

func (c *conn) SetDeadline(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if t.IsZero() {
		c.timed = -1
	} else {
		c.timed = time.Until(t).Milliseconds()
	}
	return nil
}

func (c *conn) SetReadDeadline(t time.Time) error {
	panic("not implemented")
}

func (c *conn) SetWriteDeadline(t time.Time) error {
	panic("not implemented")
}
