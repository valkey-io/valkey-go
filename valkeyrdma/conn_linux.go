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
*/
import "C"

import (
	"context"
	"fmt"
	"net"
	"strconv"
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
		ctx: (*C.RdmaContext)(C.malloc(C.sizeof_struct_RdmaContext)),
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
	raddr net.Addr
	laddr net.Addr
	close atomic.Bool
}

func (c *conn) Read(b []byte) (n int, err error) {
	var ret C.ssize_t
	if len(b) > 0 {
		ret = C.rdmaRead(c.ctx, (*C.char)(unsafe.Pointer(&b[0])), C.size_t(len(b)), C.long(100000))
		if ret < 0 {
			return 0, fmt.Errorf("%s: %d", C.GoString(&c.ctx.errstr[0]), int(c.ctx.err))
		}
	}
	return int(ret), nil
}

func (c *conn) Write(b []byte) (n int, err error) {
	var ret C.ssize_t
	if len(b) > 0 {
		ret = C.rdmaWrite(c.ctx, (*C.char)(unsafe.Pointer(&b[0])), C.size_t(len(b)), C.long(100000))
		if ret < 0 {
			return 0, fmt.Errorf("%s: %d", C.GoString(&c.ctx.errstr[0]), int(c.ctx.err))
		}
	}
	return int(ret), nil
}

func (c *conn) Close() error {
	if c.close.CompareAndSwap(false, true) {
		C.rdmaClose(c.ctx)
		C.free(unsafe.Pointer(c.ctx))
	}
	return nil
}

func (c *conn) LocalAddr() net.Addr {
	return c.laddr
}

func (c *conn) RemoteAddr() net.Addr {
	return c.raddr
}

func (c *conn) SetDeadline(t time.Time) error {
	return nil
}

func (c *conn) SetReadDeadline(t time.Time) error {
	return nil
}

func (c *conn) SetWriteDeadline(t time.Time) error {
	return nil
}
