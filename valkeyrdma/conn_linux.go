package valkeyrdma

/*
#cgo LDFLAGS: -libverbs -lrdmacm
#include <errno.h>
#include <stdlib.h>
#include "conn_linux.h"
int connectRdma(RdmaContext *ctx, const char *addr, int port, long timeout_msec);
void closeRdma(RdmaContext *ctx);
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

	if ret := C.connectRdma(c.ctx, chost, C.int(port), C.long(timeout)); ret != 0 {
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
	//TODO implement me
	panic("implement me")
}

func (c *conn) Write(b []byte) (n int, err error) {
	//TODO implement me
	panic("implement me")
}

func (c *conn) Close() error {
	if c.close.CompareAndSwap(false, true) {
		C.closeRdma(c.ctx)
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
