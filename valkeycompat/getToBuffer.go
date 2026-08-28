package valkeycompat

import "io"

type bufferWriter struct {
	buf []byte
	pos int
}

func (w *bufferWriter) Write(p []byte) (int, error) {
	remaining := len(w.buf) - w.pos

	if remaining == 0 {
		return 0, io.ErrShortBuffer
	}

	n := copy(w.buf[w.pos:], p)
	w.pos += n

	if n < len(p) {
		return n, io.ErrShortBuffer
	}

	return n, nil
}

type ZeroCopyStringCmd struct {
	baseCmd[int]
	buf []byte
}

func (cmd *ZeroCopyStringCmd) Bytes() []byte {
	n := cmd.Val()

	if n <= 0 {
		return cmd.buf[:0]
	}

	if n > len(cmd.buf) {
		n = len(cmd.buf)
	}

	return cmd.buf[:n]
}
