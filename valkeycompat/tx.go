package valkeycompat

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/valkey-io/valkey-go"
)

var TxFailedErr = errors.New("valkey: transaction failed")
// ErrCrossSlot is returned when a pipeline or transaction contains keys that hash to different cluster slots.
var ErrCrossSlot = errors.New("valkey: CROSSSLOT Keys in request don't hash to the same slot")

var _ Pipeliner = (*TxPipeline)(nil)

type rePipeline = Pipeline

func newTxPipeline(real valkey.Client) *TxPipeline {
	return &TxPipeline{rePipeline: newPipeline(real)}
}

type TxPipeline struct {
	*rePipeline
}

func (c *TxPipeline) Exec(ctx context.Context) ([]Cmder, error) {
	p := c.comp.client.(*proxy)
	if len(p.cmds) == 0 {
		return nil, nil
	}

	expectedSlot := -1
	for _, cmd := range p.cmds {
		args := cmd.Commands()
		if len(args) < 2 {
			continue
		}

		cmdName := strings.ToUpper(args[0])
		var keys []string

		switch cmdName {
		case "EVAL", "EVALSHA", "FCALL", "EVAL_RO", "EVALSHA_RO", "FCALL_RO":
			if len(args) >= 3 {
				numKeys, _ := strconv.Atoi(args[2])
				if len(args) >= 3+numKeys {
					keys = args[3 : 3+numKeys]
				}
			}
		case "MGET", "DEL", "UNLINK", "EXISTS", "TOUCH":
			keys = args[1:]
		case "MSET", "MSETNX":
			for i := 1; i < len(args); i += 2 {
				keys = append(keys, args[i])
			}
		default:
			keys = []string{args[1]}
		}

		for _, key := range keys {
			s := int(slot(key))
			if expectedSlot == -1 {
				expectedSlot = s
			} else if expectedSlot != s {
				c.rets = nil
				p.cmds = nil
				return nil, ErrCrossSlot
			}
		}
	}

	rets := c.rets
	cmds := p.cmds
	c.rets = nil
	p.cmds = nil

	cmds = append(cmds, c.comp.client.B().Multi().Build(), c.comp.client.B().Exec().Build())
	for i := len(cmds) - 2; i >= 1; i-- {
		j := i - 1
		cmds[j], cmds[i] = cmds[i], cmds[j]
	}

	resp := p.DoMulti(ctx, cmds...)
	results, err := resp[len(resp)-1].ToArray()
	if valkey.IsValkeyNil(err) {
		err = TxFailedErr
	}
	for i, r := range results {
		rets[i].SetErr(nil)
		rets[i].from(valkey.NewResult(r, resp[i+1].NonValkeyError()))
		if err == nil {
			err = rets[i].Err()
		}
	}
	return rets, err
}
func (c *TxPipeline) Pipelined(ctx context.Context, fn func(Pipeliner) error) ([]Cmder, error) {
	if err := fn(c); err != nil {
		return nil, err
	}
	return c.Exec(ctx)
}

func (c *TxPipeline) Pipeline() Pipeliner {
	return c
}

func (c *TxPipeline) TxPipelined(ctx context.Context, fn func(Pipeliner) error) ([]Cmder, error) {
	return c.Pipelined(ctx, fn)
}

func (c *TxPipeline) TxPipeline() Pipeliner {
	return c
}

var _ valkey.Client = (*txproxy)(nil)

type txproxy struct {
	valkey.CoreClient
}

func (p *txproxy) DoCache(_ context.Context, _ valkey.Cacheable, _ time.Duration) (resp valkey.ValkeyResult) {
	panic("not implemented")
}

func (p *txproxy) DoMultiCache(_ context.Context, _ ...valkey.CacheableTTL) (resp []valkey.ValkeyResult) {
	panic("not implemented")
}

func (p *txproxy) DoStream(_ context.Context, _ valkey.Completed) valkey.ValkeyResultStream {
	panic("not implemented")
}

func (p *txproxy) DoMultiStream(_ context.Context, _ ...valkey.Completed) valkey.MultiValkeyResultStream {
	panic("not implemented")
}

func (p *txproxy) Dedicated(_ func(valkey.DedicatedClient) error) (err error) {
	panic("not implemented")
}

func (p *txproxy) Dedicate() (client valkey.DedicatedClient, cancel func()) {
	panic("not implemented")
}

func (p *txproxy) Nodes() map[string]valkey.Client {
	panic("not implemented")
}

func (p *txproxy) Mode() valkey.ClientMode {
	panic("not implemented")
}

type Tx interface {
	CoreCmdable
	Watch(ctx context.Context, keys ...string) *StatusCmd
	Unwatch(ctx context.Context, keys ...string) *StatusCmd
	Close(ctx context.Context) error
}

func newTx(client valkey.DedicatedClient, cancel func()) *tx {
	return &tx{CoreCmdable: NewAdapter(&txproxy{CoreClient: client}), cancel: cancel}
}

type tx struct {
	CoreCmdable
	cancel func()
}

func (t *tx) Watch(ctx context.Context, keys ...string) *StatusCmd {
	ret := &StatusCmd{}
	if len(keys) != 0 {
		client := t.CoreCmdable.(*Compat).client
		ret.from(client.Do(ctx, client.B().Watch().Key(keys...).Build()))
	}
	return ret
}

func (t *tx) Unwatch(ctx context.Context, _ ...string) *StatusCmd {
	ret := &StatusCmd{}
	client := t.CoreCmdable.(*Compat).client
	ret.from(client.Do(ctx, client.B().Unwatch().Build()))
	return ret
}

func (t *tx) Close(_ context.Context) error {
	t.cancel()
	return nil
}

func (c *TxPipeline) GetToBuffer(ctx context.Context, key string, buf []byte) *ZeroCopyStringCmd {
	panic("GetToBuffer is not supported in TxPipeline")
}

func (c *TxPipeline) SetFromBuffer(ctx context.Context, key string, buf []byte) *StatusCmd {
	return c.rePipeline.SetFromBuffer(ctx, key, buf)
}
// slot computes the CRC16 hash slot for a given key, honoring hash-tags ({...}).
func slot(key string) uint16 {
	var s, e int
	for ; s < len(key); s++ {
		if key[s] == '{' {
			break
		}
	}
	if s == len(key) {
		return crc16(key) & 16383
	}
	for e = s + 1; e < len(key); e++ {
		if key[e] == '}' {
			break
		}
	}
	if e == len(key) || e == s+1 {
		return crc16(key) & 16383
	}
	return crc16(key[s+1:e]) & 16383
}

/*
 * Copyright 2001-2010 Georges Menie (www.menie.org)
 * Copyright 2010 Salvatore Sanfilippo (adapted to Valkey coding style)
 * All rights reserved.
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the University of California, Berkeley nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE REGENTS AND CONTRIBUTORS ``AS IS'' AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE REGENTS AND CONTRIBUTORS BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

/* CRC16 implementation according to CCITT standards.
 *
 * Note by @antirez: this is actually the XMODEM CRC 16 algorithm, using the
 * following parameters:
 *
 * Name                       : "XMODEM", also known as "ZMODEM", "CRC-16/ACORN"
 * Width                      : 16 bit
 * Poly                       : 1021 (That is actually x^16 + x^12 + x^5 + 1)
 * Initialization             : 0000
 * Reflect Input byte         : False
 * Reflect Output CRC         : False
 * Xor constant to output CRC : 0000
 * Output for "123456789"     : 31C3
 */

var crc16tab = [256]uint16{
	0x0000, 0x1021, 0x2042, 0x3063, 0x4084, 0x50a5, 0x60c6, 0x70e7,
	0x8108, 0x9129, 0xa14a, 0xb16b, 0xc18c, 0xd1ad, 0xe1ce, 0xf1ef,
	0x1231, 0x0210, 0x3273, 0x2252, 0x52b5, 0x4294, 0x72f7, 0x62d6,
	0x9339, 0x8318, 0xb37b, 0xa35a, 0xd3bd, 0xc39c, 0xf3ff, 0xe3de,
	0x2462, 0x3443, 0x0420, 0x1401, 0x64e6, 0x74c7, 0x44a4, 0x5485,
	0xa56a, 0xb54b, 0x8528, 0x9509, 0xe5ee, 0xf5cf, 0xc5ac, 0xd58d,
	0x3653, 0x2672, 0x1611, 0x0630, 0x76d7, 0x66f6, 0x5695, 0x46b4,
	0xb75b, 0xa77a, 0x9719, 0x8738, 0xf7df, 0xe7fe, 0xd79d, 0xc7bc,
	0x48c4, 0x58e5, 0x6886, 0x78a7, 0x0840, 0x1861, 0x2802, 0x3823,
	0xc9cc, 0xd9ed, 0xe98e, 0xf9af, 0x8948, 0x9969, 0xa90a, 0xb92b,
	0x5af5, 0x4ad4, 0x7ab7, 0x6a96, 0x1a71, 0x0a50, 0x3a33, 0x2a12,
	0xdbfd, 0xcbdc, 0xfbbf, 0xeb9e, 0x9b79, 0x8b58, 0xbb3b, 0xab1a,
	0x6ca6, 0x7c87, 0x4ce4, 0x5cc5, 0x2c22, 0x3c03, 0x0c60, 0x1c41,
	0xedae, 0xfd8f, 0xcdec, 0xddcd, 0xad2a, 0xbd0b, 0x8d68, 0x9d49,
	0x7e97, 0x6eb6, 0x5ed5, 0x4ef4, 0x3e13, 0x2e32, 0x1e51, 0x0e70,
	0xff9f, 0xefbe, 0xdfdd, 0xcffc, 0xbf1b, 0xaf3a, 0x9f59, 0x8f78,
	0x9188, 0x81a9, 0xb1ca, 0xa1eb, 0xd10c, 0xc12d, 0xf14e, 0xe16f,
	0x1080, 0x00a1, 0x30c2, 0x20e3, 0x5004, 0x4025, 0x7046, 0x6067,
	0x83b9, 0x9398, 0xa3fb, 0xb3da, 0xc33d, 0xd31c, 0xe37f, 0xf35e,
	0x02b1, 0x1290, 0x22f3, 0x32d2, 0x4235, 0x5214, 0x6277, 0x7256,
	0xb5ea, 0xa5cb, 0x95a8, 0x8589, 0xf56e, 0xe54f, 0xd52c, 0xc50d,
	0x34e2, 0x24c3, 0x14a0, 0x0481, 0x7466, 0x6447, 0x5424, 0x4405,
	0xa7db, 0xb7fa, 0x8799, 0x97b8, 0xe75f, 0xf77e, 0xc71d, 0xd73c,
	0x26d3, 0x36f2, 0x0691, 0x16b0, 0x6657, 0x7676, 0x4615, 0x5634,
	0xd94c, 0xc96d, 0xf90e, 0xe92f, 0x99c8, 0x89e9, 0xb98a, 0xa9ab,
	0x5844, 0x4865, 0x7806, 0x6827, 0x18c0, 0x08e1, 0x3882, 0x28a3,
	0xcb7d, 0xdb5c, 0xeb3f, 0xfb1e, 0x8bf9, 0x9bd8, 0xabbb, 0xbb9a,
	0x4a75, 0x5a54, 0x6a37, 0x7a16, 0x0af1, 0x1ad0, 0x2ab3, 0x3a92,
	0xfd2e, 0xed0f, 0xdd6c, 0xcd4d, 0xbdaa, 0xad8b, 0x9de8, 0x8dc9,
	0x7c26, 0x6c07, 0x5c64, 0x4c45, 0x3ca2, 0x2c83, 0x1ce0, 0x0cc1,
	0xef1f, 0xff3e, 0xcf5d, 0xdf7c, 0xaf9b, 0xbfba, 0x8fd9, 0x9ff8,
	0x6e17, 0x7e36, 0x4e55, 0x5e74, 0x2e93, 0x3eb2, 0x0ed1, 0x1ef0,
}

// crc16 implements the Valkey cluster CRC-16 hashing algorithm.
func crc16(key string) (crc uint16) {
	for i := 0; i < len(key); i++ {
		crc = (crc << 8) ^ crc16tab[(uint8(crc>>8)^key[i])&0x00FF]
	}
	return crc
}
