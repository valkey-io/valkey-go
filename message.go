package valkey

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/valkey-io/valkey-go/internal/util"
)

const messageStructSize = int(unsafe.Sizeof(ValkeyMessage{}))

// Nil represents a Valkey Nil message
var Nil = &ValkeyError{typ: typeNull}

// ErrParse is a parse error that occurs when a Valkey message cannot be parsed correctly.
var errParse = errors.New("valkey: parse error")

// IsValkeyNil is a handy method to check if the error is a valkey nil response.
// All valkey nil responses returned as an error.
func IsValkeyNil(err error) bool {
	return err == Nil
}

// IsParseErr checks if the error is a parse error
func IsParseErr(err error) bool {
	return errors.Is(err, errParse)
}

// IsValkeyBusyGroup checks if it is a valkey BUSYGROUP message.
func IsValkeyBusyGroup(err error) bool {
	if ret, yes := IsValkeyErr(err); yes {
		return ret.IsBusyGroup()
	}
	return false
}

// IsValkeyErr is a handy method to check if the error is a valkey ERR response.
func IsValkeyErr(err error) (ret *ValkeyError, ok bool) {
	ret, ok = err.(*ValkeyError)
	return ret, ok && ret != Nil
}

// ValkeyError is an error response or a nil message from the valkey instance
type ValkeyError ValkeyMessage

// string retrieves the contained string of the ValkeyError
func (m *ValkeyError) string() string {
	if m.bytes == nil {
		return ""
	}
	return unsafe.String(m.bytes, m.intlen)
}

func (r *ValkeyError) Error() string {
	if r.IsNil() {
		return "valkey nil message"
	}
	return r.string()
}

// IsNil checks if it is a valkey nil message.
func (r *ValkeyError) IsNil() bool {
	return r.typ == typeNull
}

// IsMoved checks if it is a valkey MOVED message and returns the moved address.
func (r *ValkeyError) IsMoved() (addr string, ok bool) {
	if ok = strings.HasPrefix(r.string(), "MOVED"); ok {
		addr = fixIPv6HostPort(strings.Split(r.string(), " ")[2])
	}
	return
}

// IsAsk checks if it is a valkey ASK message and returns ask address.
func (r *ValkeyError) IsAsk() (addr string, ok bool) {
	if ok = strings.HasPrefix(r.string(), "ASK"); ok {
		addr = fixIPv6HostPort(strings.Split(r.string(), " ")[2])
	}
	return
}

// IsRedirect checks if it is a valkey REDIRECT message and returns redirect address.
func (r *ValkeyError) IsRedirect() (addr string, ok bool) {
	if ok = strings.HasPrefix(r.string(), "REDIRECT"); ok {
		addr = fixIPv6HostPort(strings.Split(r.string(), " ")[1])
	}
	return
}

func fixIPv6HostPort(addr string) string {
	if strings.IndexByte(addr, '.') < 0 && len(addr) > 0 && addr[0] != '[' { // skip ipv4 and enclosed ipv6
		if i := strings.LastIndexByte(addr, ':'); i >= 0 {
			return net.JoinHostPort(addr[:i], addr[i+1:])
		}
	}
	return addr
}

// IsTryAgain checks if it is a valkey TRYAGAIN message and returns ask address.
func (r *ValkeyError) IsTryAgain() bool {
	return strings.HasPrefix(r.string(), "TRYAGAIN")
}

// IsLoading checks if it is a valkey LOADING message
func (r *ValkeyError) IsLoading() bool {
	return strings.HasPrefix(r.string(), "LOADING")
}

// IsClusterDown checks if it is a valkey CLUSTERDOWN message and returns ask address.
func (r *ValkeyError) IsClusterDown() bool {
	return strings.HasPrefix(r.string(), "CLUSTERDOWN")
}

// IsNoScript checks if it is a valkey NOSCRIPT message.
func (r *ValkeyError) IsNoScript() bool {
	return strings.HasPrefix(r.string(), "NOSCRIPT")
}

// IsBusyGroup checks if it is a valkey BUSYGROUP message.
func (r *ValkeyError) IsBusyGroup() bool {
	return strings.HasPrefix(r.string(), "BUSYGROUP")
}

func newResult(val ValkeyMessage, err error) ValkeyResult {
	return ValkeyResult{val: val, err: err}
}

func newErrResult(err error) ValkeyResult {
	return ValkeyResult{err: err}
}

// ValkeyResult is the return struct from Client.Do or Client.DoCache
// it contains either a valkey response or an underlying error (ex. network timeout).
type ValkeyResult struct {
	err error
	val ValkeyMessage
}

// NonValkeyError can be used to check if there is an underlying error (ex. network timeout).
func (r ValkeyResult) NonValkeyError() error {
	return r.err
}

// Error returns either underlying error or valkey error or nil
func (r ValkeyResult) Error() (err error) {
	if r.err != nil {
		err = r.err
	} else {
		err = r.val.Error()
	}
	return
}

// ToMessage retrieves the ValkeyMessage
func (r ValkeyResult) ToMessage() (v ValkeyMessage, err error) {
	if r.err != nil {
		err = r.err
	} else {
		err = r.val.Error()
	}
	return r.val, err
}

// ToInt64 delegates to ValkeyMessage.ToInt64
func (r ValkeyResult) ToInt64() (v int64, err error) {
	if r.err != nil {
		err = r.err
	} else {
		v, err = r.val.ToInt64()
	}
	return
}

// ToBool delegates to ValkeyMessage.ToBool
func (r ValkeyResult) ToBool() (v bool, err error) {
	if r.err != nil {
		err = r.err
	} else {
		v, err = r.val.ToBool()
	}
	return
}

// ToFloat64 delegates to ValkeyMessage.ToFloat64
func (r ValkeyResult) ToFloat64() (v float64, err error) {
	if r.err != nil {
		err = r.err
	} else {
		v, err = r.val.ToFloat64()
	}
	return
}

// ToString delegates to ValkeyMessage.ToString
func (r ValkeyResult) ToString() (v string, err error) {
	if r.err != nil {
		err = r.err
	} else {
		v, err = r.val.ToString()
	}
	return
}

// AsReader delegates to ValkeyMessage.AsReader
func (r ValkeyResult) AsReader() (v io.Reader, err error) {
	if r.err != nil {
		err = r.err
	} else {
		v, err = r.val.AsReader()
	}
	return
}

// AsBytes delegates to ValkeyMessage.AsBytes
func (r ValkeyResult) AsBytes() (v []byte, err error) {
	if r.err != nil {
		err = r.err
	} else {
		v, err = r.val.AsBytes()
	}
	return
}

// DecodeJSON delegates to ValkeyMessage.DecodeJSON
func (r ValkeyResult) DecodeJSON(v any) (err error) {
	if r.err != nil {
		err = r.err
	} else {
		err = r.val.DecodeJSON(v)
	}
	return
}

// AsInt64 delegates to ValkeyMessage.AsInt64
func (r ValkeyResult) AsInt64() (v int64, err error) {
	if r.err != nil {
		err = r.err
	} else {
		v, err = r.val.AsInt64()
	}
	return
}

// AsUint64 delegates to ValkeyMessage.AsUint64
func (r ValkeyResult) AsUint64() (v uint64, err error) {
	if r.err != nil {
		err = r.err
	} else {
		v, err = r.val.AsUint64()
	}
	return
}

// AsBool delegates to ValkeyMessage.AsBool
func (r ValkeyResult) AsBool() (v bool, err error) {
	if r.err != nil {
		err = r.err
	} else {
		v, err = r.val.AsBool()
	}
	return
}

// AsFloat64 delegates to ValkeyMessage.AsFloat64
func (r ValkeyResult) AsFloat64() (v float64, err error) {
	if r.err != nil {
		err = r.err
	} else {
		v, err = r.val.AsFloat64()
	}
	return
}

// ToArray delegates to ValkeyMessage.ToArray
func (r ValkeyResult) ToArray() (v []ValkeyMessage, err error) {
	if r.err != nil {
		err = r.err
	} else {
		v, err = r.val.ToArray()
	}
	return
}

// AsStrSlice delegates to ValkeyMessage.AsStrSlice
func (r ValkeyResult) AsStrSlice() (v []string, err error) {
	if r.err != nil {
		err = r.err
	} else {
		v, err = r.val.AsStrSlice()
	}
	return
}

// AsIntSlice delegates to ValkeyMessage.AsIntSlice
func (r ValkeyResult) AsIntSlice() (v []int64, err error) {
	if r.err != nil {
		err = r.err
	} else {
		v, err = r.val.AsIntSlice()
	}
	return
}

// AsFloatSlice delegates to ValkeyMessage.AsFloatSlice
func (r ValkeyResult) AsFloatSlice() (v []float64, err error) {
	if r.err != nil {
		err = r.err
	} else {
		v, err = r.val.AsFloatSlice()
	}
	return
}

// AsBoolSlice delegates to ValkeyMessage.AsBoolSlice
func (r ValkeyResult) AsBoolSlice() (v []bool, err error) {
	if r.err != nil {
		err = r.err
	} else {
		v, err = r.val.AsBoolSlice()
	}
	return
}

// AsXRangeEntry delegates to ValkeyMessage.AsXRangeEntry
func (r ValkeyResult) AsXRangeEntry() (v XRangeEntry, err error) {
	if r.err != nil {
		err = r.err
	} else {
		v, err = r.val.AsXRangeEntry()
	}
	return
}

// AsXRange delegates to ValkeyMessage.AsXRange
func (r ValkeyResult) AsXRange() (v []XRangeEntry, err error) {
	if r.err != nil {
		err = r.err
	} else {
		v, err = r.val.AsXRange()
	}
	return
}

// AsZScore delegates to ValkeyMessage.AsZScore
func (r ValkeyResult) AsZScore() (v ZScore, err error) {
	if r.err != nil {
		err = r.err
	} else {
		v, err = r.val.AsZScore()
	}
	return
}

// AsZScores delegates to ValkeyMessage.AsZScores
func (r ValkeyResult) AsZScores() (v []ZScore, err error) {
	if r.err != nil {
		err = r.err
	} else {
		v, err = r.val.AsZScores()
	}
	return
}

// AsXRead delegates to ValkeyMessage.AsXRead
func (r ValkeyResult) AsXRead() (v map[string][]XRangeEntry, err error) {
	if r.err != nil {
		err = r.err
	} else {
		v, err = r.val.AsXRead()
	}
	return
}

// AsXRangeSlice delegates to ValkeyMessage.AsXRangeSlice
func (r ValkeyResult) AsXRangeSlice() (v XRangeSlice, err error) {
	if r.err != nil {
		err = r.err
	} else {
		v, err = r.val.AsXRangeSlice()
	}
	return
}

// AsXRangeSlices delegates to ValkeyMessage.AsXRangeSlices
func (r ValkeyResult) AsXRangeSlices() (v []XRangeSlice, err error) {
	if r.err != nil {
		err = r.err
	} else {
		v, err = r.val.AsXRangeSlices()
	}
	return
}

// AsXReadSlices delegates to ValkeyMessage.AsXReadSlices
func (r ValkeyResult) AsXReadSlices() (v map[string][]XRangeSlice, err error) {
	if r.err != nil {
		err = r.err
	} else {
		v, err = r.val.AsXReadSlices()
	}
	return
}

func (r ValkeyResult) AsLMPop() (v KeyValues, err error) {
	if r.err != nil {
		err = r.err
	} else {
		v, err = r.val.AsLMPop()
	}
	return
}

func (r ValkeyResult) AsZMPop() (v KeyZScores, err error) {
	if r.err != nil {
		err = r.err
	} else {
		v, err = r.val.AsZMPop()
	}
	return
}

func (r ValkeyResult) AsFtSearch() (total int64, docs []FtSearchDoc, err error) {
	if r.err != nil {
		err = r.err
	} else {
		total, docs, err = r.val.AsFtSearch()
	}
	return
}

func (r ValkeyResult) AsFtAggregate() (total int64, docs []map[string]string, err error) {
	if r.err != nil {
		err = r.err
	} else {
		total, docs, err = r.val.AsFtAggregate()
	}
	return
}

func (r ValkeyResult) AsFtAggregateCursor() (cursor, total int64, docs []map[string]string, err error) {
	if r.err != nil {
		err = r.err
	} else {
		cursor, total, docs, err = r.val.AsFtAggregateCursor()
	}
	return
}

func (r ValkeyResult) AsGeosearch() (locations []GeoLocation, err error) {
	if r.err != nil {
		err = r.err
	} else {
		locations, err = r.val.AsGeosearch()
	}
	return
}

// AsMap delegates to ValkeyMessage.AsMap
func (r ValkeyResult) AsMap() (v map[string]ValkeyMessage, err error) {
	if r.err != nil {
		err = r.err
	} else {
		v, err = r.val.AsMap()
	}
	return
}

// AsStrMap delegates to ValkeyMessage.AsStrMap
func (r ValkeyResult) AsStrMap() (v map[string]string, err error) {
	if r.err != nil {
		err = r.err
	} else {
		v, err = r.val.AsStrMap()
	}
	return
}

// AsIntMap delegates to ValkeyMessage.AsIntMap
func (r ValkeyResult) AsIntMap() (v map[string]int64, err error) {
	if r.err != nil {
		err = r.err
	} else {
		v, err = r.val.AsIntMap()
	}
	return
}

// AsScanEntry delegates to ValkeyMessage.AsScanEntry.
func (r ValkeyResult) AsScanEntry() (v ScanEntry, err error) {
	if r.err != nil {
		err = r.err
	} else {
		v, err = r.val.AsScanEntry()
	}
	return
}

// ToMap delegates to ValkeyMessage.ToMap
func (r ValkeyResult) ToMap() (v map[string]ValkeyMessage, err error) {
	if r.err != nil {
		err = r.err
	} else {
		v, err = r.val.ToMap()
	}
	return
}

// ToAny delegates to ValkeyMessage.ToAny
func (r ValkeyResult) ToAny() (v any, err error) {
	if r.err != nil {
		err = r.err
	} else {
		v, err = r.val.ToAny()
	}
	return
}

// IsCacheHit delegates to ValkeyMessage.IsCacheHit
func (r ValkeyResult) IsCacheHit() bool {
	return r.val.IsCacheHit()
}

// CacheTTL delegates to ValkeyMessage.CacheTTL
func (r ValkeyResult) CacheTTL() int64 {
	return r.val.CacheTTL()
}

// CachePTTL delegates to ValkeyMessage.CachePTTL
func (r ValkeyResult) CachePTTL() int64 {
	return r.val.CachePTTL()
}

// CachePXAT delegates to ValkeyMessage.CachePXAT
func (r ValkeyResult) CachePXAT() int64 {
	return r.val.CachePXAT()
}

// String returns human-readable representation of ValkeyResult
func (r *ValkeyResult) String() string {
	v, _ := (*prettyValkeyResult)(r).MarshalJSON()
	return string(v)
}

type prettyValkeyResult ValkeyResult

// MarshalJSON implements json.Marshaler interface
func (r *prettyValkeyResult) MarshalJSON() ([]byte, error) {
	type PrettyValkeyResult struct {
		Message *prettyValkeyMessage `json:"Message,omitempty"`
		Error   string               `json:"Error,omitempty"`
	}
	obj := PrettyValkeyResult{}
	if r.err != nil {
		obj.Error = r.err.Error()
	} else {
		obj.Message = (*prettyValkeyMessage)(&r.val)
	}
	return json.Marshal(obj)
}

// ValkeyMessage is a valkey response message, it may be a nil response
type ValkeyMessage struct {
	attrs *ValkeyMessage
	bytes *byte
	array *ValkeyMessage

	// intlen is used for a simple number or
	// in conjunction with an array or bytes to store the length of array or string
	intlen int64
	typ    byte
	ttl    [7]byte
}

func (m *ValkeyMessage) string() string {
	if m.bytes == nil {
		return ""
	}
	return unsafe.String(m.bytes, m.intlen)
}

func (m *ValkeyMessage) values() []ValkeyMessage {
	if m.array == nil {
		return nil
	}
	return unsafe.Slice(m.array, m.intlen)
}

func (m *ValkeyMessage) setString(s string) {
	m.bytes = unsafe.StringData(s)
	m.intlen = int64(len(s))
}

func (m *ValkeyMessage) setValues(values []ValkeyMessage) {
	m.array = unsafe.SliceData(values)
	m.intlen = int64(len(values))
}

func (m *ValkeyMessage) cachesize() int {
	n := 9 // typ (1) + length (8) TODO: can we use VarInt instead of fixed 8 bytes for length?
	switch m.typ {
	case typeInteger, typeNull, typeBool:
	case typeArray, typeMap, typeSet:
		for _, val := range m.values() {
			n += val.cachesize()
		}
	default:
		n += len(m.string())
	}
	return n
}

func (m *ValkeyMessage) serialize(o *bytes.Buffer) {
	var buf [8]byte // TODO: can we use VarInt instead of fixed 8 bytes for length?
	o.WriteByte(m.typ)
	switch m.typ {
	case typeInteger, typeNull, typeBool:
		binary.BigEndian.PutUint64(buf[:], uint64(m.intlen))
		o.Write(buf[:])
	case typeArray, typeMap, typeSet:
		binary.BigEndian.PutUint64(buf[:], uint64(len(m.values())))
		o.Write(buf[:])
		for _, val := range m.values() {
			val.serialize(o)
		}
	default:
		binary.BigEndian.PutUint64(buf[:], uint64(len(m.string())))
		o.Write(buf[:])
		o.WriteString(m.string())
	}
}

var ErrCacheUnmarshal = errors.New("cache unmarshal error")

func (m *ValkeyMessage) unmarshalView(c int64, buf []byte) (int64, error) {
	var err error
	if int64(len(buf)) < c+9 {
		return 0, ErrCacheUnmarshal
	}
	m.typ = buf[c]
	c += 1
	size := int64(binary.BigEndian.Uint64(buf[c : c+8]))
	c += 8 // TODO: can we use VarInt instead of fixed 8 bytes for length?
	switch m.typ {
	case typeInteger, typeNull, typeBool:
		m.intlen = size
	case typeArray, typeMap, typeSet:
		m.setValues(make([]ValkeyMessage, size))
		for i := range m.values() {
			if c, err = m.values()[i].unmarshalView(c, buf); err != nil {
				break
			}
		}
	default:
		if int64(len(buf)) < c+size {
			return 0, ErrCacheUnmarshal
		}
		m.setString(BinaryString(buf[c : c+size]))
		c += size
	}
	return c, err
}

// CacheSize returns the buffer size needed by the CacheMarshal.
func (m *ValkeyMessage) CacheSize() int {
	return m.cachesize() + 7 // 7 for ttl
}

// CacheMarshal writes serialized ValkeyMessage to the provided buffer.
// If the provided buffer is nil, CacheMarshal will allocate one.
// Note that an output format is not compatible with different client versions.
func (m *ValkeyMessage) CacheMarshal(buf []byte) []byte {
	if buf == nil {
		buf = make([]byte, 0, m.CacheSize())
	}
	o := bytes.NewBuffer(buf)
	o.Write(m.ttl[:7])
	m.serialize(o)
	return o.Bytes()
}

// CacheUnmarshalView construct the ValkeyMessage from the buffer produced by CacheMarshal.
// Note that the buffer can't be reused after CacheUnmarshalView since it uses unsafe.String on top of the buffer.
func (m *ValkeyMessage) CacheUnmarshalView(buf []byte) error {
	if len(buf) < 7 {
		return ErrCacheUnmarshal
	}
	copy(m.ttl[:7], buf[:7])
	if _, err := m.unmarshalView(7, buf); err != nil {
		return err
	}
	m.attrs = cacheMark
	return nil
}

// IsNil check if the message is a valkey nil response
func (m *ValkeyMessage) IsNil() bool {
	return m.typ == typeNull
}

// IsInt64 check if the message is a valkey RESP3 int response
func (m *ValkeyMessage) IsInt64() bool {
	return m.typ == typeInteger
}

// IsFloat64 check if the message is a valkey RESP3 double response
func (m *ValkeyMessage) IsFloat64() bool {
	return m.typ == typeFloat
}

// IsString check if the message is a valkey string response
func (m *ValkeyMessage) IsString() bool {
	return m.typ == typeBlobString || m.typ == typeSimpleString
}

// IsBool check if the message is a valkey RESP3 bool response
func (m *ValkeyMessage) IsBool() bool {
	return m.typ == typeBool
}

// IsArray check if the message is a valkey array response
func (m *ValkeyMessage) IsArray() bool {
	return m.typ == typeArray || m.typ == typeSet
}

// IsMap check if the message is a valkey RESP3 map response
func (m *ValkeyMessage) IsMap() bool {
	return m.typ == typeMap
}

// Error check if the message is a valkey error response, including nil response
func (m *ValkeyMessage) Error() error {
	if m.typ == typeNull {
		return Nil
	}
	if m.typ == typeSimpleErr || m.typ == typeBlobErr {
		// kvrocks: https://github.com/redis/rueidis/issues/152#issuecomment-1333923750
		mm := *m
		mm.setString(strings.TrimPrefix(m.string(), "ERR "))
		return (*ValkeyError)(&mm)
	}
	return nil
}

// ToString check if the message is a valkey string response and return it
func (m *ValkeyMessage) ToString() (val string, err error) {
	if m.IsString() {
		return m.string(), nil
	}
	if m.IsInt64() || m.array != nil {
		typ := m.typ
		return "", fmt.Errorf("%w: valkey message type %s is not a string", errParse, typeNames[typ])
	}
	return m.string(), m.Error()
}

// AsReader check if the message is a valkey string response and wrap it with the strings.NewReader
func (m *ValkeyMessage) AsReader() (reader io.Reader, err error) {
	str, err := m.ToString()
	if err != nil {
		return nil, err
	}
	return strings.NewReader(str), nil
}

// AsBytes check if the message is a valkey string response and return it as an immutable []byte
func (m *ValkeyMessage) AsBytes() (bs []byte, err error) {
	str, err := m.ToString()
	if err != nil {
		return nil, err
	}
	return unsafe.Slice(unsafe.StringData(str), len(str)), nil
}

// DecodeJSON check if the message is a valkey string response and treat it as JSON, then unmarshal it into the provided value
func (m *ValkeyMessage) DecodeJSON(v any) (err error) {
	b, err := m.AsBytes()
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}

// AsInt64 check if the message is a valkey string response and parse it as int64
func (m *ValkeyMessage) AsInt64() (val int64, err error) {
	if m.IsInt64() {
		return m.intlen, nil
	}
	v, err := m.ToString()
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(v, 10, 64)
}

// AsUint64 check if the message is a valkey string response and parse it as uint64
func (m *ValkeyMessage) AsUint64() (val uint64, err error) {
	if m.IsInt64() {
		return uint64(m.intlen), nil
	}
	v, err := m.ToString()
	if err != nil {
		return 0, err
	}
	return strconv.ParseUint(v, 10, 64)
}

// AsBool checks if the message is a non-nil response and parses it as bool
func (m *ValkeyMessage) AsBool() (val bool, err error) {
	if err = m.Error(); err != nil {
		return
	}
	switch m.typ {
	case typeBlobString, typeSimpleString:
		val = m.string() == "OK"
		return
	case typeInteger:
		val = m.intlen != 0
		return
	case typeBool:
		val = m.intlen == 1
		return
	default:
		typ := m.typ
		return false, fmt.Errorf("%w: valkey message type %s is not a int, string or bool", errParse, typeNames[typ])
	}
}

// AsFloat64 check if the message is a valkey string response and parse it as float64
func (m *ValkeyMessage) AsFloat64() (val float64, err error) {
	if m.IsFloat64() {
		return util.ToFloat64(m.string())
	}
	v, err := m.ToString()
	if err != nil {
		return 0, err
	}
	return util.ToFloat64(v)
}

// ToInt64 check if the message is a valkey RESP3 int response and return it
func (m *ValkeyMessage) ToInt64() (val int64, err error) {
	if m.IsInt64() {
		return m.intlen, nil
	}
	if err = m.Error(); err != nil {
		return 0, err
	}
	typ := m.typ
	return 0, fmt.Errorf("%w: valkey message type %s is not a RESP3 int64", errParse, typeNames[typ])
}

// ToBool check if the message is a valkey RESP3 bool response and return it
func (m *ValkeyMessage) ToBool() (val bool, err error) {
	if m.IsBool() {
		return m.intlen == 1, nil
	}
	if err = m.Error(); err != nil {
		return false, err
	}
	typ := m.typ
	return false, fmt.Errorf("%w: valkey message type %s is not a RESP3 bool", errParse, typeNames[typ])
}

// ToFloat64 check if the message is a valkey RESP3 double response and return it
func (m *ValkeyMessage) ToFloat64() (val float64, err error) {
	if m.IsFloat64() {
		return util.ToFloat64(m.string())
	}
	if err = m.Error(); err != nil {
		return 0, err
	}
	typ := m.typ
	return 0, fmt.Errorf("%w: valkey message type %s is not a RESP3 float64", errParse, typeNames[typ])
}

// ToArray check if the message is a valkey array/set response and return it
func (m *ValkeyMessage) ToArray() ([]ValkeyMessage, error) {
	if m.IsArray() {
		return m.values(), nil
	}
	if err := m.Error(); err != nil {
		return nil, err
	}
	typ := m.typ
	return nil, fmt.Errorf("%w: valkey message type %s is not a array", errParse, typeNames[typ])
}

// AsStrSlice check if the message is a valkey array/set response and convert to []string.
// valkey nil element and other non-string elements will be present as zero.
func (m *ValkeyMessage) AsStrSlice() ([]string, error) {
	values, err := m.ToArray()
	if err != nil {
		return nil, err
	}
	s := make([]string, 0, len(values))
	for _, v := range values {
		s = append(s, v.string())
	}
	return s, nil
}

// AsIntSlice check if the message is a valkey array/set response and convert to []int64.
// valkey nil element and other non-integer elements will be present as zero.
func (m *ValkeyMessage) AsIntSlice() ([]int64, error) {
	values, err := m.ToArray()
	if err != nil {
		return nil, err
	}
	s := make([]int64, len(values))
	for i, v := range values {
		if len(v.string()) != 0 {
			if s[i], err = strconv.ParseInt(v.string(), 10, 64); err != nil {
				return nil, err
			}
		} else {
			s[i] = v.intlen
		}
	}
	return s, nil
}

// AsFloatSlice check if the message is a valkey array/set response and convert to []float64.
// valkey nil element and other non-float elements will be present as zero.
func (m *ValkeyMessage) AsFloatSlice() ([]float64, error) {
	values, err := m.ToArray()
	if err != nil {
		return nil, err
	}
	s := make([]float64, len(values))
	for i, v := range values {
		if len(v.string()) != 0 {
			if s[i], err = util.ToFloat64(v.string()); err != nil {
				return nil, err
			}
		} else {
			s[i] = float64(v.intlen)
		}
	}
	return s, nil
}

// AsBoolSlice checks if the message is a valkey array/set response and converts it to []bool.
// Valkey nil elements and other non-boolean elements will be represented as false.
func (m *ValkeyMessage) AsBoolSlice() ([]bool, error) {
	values, err := m.ToArray()
	if err != nil {
		return nil, err
	}
	s := make([]bool, len(values))
	for i, v := range values {
		s[i], _ = v.AsBool() // Ignore error, non-boolean values will be false
	}
	return s, nil
}

// XRangeEntry is the element type of both XRANGE and XREVRANGE command response array
type XRangeEntry struct {
	FieldValues map[string]string
	ID          string
}

// AsXRangeEntry check if the message is a valkey array/set response of length 2 and convert to XRangeEntry
func (m *ValkeyMessage) AsXRangeEntry() (XRangeEntry, error) {
	values, err := m.ToArray()
	if err != nil {
		return XRangeEntry{}, err
	}
	if len(values) != 2 {
		return XRangeEntry{}, fmt.Errorf("got %d, wanted 2", len(values))
	}
	id, err := values[0].ToString()
	if err != nil {
		return XRangeEntry{}, err
	}
	fieldValues, err := values[1].AsStrMap()
	if err != nil {
		if IsValkeyNil(err) {
			return XRangeEntry{ID: id, FieldValues: nil}, nil
		}
		return XRangeEntry{}, err
	}
	return XRangeEntry{
		ID:          id,
		FieldValues: fieldValues,
	}, nil
}

// AsXRange check if the message is a valkey array/set response and convert to []XRangeEntry
func (m *ValkeyMessage) AsXRange() ([]XRangeEntry, error) {
	values, err := m.ToArray()
	if err != nil {
		return nil, err
	}
	msgs := make([]XRangeEntry, 0, len(values))
	for _, v := range values {
		msg, err := v.AsXRangeEntry()
		if err != nil {
			return nil, err
		}
		msgs = append(msgs, msg)
	}
	return msgs, nil
}

// AsXRead converts XREAD/XREADGRUOP response to map[string][]XRangeEntry
func (m *ValkeyMessage) AsXRead() (ret map[string][]XRangeEntry, err error) {
	if err = m.Error(); err != nil {
		return nil, err
	}
	if m.IsMap() {
		ret = make(map[string][]XRangeEntry, len(m.values())/2)
		for i := 0; i < len(m.values()); i += 2 {
			if ret[m.values()[i].string()], err = m.values()[i+1].AsXRange(); err != nil {
				return nil, err
			}
		}
		return ret, nil
	}
	if m.IsArray() {
		ret = make(map[string][]XRangeEntry, len(m.values()))
		for _, v := range m.values() {
			if !v.IsArray() || len(v.values()) != 2 {
				return nil, fmt.Errorf("got %d, wanted 2", len(v.values()))
			}
			if ret[v.values()[0].string()], err = v.values()[1].AsXRange(); err != nil {
				return nil, err
			}
		}
		return ret, nil
	}
	typ := m.typ
	return nil, fmt.Errorf("%w: valkey message type %s is not a map/array/set", errParse, typeNames[typ])
}

// New slice-based structures that preserve order and duplicates
type XRangeSlice struct {
	ID          string
	FieldValues []XRangeFieldValue
}

type XRangeFieldValue struct {
	Field string
	Value string
}

// AsXRangeSlice converts a ValkeyMessage to XRangeSlice (preserves order and duplicates)
func (m *ValkeyMessage) AsXRangeSlice() (XRangeSlice, error) {
	values, err := m.ToArray()
	if err != nil {
		return XRangeSlice{}, err
	}
	if len(values) != 2 {
		return XRangeSlice{}, fmt.Errorf("got %d, wanted 2", len(values))
	}
	id, err := values[0].ToString()
	if err != nil {
		return XRangeSlice{}, err
	}
	// Handle the field-values array
	fieldArray, err := values[1].ToArray()
	if err != nil {
		if IsValkeyNil(err) {
			return XRangeSlice{ID: id, FieldValues: nil}, nil
		}
		return XRangeSlice{}, err
	}
	// Convert pairs to slice (preserving order)
	fieldValues := make([]XRangeFieldValue, 0, len(fieldArray)/2)
	for i := 0; i < cap(fieldValues); i++ {
		field := fieldArray[i*2].string()
		value := fieldArray[i*2+1].string()
		fieldValues = append(fieldValues, XRangeFieldValue{
			Field: field,
			Value: value,
		})
	}
	return XRangeSlice{
		ID:          id,
		FieldValues: fieldValues,
	}, nil
}

// AsXRangeSlices converts multiple XRange entries to slice format
func (m *ValkeyMessage) AsXRangeSlices() ([]XRangeSlice, error) {
	values, err := m.ToArray()
	if err != nil {
		return nil, err
	}
	msgs := make([]XRangeSlice, 0, len(values))
	for _, v := range values {
		msg, err := v.AsXRangeSlice()
		if err != nil {
			return nil, err
		}
		msgs = append(msgs, msg)
	}
	return msgs, nil
}

// AsXReadSlices converts XREAD/XREADGROUP response to use slice format
func (m *ValkeyMessage) AsXReadSlices() (map[string][]XRangeSlice, error) {
	if err := m.Error(); err != nil {
		return nil, err
	}
	var ret map[string][]XRangeSlice
	var err error
	if m.IsMap() {
		ret = make(map[string][]XRangeSlice, len(m.values())/2)
		for i := 0; i < len(m.values()); i += 2 {
			if ret[m.values()[i].string()], err = m.values()[i+1].AsXRangeSlices(); err != nil {
				return nil, err
			}
		}
		return ret, nil
	}
	if m.IsArray() {
		ret = make(map[string][]XRangeSlice, len(m.values()))
		for _, v := range m.values() {
			if !v.IsArray() || len(v.values()) != 2 {
				return nil, fmt.Errorf("got %d, wanted 2", len(v.values()))
			}
			if ret[v.values()[0].string()], err = v.values()[1].AsXRangeSlices(); err != nil {
				return nil, err
			}
		}
		return ret, nil
	}
	typ := m.typ
	return nil, fmt.Errorf("%w: valkey message type %s is not a map/array/set", errParse, typeNames[typ])
}

// ZScore is the element type of ZRANGE WITHSCORES, ZDIFF WITHSCORES and ZPOPMAX command response
type ZScore struct {
	Member string
	Score  float64
}

func toZScore(values []ValkeyMessage) (s ZScore, err error) {
	if len(values) == 2 {
		if s.Member, err = values[0].ToString(); err == nil {
			s.Score, err = values[1].AsFloat64()
		}
		return s, err
	}
	return ZScore{}, fmt.Errorf("valkey message is not a map/array/set or its length is not 2")
}

// AsZScore converts ZPOPMAX and ZPOPMIN command with count 1 response to a single ZScore
func (m *ValkeyMessage) AsZScore() (s ZScore, err error) {
	arr, err := m.ToArray()
	if err != nil {
		return s, err
	}
	return toZScore(arr)
}

// AsZScores converts ZRANGE WITHSCORES, ZDIFF WITHSCORES and ZPOPMAX/ZPOPMIN command with count > 1 responses to []ZScore
func (m *ValkeyMessage) AsZScores() ([]ZScore, error) {
	arr, err := m.ToArray()
	if err != nil {
		return nil, err
	}
	if len(arr) > 0 && arr[0].IsArray() {
		scores := make([]ZScore, len(arr))
		for i, v := range arr {
			if scores[i], err = toZScore(v.values()); err != nil {
				return nil, err
			}
		}
		return scores, nil
	}
	scores := make([]ZScore, len(arr)/2)
	for i := 0; i < len(scores); i++ {
		j := i * 2
		if scores[i], err = toZScore(arr[j : j+2]); err != nil {
			return nil, err
		}
	}
	return scores, nil
}

// ScanEntry is the element type of both SCAN, SSCAN, HSCAN and ZSCAN command response.
type ScanEntry struct {
	Elements []string
	Cursor   uint64
}

// AsScanEntry check if the message is a valkey array/set response of length 2 and convert to ScanEntry.
func (m *ValkeyMessage) AsScanEntry() (e ScanEntry, err error) {
	msgs, err := m.ToArray()
	if err != nil {
		return ScanEntry{}, err
	}
	if len(msgs) >= 2 {
		if e.Cursor, err = msgs[0].AsUint64(); err == nil {
			e.Elements, err = msgs[1].AsStrSlice()
		}
		return e, err
	}
	typ := m.typ
	return ScanEntry{}, fmt.Errorf("%w: valkey message type %s is not a scan response or its length is not at least 2", errParse, typeNames[typ])
}

// AsMap check if the message is a valkey array/set response and convert to map[string]ValkeyMessage
func (m *ValkeyMessage) AsMap() (map[string]ValkeyMessage, error) {
	if err := m.Error(); err != nil {
		return nil, err
	}
	if (m.IsMap() || m.IsArray()) && len(m.values())%2 == 0 {
		return toMap(m.values())
	}
	typ := m.typ
	return nil, fmt.Errorf("%w: valkey message type %s is not a map/array/set or its length is not even", errParse, typeNames[typ])
}

// AsStrMap check if the message is a valkey map/array/set response and convert to map[string]string.
// valkey nil element and other non-string elements will be present as zero.
func (m *ValkeyMessage) AsStrMap() (map[string]string, error) {
	if err := m.Error(); err != nil {
		return nil, err
	}
	if (m.IsMap() || m.IsArray()) && len(m.values())%2 == 0 {
		r := make(map[string]string, len(m.values())/2)
		for i := 0; i < len(m.values()); i += 2 {
			k := m.values()[i]
			v := m.values()[i+1]
			r[k.string()] = v.string()
		}
		return r, nil
	}
	typ := m.typ
	return nil, fmt.Errorf("%w: valkey message type %s is not a map/array/set or its length is not even", errParse, typeNames[typ])
}

// AsIntMap check if the message is a valkey map/array/set response and convert to map[string]int64.
// valkey nil element and other non-integer elements will be present as zero.
func (m *ValkeyMessage) AsIntMap() (map[string]int64, error) {
	if err := m.Error(); err != nil {
		return nil, err
	}
	if (m.IsMap() || m.IsArray()) && len(m.values())%2 == 0 {
		var err error
		r := make(map[string]int64, len(m.values())/2)
		for i := 0; i < len(m.values()); i += 2 {
			k := m.values()[i]
			v := m.values()[i+1]
			if k.typ == typeBlobString || k.typ == typeSimpleString {
				if len(v.string()) != 0 {
					if r[k.string()], err = strconv.ParseInt(v.string(), 0, 64); err != nil {
						return nil, err
					}
				} else if v.typ == typeInteger || v.typ == typeNull {
					r[k.string()] = v.intlen
				}
			}
		}
		return r, nil
	}
	typ := m.typ
	return nil, fmt.Errorf("%w: valkey message type %s is not a map/array/set or its length is not even", errParse, typeNames[typ])
}

type KeyValues struct {
	Key    string
	Values []string
}

func (m *ValkeyMessage) AsLMPop() (kvs KeyValues, err error) {
	if err = m.Error(); err != nil {
		return KeyValues{}, err
	}
	if len(m.values()) >= 2 {
		kvs.Key = m.values()[0].string()
		kvs.Values, err = m.values()[1].AsStrSlice()
		return
	}
	typ := m.typ
	return KeyValues{}, fmt.Errorf("%w: valkey message type %s is not a LMPOP response", errParse, typeNames[typ])
}

type KeyZScores struct {
	Key    string
	Values []ZScore
}

func (m *ValkeyMessage) AsZMPop() (kvs KeyZScores, err error) {
	if err = m.Error(); err != nil {
		return KeyZScores{}, err
	}
	if len(m.values()) >= 2 {
		kvs.Key = m.values()[0].string()
		kvs.Values, err = m.values()[1].AsZScores()
		return
	}
	typ := m.typ
	return KeyZScores{}, fmt.Errorf("%w: valkey message type %s is not a ZMPOP response", errParse, typeNames[typ])
}

type FtSearchDoc struct {
	Doc   map[string]string
	Key   string
	Score float64
}

func (m *ValkeyMessage) AsFtSearch() (total int64, docs []FtSearchDoc, err error) {
	if err = m.Error(); err != nil {
		return 0, nil, err
	}
	if m.IsMap() {
		for i := 0; i < len(m.values()); i += 2 {
			switch m.values()[i].string() {
			case "total_results":
				total = m.values()[i+1].intlen
			case "results":
				records := m.values()[i+1].values()
				docs = make([]FtSearchDoc, len(records))
				for d, record := range records {
					for j := 0; j < len(record.values()); j += 2 {
						switch record.values()[j].string() {
						case "id":
							docs[d].Key = record.values()[j+1].string()
						case "extra_attributes":
							docs[d].Doc, _ = record.values()[j+1].AsStrMap()
						case "score":
							docs[d].Score, _ = strconv.ParseFloat(record.values()[j+1].string(), 64)
						}
					}
				}
			case "error":
				for _, e := range m.values()[i+1].values() {
					e := e
					return 0, nil, (*ValkeyError)(&e)
				}
			}
		}
		return
	}
	if len(m.values()) > 0 {
		total = m.values()[0].intlen
		wscore := false
		wattrs := false
		offset := 1
		if len(m.values()) > 2 {
			if m.values()[2].string() == "" {
				wattrs = true
				offset++
			} else {
				_, err1 := strconv.ParseFloat(m.values()[1].string(), 64)
				_, err2 := strconv.ParseFloat(m.values()[2].string(), 64)
				wscore = err1 != nil && err2 == nil
				offset++
			}
		}
		if len(m.values()) > 3 && m.values()[3].string() == "" {
			wattrs = true
			offset++
		}
		docs = make([]FtSearchDoc, 0, (len(m.values())-1)/offset)
		for i := 1; i < len(m.values()); i++ {
			doc := FtSearchDoc{Key: m.values()[i].string()}
			if wscore {
				i++
				doc.Score, _ = strconv.ParseFloat(m.values()[i].string(), 64)
			}
			if wattrs {
				i++
				doc.Doc, _ = m.values()[i].AsStrMap()
			}
			docs = append(docs, doc)
		}
		return
	}
	typ := m.typ
	return 0, nil, fmt.Errorf("%w: valkey message type %s is not a FT.SEARCH response", errParse, typeNames[typ])
}

func (m *ValkeyMessage) AsFtAggregate() (total int64, docs []map[string]string, err error) {
	if err = m.Error(); err != nil {
		return 0, nil, err
	}
	if m.IsMap() {
		for i := 0; i < len(m.values()); i += 2 {
			switch m.values()[i].string() {
			case "total_results":
				total = m.values()[i+1].intlen
			case "results":
				records := m.values()[i+1].values()
				docs = make([]map[string]string, len(records))
				for d, record := range records {
					for j := 0; j < len(record.values()); j += 2 {
						switch record.values()[j].string() {
						case "extra_attributes":
							docs[d], _ = record.values()[j+1].AsStrMap()
						}
					}
				}
			case "error":
				for _, e := range m.values()[i+1].values() {
					e := e
					return 0, nil, (*ValkeyError)(&e)
				}
			}
		}
		return
	}
	if len(m.values()) > 0 {
		total = m.values()[0].intlen
		docs = make([]map[string]string, len(m.values())-1)
		for d, record := range m.values()[1:] {
			docs[d], _ = record.AsStrMap()
		}
		return
	}
	typ := m.typ
	return 0, nil, fmt.Errorf("%w: valkey message type %s is not a FT.AGGREGATE response", errParse, typeNames[typ])
}

func (m *ValkeyMessage) AsFtAggregateCursor() (cursor, total int64, docs []map[string]string, err error) {
	if m.IsArray() && len(m.values()) == 2 && (m.values()[0].IsArray() || m.values()[0].IsMap()) {
		total, docs, err = m.values()[0].AsFtAggregate()
		cursor = m.values()[1].intlen
	} else {
		total, docs, err = m.AsFtAggregate()
	}
	return
}

type GeoLocation struct {
	Name                      string
	Longitude, Latitude, Dist float64
	GeoHash                   int64
}

func (m *ValkeyMessage) AsGeosearch() ([]GeoLocation, error) {
	arr, err := m.ToArray()
	if err != nil {
		return nil, err
	}
	geoLocations := make([]GeoLocation, 0, len(arr))
	for _, v := range arr {
		var loc GeoLocation
		if v.IsString() {
			loc.Name = v.string()
		} else {
			info := v.values()
			var i int

			//name
			loc.Name = info[i].string()
			i++
			//distance
			if i < len(info) && info[i].string() != "" {
				loc.Dist, err = util.ToFloat64(info[i].string())
				if err != nil {
					return nil, err
				}
				i++
			}
			//hash
			if i < len(info) && info[i].IsInt64() {
				loc.GeoHash = info[i].intlen
				i++
			}
			//coordinates
			if i < len(info) && info[i].array != nil {
				cord := info[i].values()
				if len(cord) < 2 {
					return nil, fmt.Errorf("got %d, expected 2", len(info))
				}
				loc.Longitude, _ = cord[0].AsFloat64()
				loc.Latitude, _ = cord[1].AsFloat64()
			}
		}
		geoLocations = append(geoLocations, loc)
	}
	return geoLocations, nil
}

// ToMap check if the message is a valkey RESP3 map response and return it
func (m *ValkeyMessage) ToMap() (map[string]ValkeyMessage, error) {
	if m.IsMap() {
		return toMap(m.values())
	}
	if err := m.Error(); err != nil {
		return nil, err
	}
	typ := m.typ
	return nil, fmt.Errorf("%w: valkey message type %s is not a RESP3 map", errParse, typeNames[typ])
}

// ToAny turns the message into go any value
func (m *ValkeyMessage) ToAny() (any, error) {
	if err := m.Error(); err != nil {
		return nil, err
	}
	switch m.typ {
	case typeFloat:
		return util.ToFloat64(m.string())
	case typeBlobString, typeSimpleString, typeVerbatimString, typeBigNumber:
		return m.string(), nil
	case typeBool:
		return m.intlen == 1, nil
	case typeInteger:
		return m.intlen, nil
	case typeMap:
		vs := make(map[string]any, len(m.values())/2)
		for i := 0; i < len(m.values()); i += 2 {
			if v, err := m.values()[i+1].ToAny(); err != nil && !IsValkeyNil(err) {
				vs[m.values()[i].string()] = err
			} else {
				vs[m.values()[i].string()] = v
			}
		}
		return vs, nil
	case typeSet, typeArray:
		vs := make([]any, len(m.values()))
		for i := 0; i < len(m.values()); i++ {
			if v, err := m.values()[i].ToAny(); err != nil && !IsValkeyNil(err) {
				vs[i] = err
			} else {
				vs[i] = v
			}
		}
		return vs, nil
	}
	typ := m.typ
	return nil, fmt.Errorf("%w: valkey message type %s is not a supported in ToAny", errParse, typeNames[typ])
}

// IsCacheHit check if the message is from the client side cache
func (m *ValkeyMessage) IsCacheHit() bool {
	return m.attrs == cacheMark
}

// CacheTTL returns the remaining TTL in seconds of client side cache
func (m *ValkeyMessage) CacheTTL() (ttl int64) {
	milli := m.CachePTTL()
	if milli > 0 {
		if ttl = milli / 1000; milli > ttl*1000 {
			ttl++
		}
		return ttl
	}
	return milli
}

// CachePTTL returns the remaining PTTL in seconds of client side cache
func (m *ValkeyMessage) CachePTTL() int64 {
	milli := m.getExpireAt()
	if milli == 0 {
		return -1
	}
	if milli = milli - time.Now().UnixMilli(); milli < 0 {
		milli = 0
	}
	return milli
}

// CachePXAT returns the remaining PXAT in seconds of client side cache
func (m *ValkeyMessage) CachePXAT() int64 {
	milli := m.getExpireAt()
	if milli == 0 {
		return -1
	}
	return milli
}

func (m *ValkeyMessage) relativePTTL(now time.Time) int64 {
	return m.getExpireAt() - now.UnixMilli()
}

func (m *ValkeyMessage) getExpireAt() int64 {
	return int64(m.ttl[0]) | int64(m.ttl[1])<<8 | int64(m.ttl[2])<<16 | int64(m.ttl[3])<<24 |
		int64(m.ttl[4])<<32 | int64(m.ttl[5])<<40 | int64(m.ttl[6])<<48
}

func (m *ValkeyMessage) setExpireAt(pttl int64) {
	m.ttl[0] = byte(pttl)
	m.ttl[1] = byte(pttl >> 8)
	m.ttl[2] = byte(pttl >> 16)
	m.ttl[3] = byte(pttl >> 24)
	m.ttl[4] = byte(pttl >> 32)
	m.ttl[5] = byte(pttl >> 40)
	m.ttl[6] = byte(pttl >> 48)
}

func toMap(values []ValkeyMessage) (map[string]ValkeyMessage, error) {
	r := make(map[string]ValkeyMessage, len(values)/2)
	for i := 0; i < len(values); i += 2 {
		if values[i].typ == typeBlobString || values[i].typ == typeSimpleString {
			r[values[i].string()] = values[i+1]
			continue
		}
		typ := values[i].typ
		return nil, fmt.Errorf("%w: valkey message type %s as map key is not supported", errParse, typeNames[typ])
	}
	return r, nil
}

func (m *ValkeyMessage) approximateSize() (s int) {
	s += messageStructSize
	s += len(m.string())
	for _, v := range m.values() {
		s += v.approximateSize()
	}
	return
}

// String returns the human-readable representation of ValkeyMessage
func (m *ValkeyMessage) String() string {
	v, _ := (*prettyValkeyMessage)(m).MarshalJSON()
	return string(v)
}

type prettyValkeyMessage ValkeyMessage

func (m *prettyValkeyMessage) string() string {
	if m.bytes == nil {
		return ""
	}
	return unsafe.String(m.bytes, m.intlen)
}

func (m *prettyValkeyMessage) values() []ValkeyMessage {
	if m.array == nil {
		return nil
	}
	return unsafe.Slice(m.array, m.intlen)
}

// MarshalJSON implements json.Marshaler interface
func (m *prettyValkeyMessage) MarshalJSON() ([]byte, error) {
	type PrettyValkeyMessage struct {
		Value any    `json:"Value,omitempty"`
		Type  string `json:"Type,omitempty"`
		Error string `json:"Error,omitempty"`
		Ttl   string `json:"TTL,omitempty"`
	}
	org := (*ValkeyMessage)(m)
	strType, ok := typeNames[m.typ]
	if !ok {
		strType = "unknown"
	}
	obj := PrettyValkeyMessage{Type: strType}
	if m.ttl != [7]byte{} {
		obj.Ttl = time.UnixMilli(org.CachePXAT()).UTC().String()
	}
	if err := org.Error(); err != nil {
		obj.Error = err.Error()
	}
	switch m.typ {
	case typeFloat, typeBlobString, typeSimpleString, typeVerbatimString, typeBigNumber:
		obj.Value = m.string()
	case typeBool:
		obj.Value = m.intlen == 1
	case typeInteger:
		obj.Value = m.intlen
	case typeMap, typeSet, typeArray:
		values := make([]prettyValkeyMessage, len(m.values()))
		for i, value := range m.values() {
			values[i] = prettyValkeyMessage(value)
		}
		obj.Value = values
	}
	return json.Marshal(obj)
}

func slicemsg(typ byte, values []ValkeyMessage) ValkeyMessage {
	return ValkeyMessage{
		typ:    typ,
		array:  unsafe.SliceData(values),
		intlen: int64(len(values)),
	}
}

func strmsg(typ byte, value string) ValkeyMessage {
	return ValkeyMessage{
		typ:    typ,
		bytes:  unsafe.StringData(value),
		intlen: int64(len(value)),
	}
}
