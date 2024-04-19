// Copyright (c) 2013 The github.com/go-redis/redis Authors.
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
// * Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
// * Redistributions in binary form must reproduce the above
// copyright notice, this list of conditions and the following disclaimer
// in the documentation and/or other materials provided with the
// distribution.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

package valkeycompat

import (
	"math"
	"strconv"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type data struct {
	Omit  string `valkey:"-"`
	Empty string

	String         string   `valkey:"string"`
	Bytes          []byte   `valkey:"byte"`
	Int            int      `valkey:"int"`
	Int8           int8     `valkey:"int8"`
	Int16          int16    `valkey:"int16"`
	Int32          int32    `valkey:"int32"`
	Int64          int64    `valkey:"int64"`
	Uint           uint     `valkey:"uint"`
	Uint8          uint8    `valkey:"uint8"`
	Uint16         uint16   `valkey:"uint16"`
	Uint32         uint32   `valkey:"uint32"`
	Uint64         uint64   `valkey:"uint64"`
	Float          float32  `valkey:"float"`
	Float64        float64  `valkey:"float64"`
	Bool           bool     `valkey:"bool"`
	StringPointer  *string  `valkey:"stringPointer"`
	IntPointer     *int     `valkey:"intPointer"`
	Int8Pointer    *int8    `valkey:"int8Pointer"`
	Int16Pointer   *int16   `valkey:"int16Pointer"`
	Int32Pointer   *int32   `valkey:"int32Pointer"`
	Int64Pointer   *int64   `valkey:"int64Pointer"`
	UintPointer    *uint    `valkey:"uintPointer"`
	Uint8Pointer   *uint8   `valkey:"uint8Pointer"`
	Uint16Pointer  *uint16  `valkey:"uint16Pointer"`
	Uint32Pointer  *uint32  `valkey:"uint32Pointer"`
	Uint64Pointer  *uint64  `valkey:"uint64Pointer"`
	FloatPointer   *float32 `valkey:"floatPointer"`
	Float64Pointer *float64 `valkey:"float64Pointer"`
	BoolPointer    *bool    `valkey:"boolPointer"`
}

type TimeRFC3339Nano struct {
	time.Time
}

func (t *TimeRFC3339Nano) ScanValkey(s string) (err error) {
	t.Time, err = time.Parse(time.RFC3339Nano, s)
	return
}

type TimeData struct {
	Name string           `valkey:"name"`
	Time *TimeRFC3339Nano `valkey:"login"`
}

type i []interface{}

var _ = Describe("Scan", func() {
	It("catches bad args", func() {
		var d data

		Expect(Scan(&d, []string{}, i{})).NotTo(HaveOccurred())
		Expect(d).To(Equal(data{}))

		Expect(Scan(&d, []string{"key"}, i{})).To(HaveOccurred())
		Expect(Scan(&d, []string{"key"}, i{"1", "2"})).To(HaveOccurred())
		Expect(Scan(nil, []string{"key", "1"}, i{})).To(HaveOccurred())

		var m map[string]interface{}
		Expect(Scan(&m, []string{"key"}, i{"1"})).To(HaveOccurred())
		Expect(Scan(data{}, []string{"key"}, i{"1"})).To(HaveOccurred())
		Expect(Scan(data{}, []string{"key", "string"}, i{nil, nil})).To(HaveOccurred())
	})

	It("number out of range", func() {
		f := func(v uint64) string {
			return strconv.FormatUint(v, 10) + "1"
		}
		keys := []string{"int8", "int16", "int32", "int64", "uint8", "uint16", "uint32", "uint64", "float", "float64"}
		vals := i{
			f(math.MaxInt8), f(math.MaxInt16), f(math.MaxInt32), f(math.MaxInt64),
			f(math.MaxUint8), f(math.MaxUint16), f(math.MaxUint32), strconv.FormatUint(math.MaxUint64, 10) + "1",
			"13.4028234663852886e+38", "11.79769313486231570e+308",
		}
		for k, v := range keys {
			var d data
			Expect(Scan(&d, []string{v}, i{vals[k]})).To(HaveOccurred())
		}

		// success
		f = func(v uint64) string {
			return strconv.FormatUint(v, 10)
		}
		keys = []string{"int8", "int16", "int32", "int64", "uint8", "uint16", "uint32", "uint64", "float", "float64"}
		vals = i{
			f(math.MaxInt8), f(math.MaxInt16), f(math.MaxInt32), f(math.MaxInt64),
			f(math.MaxUint8), f(math.MaxUint16), f(math.MaxUint32), strconv.FormatUint(math.MaxUint64, 10),
			"3.40282346638528859811704183484516925440e+38", "1.797693134862315708145274237317043567981e+308",
		}
		var d data
		Expect(Scan(&d, keys, vals)).NotTo(HaveOccurred())
		Expect(d).To(Equal(data{
			Int8:    math.MaxInt8,
			Int16:   math.MaxInt16,
			Int32:   math.MaxInt32,
			Int64:   math.MaxInt64,
			Uint8:   math.MaxUint8,
			Uint16:  math.MaxUint16,
			Uint32:  math.MaxUint32,
			Uint64:  math.MaxUint64,
			Float:   math.MaxFloat32,
			Float64: math.MaxFloat64,
		}))
	})

	It("scans good values", func() {
		var d data

		// non-tagged fields.
		Expect(Scan(&d, []string{"key"}, i{"value"})).NotTo(HaveOccurred())
		Expect(d).To(Equal(data{}))

		keys := []string{"string", "byte", "int", "int64", "uint", "uint64", "float", "float64", "bool"}
		vals := i{
			"str!", "bytes!", "123", "123456789123456789", "456", "987654321987654321",
			"123.456", "123456789123456789.987654321987654321", "1",
		}
		Expect(Scan(&d, keys, vals)).NotTo(HaveOccurred())
		Expect(d).To(Equal(data{
			String:  "str!",
			Bytes:   []byte("bytes!"),
			Int:     123,
			Int64:   123456789123456789,
			Uint:    456,
			Uint64:  987654321987654321,
			Float:   123.456,
			Float64: 1.2345678912345678e+17,
			Bool:    true,
		}))

		// Scan a different type with the same values to test that
		// the struct spec maps don't conflict.
		type data2 struct {
			String string  `valkey:"string"`
			Bytes  []byte  `valkey:"byte"`
			Int    int     `valkey:"int"`
			Uint   uint    `valkey:"uint"`
			Float  float32 `valkey:"float"`
			Bool   bool    `valkey:"bool"`
		}
		var d2 data2
		Expect(Scan(&d2, keys, vals)).NotTo(HaveOccurred())
		Expect(d2).To(Equal(data2{
			String: "str!",
			Bytes:  []byte("bytes!"),
			Int:    123,
			Uint:   456,
			Float:  123.456,
			Bool:   true,
		}))

		Expect(Scan(&d, []string{"string", "float", "bool"}, i{"", "1", "t"})).NotTo(HaveOccurred())
		Expect(d).To(Equal(data{
			String:  "",
			Bytes:   []byte("bytes!"),
			Int:     123,
			Int64:   123456789123456789,
			Uint:    456,
			Uint64:  987654321987654321,
			Float:   1.0,
			Float64: 1.2345678912345678e+17,
			Bool:    true,
		}))

		// Scan to pointers
		var String = "str"
		var Int int = 123
		var Int64 int64 = 123456789123456789
		var Uint uint = 456
		var Uint64 uint64 = 987654321987654321
		var Float float32 = 123.456
		var Float64 float64 = 1.2345678912345678e+17
		var Bool bool = true

		var d3 data
		keys = []string{"stringPointer", "intPointer", "int64Pointer", "uintPointer", "uint64Pointer", "floatPointer", "float64Pointer", "boolPointer"}
		vals = i{"str", "123", "123456789123456789", "456", "987654321987654321", "123.456", "123456789123456789.987654321987654321", "1"}
		Expect(Scan(&d3, keys, vals)).NotTo(HaveOccurred())
		Expect(d3).To(Equal(data{
			StringPointer:  &String,
			IntPointer:     &Int,
			Int64Pointer:   &Int64,
			UintPointer:    &Uint,
			Uint64Pointer:  &Uint64,
			FloatPointer:   &Float,
			Float64Pointer: &Float64,
			BoolPointer:    &Bool,
		}))
	})

	It("omits untagged fields", func() {
		var d data

		Expect(Scan(&d, []string{"empty", "omit", "string"}, i{"value", "value", "str!"})).NotTo(HaveOccurred())
		Expect(d).To(Equal(data{
			String: "str!",
		}))
	})

	It("catches bad values", func() {
		var d data

		Expect(Scan(&d, []string{"int"}, i{"a"})).To(HaveOccurred())
		Expect(Scan(&d, []string{"uint"}, i{"a"})).To(HaveOccurred())
		Expect(Scan(&d, []string{"uint"}, i{""})).To(HaveOccurred())
		Expect(Scan(&d, []string{"float"}, i{"b"})).To(HaveOccurred())
		Expect(Scan(&d, []string{"bool"}, i{"-1"})).To(HaveOccurred())
		Expect(Scan(&d, []string{"bool"}, i{""})).To(HaveOccurred())
		Expect(Scan(&d, []string{"bool"}, i{"123"})).To(HaveOccurred())
	})

	It("Implements Scanner", func() {
		var td TimeData

		now := time.Now()
		Expect(Scan(&td, []string{"name", "login"}, i{"hello", now.Format(time.RFC3339Nano)})).NotTo(HaveOccurred())
		Expect(td.Name).To(Equal("hello"))
		Expect(td.Time.UnixNano()).To(Equal(now.UnixNano()))
		Expect(td.Time.Format(time.RFC3339Nano)).To(Equal(now.Format(time.RFC3339Nano)))
	})

	It("should time.Time RFC3339Nano", func() {
		type TimeTime struct {
			Time time.Time `valkey:"time"`
		}

		now := time.Now()

		var tt TimeTime
		Expect(Scan(&tt, []string{"time"}, i{now.Format(time.RFC3339Nano)})).NotTo(HaveOccurred())
		Expect(now.Unix()).To(Equal(tt.Time.Unix()))
	})
})
