// SPDX-License-Identifier: Apache-2.0
//
// Copyright (C) 2020 Micron Technology, Inc. All rights reserved.

package hse

/*
#include <hse/hse.h>
*/
import "C"
import (
	"unsafe"

	"github.com/hse-project/hse-go/limits"
)

type Cursor struct {
	impl *C.struct_hse_kvs_cursor
	eof  bool
}

type CursorOptions struct {
	Reverse    bool
	StaticView bool
	BindTxn    bool
	Txn        *Transaction
}

func (c *Cursor) Update(options *CursorOptions) error {
	err := C.hse_kvs_cursor_update(c.impl, nil)
	if err != 0 {
		return hseErrToErrno(err)
	}

	return nil
}

func (c *Cursor) Seek(key []byte) ([]byte, error) {
	var keyPtr unsafe.Pointer
	var found unsafe.Pointer
	var foundLen C.size_t

	if key != nil {
		keyPtr = unsafe.Pointer(&key[0])
	}

	err := C.hse_kvs_cursor_seek(c.impl, nil, keyPtr, C.size_t(len(key)), &found, &foundLen)
	if err != 0 {
		return nil, hseErrToErrno(err)
	}

	if found == nil {
		return nil, nil
	}

	return (*[limits.KvsVLenMax]byte)(found)[:foundLen:foundLen], nil
}

func (c *Cursor) SeekRange(filtMin []byte, filtMax []byte) ([]byte, error) {
	var filtMinPtr unsafe.Pointer
	var filtMaxPtr unsafe.Pointer
	var found unsafe.Pointer
	var foundLen C.size_t

	if filtMin != nil {
		filtMinPtr = unsafe.Pointer(&filtMin[0])
	}
	if filtMax != nil {
		filtMaxPtr = unsafe.Pointer(&filtMax[0])
	}

	err := C.hse_kvs_cursor_seek_range(c.impl, nil, filtMinPtr, C.size_t(len(filtMin)), filtMaxPtr, C.size_t(len(filtMax)), &found, &foundLen)
	if err != 0 {
		return nil, hseErrToErrno(err)
	}

	if found == nil {
		return nil, nil
	}

	return (*[limits.KvsVLenMax]byte)(found)[:foundLen:foundLen], nil
}

func (c *Cursor) Read() ([]byte, []byte, error) {
	var keyPtr unsafe.Pointer
	var keyLen C.size_t
	var valuePtr unsafe.Pointer
	var valueLen C.size_t
	var eof C.bool

	err := C.hse_kvs_cursor_read(c.impl, nil, &keyPtr, &keyLen, &valuePtr, &valueLen, &eof)
	if err != 0 {
		return nil, nil, hseErrToErrno(err)
	}

	var key []byte
	var value []byte

	if keyPtr != nil {
		key = (*[limits.KvsKLenMax]byte)(keyPtr)[:keyLen:keyLen]
	}
	if valuePtr != nil {
		value = (*[limits.KvsVLenMax]byte)(valuePtr)[:valueLen:valueLen]
	}

	c.eof = bool(eof)

	if eof {
		return key, value, nil
	}

	return key, value, nil
}

func (c *Cursor) Destroy() error {
	if c.impl == nil {
		return nil
	}

	err := C.hse_kvs_cursor_destroy(c.impl)
	if err != 0 {
		return hseErrToErrno(err)
	}

	c.impl = nil

	return nil
}

// Eof returns whether or not the cursor is at EOF
func (c *Cursor) Eof() bool {
	return c.eof
}
