// SPDX-License-Identifier: Apache-2.0
//
// Copyright (C) 2020 Micron Technology, Inc. All rights reserved.

package hse

// #include <hse/hse.h>
import "C"
import (
	"unsafe"

	"github.com/hse-project/hse-go/limits"
)

type Kvs struct {
	impl *C.struct_hse_kvs
}

func (k *Kvs) Close() error {
	if k.impl == nil {
		return nil
	}

	err := C.hse_kvdb_kvs_close(k.impl)
	if err != 0 {
		return hseErrToErrno(err)
	}

	k.impl = nil

	return nil
}

func (k *Kvs) Put(key, value []byte) error {
	var keyPtr unsafe.Pointer
	var valuePtr unsafe.Pointer

	if key != nil {
		keyPtr = unsafe.Pointer(&key[0])
	}
	if value != nil {
		valuePtr = unsafe.Pointer(&value[0])
	}

	err := C.hse_kvs_put(k.impl, nil, keyPtr, C.size_t(len(key)), valuePtr, C.size_t(len(value)))
	if err != 0 {
		return hseErrToErrno(err)
	}

	return nil
}

func (k *Kvs) Get(key []byte) ([]byte, uint, error) {
	buf := make([]byte, limits.KvsVLenMax)

	return k.GetWithBuffer(key, buf)
}

func (k *Kvs) GetWithBuffer(key, buf []byte) ([]byte, uint, error) {
	var keyPtr unsafe.Pointer
	var bufPtr unsafe.Pointer
	var found C.bool
	var valueLen C.size_t

	if key != nil {
		keyPtr = unsafe.Pointer(&key[0])
	}

	if buf != nil {
		bufPtr = unsafe.Pointer(&buf[0])
	}

	err := C.hse_kvs_get(k.impl, nil, keyPtr, C.size_t(len(key)), &found, bufPtr, C.size_t(len(buf)), &valueLen)
	if err != 0 {
		return nil, uint(valueLen), hseErrToErrno(err)
	}

	if valueLen == 0 {
		return nil, uint(valueLen), nil
	}

	if buf == nil {
		return nil, uint(valueLen), nil
	}

	return buf[:valueLen:valueLen], uint(valueLen), nil
}

func (k *Kvs) Delete(key []byte) error {
	var keyPtr unsafe.Pointer

	if key != nil {
		keyPtr = unsafe.Pointer(&key[0])
	}

	err := C.hse_kvs_delete(k.impl, nil, keyPtr, C.size_t(len(key)))
	if err != 0 {
		return hseErrToErrno(err)
	}

	return nil
}

func (k *Kvs) PrefixDelete(filt []byte) (uint, error) {
	var kvsPfxLen C.size_t
	var filtPtr unsafe.Pointer

	if filt != nil {
		filtPtr = unsafe.Pointer(&filt[0])
	}

	err := C.hse_kvs_prefix_delete(k.impl, nil, filtPtr, C.size_t(len(filt)), &kvsPfxLen)
	if err != 0 {
		return uint(kvsPfxLen), hseErrToErrno(err)
	}

	return uint(kvsPfxLen), nil
}

func (k *Kvs) CursorCreate(filt []byte) (*KvsCursor, error) {
	var c KvsCursor
	var filtPtr unsafe.Pointer

	if filt != nil {
		filtPtr = unsafe.Pointer(&filt[0])
	}

	err := C.hse_kvs_cursor_create(k.impl, nil, filtPtr, C.size_t(len(filt)), &c.impl)
	if err != 0 {
		return nil, hseErrToErrno(err)
	}

	return &c, nil
}
