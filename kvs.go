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

// Kvs is a logical grouping of k/v pairs within a Kvdb
type Kvs struct {
	impl *C.struct_hse_kvs
}

// PutOptions are options that can be supplied to a PUT operation
type PutOptions struct {
	Priority bool
	// Transaction context
	Txn *Transaction
}

// GetOptions are options that can be supplied to a GET operation
type GetOptions struct {
	// Buffer is a user-supplied buffer for storing the value from the GET operation
	Buffer []byte
	// Allocate a buffer of size limits.KvsVLenMax and ignore Buffer
	Allocate bool
	// Transaction context
	Txn *Transaction
}

// DeleteOptions are options that can be supplied to a DELETE operation
type DeleteOptions struct {
	Priority bool
	// Transaction context
	Txn *Transaction
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

func (k *Kvs) Put(key, value []byte, options *PutOptions) error {
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

func (k *Kvs) Get(key []byte, options *GetOptions) ([]byte, uint, error) {
	var buf []byte
	var keyPtr unsafe.Pointer
	var bufPtr unsafe.Pointer
	var found C.bool
	var valueLen C.size_t

	if key != nil {
		keyPtr = unsafe.Pointer(&key[0])
	}
	if options != nil {
		if options.Allocate {
			buf = make([]byte, limits.KvsVLenMax)
		} else {
			buf = options.Buffer
		}
	}
	if buf != nil {
		bufPtr = unsafe.Pointer(&buf[0])
	}

	err := C.hse_kvs_get(k.impl, nil, keyPtr, C.size_t(len(key)), &found, bufPtr, C.size_t(len(buf)), &valueLen)
	if err != 0 {
		return nil, uint(valueLen), hseErrToErrno(err)
	}

	if buf == nil {
		return nil, uint(valueLen), nil
	}

	return buf[:valueLen:valueLen], uint(valueLen), nil
}

func (k *Kvs) Delete(key []byte, options *DeleteOptions) error {
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

func (k *Kvs) PrefixDelete(filt []byte, options *DeleteOptions) (uint, error) {
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

// NewCursor creates a cursor used to iterate over a KVS
//
// When cursors are created they are by default forward iterating. If the caller of
// hse_kvs_cursor_create() passes a reference to an initialized opspec with bit flag
// HSE_KVDB_KOP_FLAG_REVERSE set, then a backwards (reverse sort order) iterating cursor
// is created. A cursor's direction is determined when it is created and is immutable.
//
// Cursors are of one of three types: (1) free, (2) transaction snapshot, and (3)
// transaction bound. A cursor of type (1) is based on an ephemeral snapshot view of the
// KVS at the time it is created. New data is not visible to the cursor until
// hse_kvs_cursor_update() is called on it. A cursor of type (2) takes on the
// transaction's ephemeral snapshot but cannot see any of the mutations made by its
// associated transaction. A cursor of type (3) is like type (2) but it always can see
// the mutations made by the transaction. Calling hse_kvs_cursor_update() on a cursor of
// types (2) and (3) without changing the hse_kvdb_opspec fields is a no-op. This
// function is thread safe.
//
// The hse_kvdb_opspec referent shapes the type and behavior of the cursor created. The
// flag fields within kop_flags are independent. Passing a NULL for the opspec is the
// same as passing an initialized but otherwise unmodified opspec.
//
//   - To create a cursor of type (1):
//       - Pass either a NULL for opspec, or
//       - Pass an initialized opspec with kop_txn == NULL
//
//   - To create a cursor of type (2):
//       - Pass an initialized opspec with kop_txn == <target txn>
//
//   - To create a cursor of type (3):
//       - Pass an initialized opspec with kop_txn == <target txn> and
//         a kop_flags value with position HSE_KVDB_KOP_FLAG_BIND_TXN set
//
// The primary utility of the prefix filter mechanism is to maximize the efficiency of
// cursor iteration on a KVS with multi-segment keys. For that use case, the caller
// should supply a filter whose length is greater than or equal to the KVS key prefix
// length. The caller can also provide a filter that is shorter than the key prefix
// length or can perform this operation on a KVS whose key prefix length is zero. In all
// cases, the cursor will be restricted to keys matching the given prefix filter.
//
// When a transaction associated with a cursor of type (3) commits or aborts, the state
// of the cursor becomes unbound, i.e., it becomes of type (1). What can be seen through
// the cursor depends on whether it was created with the CursorOptions.StaticView flag set.
//
// If it was set, then the cursor retains the snapshot view of the transaction (for both
// commit and abort). If it was not set then the view of the cursor is that of the
// database at the time of the commit or abort. In the commit case, the cursor can see
// the mutations of the transaction, if any. Note that this will make any other
// mutations that occurred during the lifespan of the transaction visible as well.
func (k *Kvs) NewCursor(filt []byte, options *CursorOptions) (*Cursor, error) {
	var c Cursor
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
