/* SPDX-License-Identifier: Apache-2.0 OR MIT
 *
 * SPDX-FileCopyrightText: Copyright 2022 Micron Technology, Inc.
 */

package hse

// #include <hse/hse.h>
import "C"
import (
	"unsafe"

	"github.com/hse-project/hse-go/limits"
)

// Kvs is a logical grouping of k/v pairs within a Kvdb
type Kvs struct {
	impl *C.struct_hse_kvs
}

type DeleteFlags uint
type GetFlags uint
type PrefixDeleteFlags uint
type PutFlags uint

const (
	KVS_PUT_PRIO      PutFlags = C.HSE_KVS_PUT_PRIO
	KVS_PUT_VCOMP_OFF PutFlags = C.HSE_KVS_PUT_VCOMP_OFF
)

// Close closes an open KVS
//
// No client thread may enter the HSE Kvdb API with the referenced Kvs after this
// function starts. This function is not thread safe.
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

// Put places a KV pair into a Kvs
//
// If the key already exists in the Kvs then the value is effectively overwritten. The
// key length must be in the range [1, HSE_KVS_KLEN_MAX] while the value length must be
// in the range [0, HSE_KVS_VLEN_MAX]. See the section on transactions for information
// on how puts within transactions are handled. This function is thread safe.
//
// The HSE Kvdb attempts to maintain reasonable QoS and for high-throughput clients this
// results in very short sleep's being inserted into the put path. For some kinds of
// data (e.g., control metadata) the client may wish to not experience that delay. For
// relatively low data rate uses, the caller can set the Priority flag for a PutOptions object.
// Care should be taken when doing so to ensure that the system does not become overrun. As a rough
// approximation, doing 1M priority puts per second marked as PRIORITY is likely an issue. On the
// other hand, doing 1K small puts per second marked as PRIORITY is almost certainly fine.
func (k *Kvs) Put(key, value []byte, flags PutFlags) error {
	var keyPtr unsafe.Pointer
	var valuePtr unsafe.Pointer

	if key != nil {
		keyPtr = unsafe.Pointer(&key[0])
	}
	if value != nil {
		valuePtr = unsafe.Pointer(&value[0])
	}

	err := C.hse_kvs_put(k.impl, 0, nil, keyPtr, C.size_t(len(key)), valuePtr, C.size_t(len(value)))
	if err != 0 {
		return hseErrToErrno(err)
	}

	return nil
}

// Get retrieves the value for a given key from Kvs
//
// If the key exists in the Kvs then the referent of "found" is set to true. If the
// caller's value buffer is large enough then the data will be returned. Regardless, the
// actual length of the value is returned . See the section on transactions for
// information on how gets within transactions are handled. This function is thread
// safe.
func (k *Kvs) Get(key []byte, flags GetFlags) ([]byte, uint, error) {
	var buf []byte
	var keyPtr unsafe.Pointer
	var bufPtr unsafe.Pointer
	var found C.bool
	var valueLen C.size_t

	if key != nil {
		keyPtr = unsafe.Pointer(&key[0])
	}

	buf = make([]byte, limits.KVS_VALUE_LEN_MAX)
	if buf != nil {
		bufPtr = unsafe.Pointer(&buf[0])
	}

	err := C.hse_kvs_get(k.impl, 0, nil, keyPtr, C.size_t(len(key)), &found, bufPtr, C.size_t(len(buf)), &valueLen)
	if err != 0 {
		return nil, uint(valueLen), hseErrToErrno(err)
	}

	if buf == nil {
		return nil, uint(valueLen), nil
	}

	return buf[:valueLen:valueLen], uint(valueLen), nil
}

// Delete deletes the key and its associated value from the Kvs
//
// It is not an error if the key does not exist within the Kvs. See the section on
// transactions for information on how deletes within transactions are handled. This
// function is thread safe.
func (k *Kvs) Delete(key []byte, flags DeleteFlags) error {
	var keyPtr unsafe.Pointer

	if key != nil {
		keyPtr = unsafe.Pointer(&key[0])
	}

	err := C.hse_kvs_delete(k.impl, C.uint(flags), nil, keyPtr, C.size_t(len(key)))
	if err != 0 {
		return hseErrToErrno(err)
	}

	return nil
}

// PrefixDelete deletes all KV pairs matching the key prefix from a KVS storing multi-segment keys
//
// This interface is used to delete an entire range of multi-segment keys. To do this
// the caller passes a filter with a length equal to the Kvs' key prefix length. It is
// not an error if no keys exist matching the filter. If there is a filtered iteration
// in progress, then that iteration can fail if Kvs.PrefixDelete() is called with
// a filter matching the iteration. This function is thread safe.
//
// If Kvs.PrefixDelete() is called from a transaction context, it affects no
// key-value mutations that are part of the same transaction. Stated differently, for
// Kvs commands issued within a transaction, all calls to Kvs.PrefixDelete() are
// treated as though they were issued serially at the beginning of the transaction
// regardless of the actual order these commands appeared in.
func (k *Kvs) PrefixDelete(filt []byte, flags PrefixDeleteFlags) error {
	var filtPtr unsafe.Pointer

	if filt != nil {
		filtPtr = unsafe.Pointer(&filt[0])
	}

	err := C.hse_kvs_prefix_delete(k.impl, C.uint(flags), nil, filtPtr, C.size_t(len(filt)))
	if err != 0 {
		return hseErrToErrno(err)
	}

	return nil
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
//
//   - Pass either a NULL for opspec, or
//
//   - Pass an initialized opspec with kop_txn == NULL
//
//   - To create a cursor of type (2):
//
//   - Pass an initialized opspec with kop_txn == <target txn>
//
//   - To create a cursor of type (3):
//
//   - Pass an initialized opspec with kop_txn == <target txn> and
//     a kop_flags value with position HSE_KVDB_KOP_FLAG_BIND_TXN set
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
func (k *Kvs) CreateCursor(filt []byte, flags CursorCreateFlag) (*Cursor, error) {
	var c Cursor
	var filtPtr unsafe.Pointer

	if filt != nil {
		filtPtr = unsafe.Pointer(&filt[0])
	}

	err := C.hse_kvs_cursor_create(k.impl, C.uint(flags), nil, filtPtr, C.size_t(len(filt)), &c.impl)
	if err != 0 {
		return nil, hseErrToErrno(err)
	}

	return &c, nil
}
