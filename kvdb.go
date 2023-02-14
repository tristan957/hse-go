/* SPDX-License-Identifier: Apache-2.0 OR MIT
 *
 * SPDX-FileCopyrightText: Copyright 2022 Micron Technology, Inc.
 */

package hse

// #include <hse/hse.h>
// #include <hse/experimental.h>
import "C"
import (
	"unsafe"

	"github.com/hse-project/hse-go/limits"
)

// KvdbCompactFlag constants are flags to set in Kvdb.Compact()
type KvdbCompactFlag uint

const (
	// KVDB_COMPACT_CANCEL will cancel a compaction
	KVDB_COMPACT_CANCEL KvdbCompactFlag = C.HSE_KVDB_COMPACT_CANCEL
	// KVDB_COMPACT_SAMP_LWM will compact to the space amplification low watermark
	KVDB_COMPACT_SAMP_LWM KvdbCompactFlag = C.HSE_KVDB_COMPACT_SAMP_LWM
	KVDB_COMPACT_FULL     KvdbCompactFlag = C.HSE_KVDB_COMPACT_FULL
)

// Kvdb is a key-value database which is comprised of one or many Kvs
type Kvdb struct {
	impl *C.struct_hse_kvdb
}

// KvdbCompactStatus is the current state of a compaction
type KvdbCompactStatus struct {
	// SampLwm is the space amp low watermark (%)
	SampLwm uint
	// SampHwm is the space amp high watermark (%)
	SampHwm uint
	// SampCurr is the current space amplification
	SampCurr uint
	// Active is whether an externally requested compaction is underway
	Active bool
	// Canceled is whether an externally requested compaction is canceled
	Canceled bool
}

// KvdbCreate creates a new Kvdb instance within the named mpool
//
// The mpool must already exist and the client must have permission to use the
// mpool. This function is not thread safe.
func KvdbCreate(home string, params ...string) error {
	homeC := C.CString(home)
	defer C.free(unsafe.Pointer(homeC))

	cparams := newCParams(params)
	defer cparams.free()

	err := C.hse_kvdb_create(homeC, cparams.Len(), cparams.Ptr())
	if err != 0 {
		return hseErrToErrno(err)
	}

	return nil
}

// KvdbOpen opens a Kvdb for use by the application
//
// The KVDB must already exist and the client must have permission to use it.
// This function is not thread safe.
func KvdbOpen(home string, params []string) (*Kvdb, error) {
	homeC := C.CString(home)
	defer C.free(unsafe.Pointer(homeC))

	cparams := newCParams(params)
	defer cparams.free()

	var kvdb Kvdb

	err := C.hse_kvdb_open(homeC, cparams.Len(), cparams.Ptr(), &kvdb.impl)
	if err != 0 {
		return nil, hseErrToErrno(err)
	}

	return &kvdb, nil
}

// Close closes an open Kvdb
//
// No client thread may enter the HSE KVDB API with the referenced KVDB after
// this function starts. This function is not thread safe.
func (k *Kvdb) Close() error {
	if k.impl == nil {
		return nil
	}

	err := C.hse_kvdb_close(k.impl)
	if err != 0 {
		return hseErrToErrno(err)
	}

	k.impl = nil

	return nil
}

// KvsCreate creates a new Kvs within the referenced Kvdb
//
// If the KVS will store multi-segment keys then the parameter "pfx_len" should
// be set to the desired key prefix length - see Params.Set() and related
// functions below. Otherwise the param should be set to 0 (the default). An
// error will result if there is already a KVS with the given name. This
// function is not thread safe.
func (k *Kvdb) KvsCreate(kvsName string, params ...string) error {
	kvsNameC := C.CString(kvsName)
	defer C.free(unsafe.Pointer(kvsNameC))

	cparams := newCParams(params)
	defer cparams.free()

	err := C.hse_kvdb_kvs_create(k.impl, kvsNameC, cparams.Len(), cparams.Ptr())
	if err != 0 {
		return hseErrToErrno(err)
	}

	return nil
}

// KvsDrop removes a Kvs from the referenced Kvdb
//
// It is an error to call this function on a KVS that is open. This function is
// not thread safe.
func (k *Kvdb) KvsDrop(kvsName string) error {
	kvsNameC := C.CString(kvsName)
	defer C.free(unsafe.Pointer(kvsNameC))

	err := C.hse_kvdb_kvs_drop(k.impl, kvsNameC)
	if err != 0 {
		return hseErrToErrno(err)
	}

	return nil
}

// KvsOpen opens a Kvs in a Kvdb
//
// This function is not thread safe.
func (k *Kvdb) KvsOpen(kvsName string, params ...string) (*Kvs, error) {
	kvsNameC := C.CString(kvsName)
	defer C.free(unsafe.Pointer(kvsNameC))

	cparams := newCParams(params)
	defer cparams.free()

	var kvs Kvs

	err := C.hse_kvdb_kvs_open(k.impl, kvsNameC, cparams.Len(), cparams.Ptr(), &kvs.impl)
	if err != 0 {
		return nil, hseErrToErrno(err)
	}

	return &kvs, nil
}

// Names returns the Kvs names within a Kvdb
func (k *Kvdb) KvsNames() ([]string, error) {
	var namesc C.size_t
	var namesv **C.char

	err := C.hse_kvdb_kvs_names_get(k.impl, &namesc, &namesv)
	if err != 0 {
		return nil, hseErrToErrno(err)
	}

	names := make([]string, namesc)
	for i, s := range (*[limits.KVS_COUNT_MAX]*C.char)(unsafe.Pointer(namesv))[:namesc:namesc] {
		names[i] = C.GoString(s)
	}

	C.hse_kvdb_kvs_names_free(k.impl, namesv)

	return names, nil
}

// NewTransaction allocates a transaction object
//
// This object can and should be re-used many times to avoid the overhead of
// allocation. This function is thread safe.
func (k *Kvdb) NewTransaction() *Transaction {
	txn := C.hse_kvdb_txn_alloc(k.impl)
	if txn == nil {
		return nil
	}

	return &Transaction{
		impl: txn,
		kvdb: k,
	}
}

// Sync flushes data in all of the referenced KVDB's KVSs to stable media and
// returns
func (k *Kvdb) Sync() error {
	err := C.hse_kvdb_sync(k.impl, 0)
	if err != 0 {
		return hseErrToErrno(err)
	}

	return nil
}

// Compact requests a data compaction operation
//
// In managing the data within an HSE KVDB, there are maintenance activities
// that occur as background processing. The application may be aware that it is
// advantageous to do enough maintenance now for the database to be as compact
// as it ever would be in normal operation. To achieve this, the client calls
// this function in the following fashion:
//
//	kvdb.Compact(CompactSampLwm);
//
// To cancel an ongoing compaction request for a Kvdb:
//
//	kvdb.Compact(CompactCancel);
//
// See the function Kvdb.CompactStatus(). This function is thread safe.
func (k *Kvdb) Compact(flags KvdbCompactFlag) error {
	err := C.hse_kvdb_compact(k.impl, C.uint(flags))
	if err != 0 {
		return hseErrToErrno(err)
	}

	return nil
}

// CompactStatus gets the status of an ongoing compaction activity
//
// The caller can examine the fields of the hse_kvdb_compact_status struct to
// determine the current state of maintenance compaction. This function is
// thread safe.
func (k *Kvdb) CompactStatus() (KvdbCompactStatus, error) {
	var compactStatus C.struct_hse_kvdb_compact_status

	err := C.hse_kvdb_compact_status_get(k.impl, &compactStatus)
	if err != 0 {
		return KvdbCompactStatus{}, hseErrToErrno(err)
	}

	return KvdbCompactStatus{
		SampLwm:  uint(compactStatus.kvcs_samp_lwm),
		SampHwm:  uint(compactStatus.kvcs_samp_hwm),
		SampCurr: uint(compactStatus.kvcs_samp_curr),
		Active:   compactStatus.kvcs_active != 0,
		Canceled: compactStatus.kvcs_canceled != 0,
	}, nil
}
