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

// KvdbCompactFlag constants are flags to set in Kvdb.Compact()
type KvdbCompactFlag int

const (
	// CompactCancel will cancel a compaction
	CompactCancel KvdbCompactFlag = 0x01
	// CompactSampLwm will compact to the space amplification low watermark
	CompactSampLwm KvdbCompactFlag = 0x02
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
	// Canceled is whether an externall requested compaction is canceled
	Canceled bool
}

// KvdbInit initializes the HSE KVDB subsystem
//
// This function initializes a range of different internal HSE structures. It
// must be called before any other HSE functions are used. It is not thread safe
// and is idempotent.
func KvdbInit() {
	C.hse_kvdb_init()
}

// KvdbFini shuts down the HSE KVDB subsystem
//
// This function cleanly finalizes a range of different internal HSE structures.
// It should be called prior to application exit and is not thread safe. After
// it is invoked (and even before it returns), calling any other HSE functions
// will result in undefined behavior. This function is not thread safe.
func KvdbFini() {
	C.hse_kvdb_fini()
}

// KvdbMake creates a new Kvdb instance within the named mpool
//
// The mpool must already exist and the client must have permission to use the
// mpool. This function is not thread safe.
func KvdbMake(mpName string, params *Params) error {
	var paramsC *C.struct_hse_params
	if params != nil {
		paramsC = params.impl
	}

	mpNameC := C.CString(mpName)
	defer C.free(unsafe.Pointer(mpNameC))

	err := C.hse_kvdb_make(mpNameC, paramsC)
	if err != 0 {
		return hseErrToErrno(err)
	}

	return nil
}

// KvdbOpen opens a Kvdb for use by the application
//
// The KVDB must already exist and the client must have permission to use it.
// This function is not thread safe.
func KvdbOpen(mpName string, params *Params) (*Kvdb, error) {
	var paramsC *C.struct_hse_params
	if params != nil {
		paramsC = params.impl
	}

	mpNameC := C.CString(mpName)
	defer C.free(unsafe.Pointer(mpNameC))

	var kvdb Kvdb

	err := C.hse_kvdb_open(mpNameC, paramsC, &kvdb.impl)
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

// KvsMake creates a new Kvs within the referenced Kvdb
//
// If the KVS will store multi-segment keys then the parameter "pfx_len" should
// be set to the desired key prefix length - see Params.Set() and related
// functions below. Otherwise the param should be set to 0 (the default). An
// error will result if there is already a KVS with the given name. This
// function is not thread safe.
func (k *Kvdb) KvsMake(kvsName string, params *Params) error {
	var paramsC *C.struct_hse_params
	if params != nil {
		paramsC = params.impl
	}

	kvsNameC := C.CString(kvsName)
	defer C.free(unsafe.Pointer(kvsNameC))

	err := C.hse_kvdb_kvs_make(k.impl, kvsNameC, paramsC)
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
func (k *Kvdb) KvsOpen(kvsName string, params *Params) (*Kvs, error) {
	var paramsC *C.struct_hse_params
	if params != nil {
		paramsC = params.impl
	}

	kvsNameC := C.CString(kvsName)
	defer C.free(unsafe.Pointer(kvsNameC))

	var kvs Kvs

	err := C.hse_kvdb_kvs_open(k.impl, kvsNameC, paramsC, &kvs.impl)
	if err != 0 {
		return nil, hseErrToErrno(err)
	}

	return &kvs, nil
}

// Names returns the Kvs names within a Kvdb
func (k *Kvdb) Names() ([]string, error) {
	var namesc C.uint
	var namesv **C.char

	err := C.hse_kvdb_get_names(k.impl, &namesc, &namesv)
	if err != 0 {
		return nil, hseErrToErrno(err)
	}

	names := make([]string, namesc)
	for i, s := range (*[limits.KvsCountMax]*C.char)(unsafe.Pointer(namesv))[:namesc:namesc] {
		names[i] = C.GoString(s)
	}

	C.hse_kvdb_free_names(k.impl, namesv)

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
	err := C.hse_kvdb_sync(k.impl)
	if err != 0 {
		return hseErrToErrno(err)
	}

	return nil
}

// Flush initiates a data flush in all of the referenced Kvdb's Kvss
func (k *Kvdb) Flush() error {
	err := C.hse_kvdb_flush(k.impl)
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
//     kvdb.Compact(CompactSampLwm);
//
// To cancel an ongoing compaction request for a Kvdb:
//
//     kvdb.Compact(CompactCancel);
//
// See the function Kvdb.CompactStatus(). This function is thread safe.
func (k *Kvdb) Compact(flags KvdbCompactFlag) error {
	err := C.hse_kvdb_compact(k.impl, C.int(flags))
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
