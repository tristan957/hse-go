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

type KvdbOpspecFlag int

const (
	OpspecReverse    KvdbOpspecFlag = 0x01
	OpspecBindTxn    KvdbOpspecFlag = 0x02
	OpspecStaticView KvdbOpspecFlag = 0x04
	OpspecPriority   KvdbOpspecFlag = 0x08
)

type KvdbOpspec struct {
	impl C.struct_hse_kvdb_opspec
}

type KvdbCompactFlag int

const (
	CompactCancel  KvdbCompactFlag = 0x01
	CompactSampLwm KvdbCompactFlag = 0x02
)

type Kvdb struct {
	impl *C.struct_hse_kvdb
}

type KvdbCompactStatus struct {
	SampLwm  uint
	SampHwm  uint
	SampCurr uint
	Active   bool
	Canceled bool
}

func KvdbInit() {
	C.hse_kvdb_init()
}

func KvdbFini() {
	C.hse_kvdb_fini()
}

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

func (k *Kvdb) KvsDrop(kvsName string) error {
	kvsNameC := C.CString(kvsName)
	defer C.free(unsafe.Pointer(kvsNameC))

	err := C.hse_kvdb_kvs_drop(k.impl, kvsNameC)
	if err != 0 {
		return hseErrToErrno(err)
	}

	return nil
}

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

func (k *Kvdb) TxnAlloc() *KvdbTxn {
	txn := C.hse_kvdb_txn_alloc(k.impl)
	if txn == nil {
		return nil
	}

	return &KvdbTxn{
		impl: txn,
		kvdb: k.impl,
	}
}

func (k *Kvdb) Sync() error {
	err := C.hse_kvdb_sync(k.impl)
	if err != 0 {
		return hseErrToErrno(err)
	}

	return nil
}

func (k *Kvdb) Flush() error {
	err := C.hse_kvdb_flush(k.impl)
	if err != 0 {
		return hseErrToErrno(err)
	}

	return nil
}

func (k *Kvdb) Compact(flags KvdbCompactFlag) error {
	err := C.hse_kvdb_compact(k.impl, C.int(flags))
	if err != 0 {
		return hseErrToErrno(err)
	}

	return nil
}

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
