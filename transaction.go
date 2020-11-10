// SPDX-License-Identifier: Apache-2.0
//
// Copyright (C) 2020 Micron Technology, Inc. All rights reserved.

package hse

// #include <hse/hse.h>
import "C"

type KvdbTxnState int

const (
	KvdbTxnValid     KvdbTxnState = C.HSE_KVDB_TXN_INVALID
	KvdbTxnActive    KvdbTxnState = C.HSE_KVDB_TXN_ACTIVE
	KvdbTxnCommitted KvdbTxnState = C.HSE_KVDB_TXN_COMMITTED
	KvdbTxnAborted   KvdbTxnState = C.HSE_KVDB_TXN_ABORTED
)

type KvdbTxn struct {
	impl *C.struct_hse_kvdb_txn
	kvdb *C.struct_hse_kvdb
}

func (t *KvdbTxn) Free() {
	if t.impl == nil {
		return
	}

	C.hse_kvdb_txn_free(t.kvdb, t.impl)

	t.impl = nil
}

func (t *KvdbTxn) Begin() error {
	err := C.hse_kvdb_txn_begin(t.kvdb, t.impl)
	if err != 0 {
		return hseErrToErrno(err)
	}

	return nil
}

func (t *KvdbTxn) Commit() error {
	err := C.hse_kvdb_txn_commit(t.kvdb, t.impl)
	if err != 0 {
		return hseErrToErrno(err)
	}

	return nil
}

func (t *KvdbTxn) Abort() error {
	err := C.hse_kvdb_txn_abort(t.kvdb, t.impl)
	if err != 0 {
		return hseErrToErrno(err)
	}

	return nil
}

func (t *KvdbTxn) State() KvdbTxnState {
	return KvdbTxnState(C.hse_kvdb_txn_get_state(t.kvdb, t.impl))
}
