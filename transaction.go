// SPDX-License-Identifier: Apache-2.0
//
// Copyright (C) 2020 Micron Technology, Inc. All rights reserved.

package hse

/*
#include <hse/hse.h>
*/
import "C"

// TransactionState represents the states a transaction can exist in
type TransactionState int

const (
	// TransactionInvalid is the INVALID state
	TransactionInvalid TransactionState = C.HSE_KVDB_TXN_INVALID
	// TransactionActive is the ACTIVE state
	TransactionActive TransactionState = C.HSE_KVDB_TXN_ACTIVE
	// TransactionCommitted is the COMMITTED state
	TransactionCommitted TransactionState = C.HSE_KVDB_TXN_COMMITTED
	// TransactionAborted is the ABORTED state
	TransactionAborted TransactionState = C.HSE_KVDB_TXN_ABORTED
)

// Transaction represents a context in which multiple operations will be run
//
// The HSE KVDB provides transactions with operations spanning KVSs within a
// single KVDB. These transactions have snapshot isolation (a specific form of
// MVCC) with the normal semantics (see "Concurrency Control and Recovery in
// Database Systems" by PA Bernstein).
//
// One unusual aspect of the API as it relates to transactions is that the data
// object that is used to hold client-level transaction state is allocated
// separately from the transaction being initiated. As a result, the same object
// handle should be reused again and again.
//
// In addition, there is very limited coupling between threading and
// transactions. A single thread may have many transactions in flight
// simultaneously. Also operations within a transaction can be performed by
// multiple threads. The latter mode of operation must currently restrict calls
// so that only one thread is actively performing an operation in the context of
// a particular transaction at any particular time.
//
// The general lifecycle of a transaction is as follows:
//
//                       +----------+
//                       | INVALID  |
//                       +----------+
//                             |
//                             v
//                       +----------+
//     +---------------->|  ACTIVE  |<----------------+
//     |                 +----------+                 |
//     |  +-----------+    |      |     +----------+  |
//     +--| COMMITTED |<---+      +---->| ABORTED  |--+
//        +-----------+                 +----------+
//
// When a transaction is initially allocated, it starts in the INVALID state.
// When Transaction.Begin() is called with transaction in the INVALID,
// COMMITTED, or ABORTED states, it moves to the ACTIVE state. It is an error to
// call the Transation.Begin() function on a transaction in the ACTIVE state.
// For a transaction in the ACTIVE state, only the functions
// Transaction.Commit(), Transaction.Abort), or Transaction.Free() may be called
// (with the last doing an abort prior to the free).
//
// When a transaction becomes ACTIVE, it establishes an ephemeral snapshot view
// of the state of the KVDB. Any data mutations outside of the transaction's
// context after that point are not visible to the transaction. Similarly, any
// mutations performed within the context of the transaction are not visible
// outside of the transaction unless and until it is committed. All such
// mutations become visible atomically.
type Transaction struct {
	impl *C.struct_hse_kvdb_txn
	kvdb *Kvdb
}

// Free frees transaction object
//
// If the transaction handle refers to an ACTIVE transaction, the transaction is
// aborted prior to being freed. This function is thread safe with different
// transactions.
func (t *Transaction) Free() {
	if t.impl == nil {
		return
	}

	C.hse_kvdb_txn_free(t.kvdb.impl, t.impl)

	t.impl = nil
}

// Begin initiates transaction
//
// The call fails if the transaction handle refers to an ACTIVE transaction.
// This function is thread safe with different transactions.
func (t *Transaction) Begin() error {
	err := C.hse_kvdb_txn_begin(t.kvdb.impl, t.impl)
	if err != 0 {
		return hseErrToErrno(err)
	}

	return nil
}

// Commit commits all the mutations of the referenced transaction
//
// The call fails if the referenced transaction is not in the ACTIVE state. This
// function is thread safe with different transactions.
func (t *Transaction) Commit() error {
	err := C.hse_kvdb_txn_commit(t.kvdb.impl, t.impl)
	if err != 0 {
		return hseErrToErrno(err)
	}

	return nil
}

// Abort aborts/rollsback transaction
//
// The call fails if the referenced transaction is not in the ACTIVE state. This
// function is thread safe with different transactions.
func (t *Transaction) Abort() error {
	err := C.hse_kvdb_txn_abort(t.kvdb.impl, t.impl)
	if err != 0 {
		return hseErrToErrno(err)
	}

	return nil
}

// State retrieves the state of the referenced transaction
//
// This function is thread safe with different transactions.
func (t *Transaction) State() TransactionState {
	return TransactionState(C.hse_kvdb_txn_get_state(t.kvdb.impl, t.impl))
}
