// SPDX-License-Identifier: Apache-2.0
//
// Copyright (C) 2020 Micron Technology, Inc. All rights reserved.

package hse

import "testing"

func TestTxnStates(t *testing.T) {
	txn := kvdb.TxnAlloc()

	if txn.State() != KvdbTxnValid {
		t.Fatal("txn state is not valid")
	}

	if err := txn.Begin(); err != nil {
		t.Fatalf("failed to begin txn: %s", err)
	}
	if txn.State() != KvdbTxnActive {
		t.Fatal("txn state is not active")
	}

	if err := txn.Abort(); err != nil {
		t.Fatalf("failed to abort txn: %s", err)
	}
	if txn.State() != KvdbTxnAborted {
		t.Fatal("txn state is not committed")
	}

	if err := txn.Begin(); err != nil {
		t.Fatalf("failed to begin txn: %s", err)
	}
	if txn.State() != KvdbTxnActive {
		t.Fatal("txn state is not active")
	}

	if err := txn.Commit(); err != nil {
		t.Fatalf("failed to commit txn: %s", err)
	}
	if txn.State() != KvdbTxnCommitted {
		t.Fatal("txn state is not committed")
	}
}
