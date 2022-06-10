// SPDX-License-Identifier: Apache-2.0
//
// Copyright (C) 2022 Micron Technology, Inc. All rights reserved.

package hse

import "testing"

func TestTransactionStates(t *testing.T) {
	txn := kvdb.NewTransaction()

	if txn.State() != TransactionInvalid {
		t.Fatal("txn state is not valid")
	}

	if err := txn.Begin(); err != nil {
		t.Fatalf("failed to begin txn: %s", err)
	}
	if txn.State() != TransactionActive {
		t.Fatal("txn state is not active")
	}

	if err := txn.Abort(); err != nil {
		t.Fatalf("failed to abort txn: %s", err)
	}
	if txn.State() != TransactionAborted {
		t.Fatal("txn state is not committed")
	}

	if err := txn.Begin(); err != nil {
		t.Fatalf("failed to begin txn: %s", err)
	}
	if txn.State() != TransactionActive {
		t.Fatal("txn state is not active")
	}

	if err := txn.Commit(); err != nil {
		t.Fatalf("failed to commit txn: %s", err)
	}
	if txn.State() != TransactionCommitted {
		t.Fatal("txn state is not committed")
	}
}
