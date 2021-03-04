// SPDX-License-Identifier: Apache-2.0
//
// Copyright (C) 2020 Micron Technology, Inc. All rights reserved.

package hse

import (
	"testing"
)

const (
	kvsTestKvsName = "hse-go-kvs-test-kvs"
)

var kvsTestKvs *Kvs

func TestKvsKeyOperations(t *testing.T) {
	if err := kvsTestKvs.Put([]byte("key"), []byte("value"), nil); err != nil {
		t.Fatalf("failed to put key: %s", err)
	}

	value, _, err := kvsTestKvs.Get([]byte("key"), nil)
	if err != nil {
		t.Fatalf("failed to get key: %s", err)
	}

	if string(value) != "value" {
		t.Fatalf("value that was retrieved does not match what was inserted (%s)", value)
	}

	if err = kvsTestKvs.Delete([]byte("key"), nil); err != nil {
		t.Fatalf("failed to delete key: %s", err)
	}

	kvsTestKvs.Put([]byte("key1"), []byte("value1"), nil)
	kvsTestKvs.Put([]byte("key2"), []byte("value2"), nil)

	if _, err = kvsTestKvs.PrefixDelete([]byte("key"), nil); err != nil {
		t.Fatalf("failed to delete key* prefix: %s", err)
	}

	value, _, err = kvsTestKvs.Get([]byte("key1"), nil)
	if err != nil {
		t.Fatalf("failed to get key1: %s", err)
	}
	if value != nil {
		t.Fatalf("value1 was not deleted in prefix delete: %s", err)
	}

	value, _, err = kvsTestKvs.Get([]byte("key2"), nil)
	if err != nil {
		t.Fatalf("failed to get key2: %s", err)
	}
	if value != nil {
		t.Fatalf("value2 was not deleted in prefix delete: %s", err)
	}
}

func TestPrefixDelete(t *testing.T) {

}
