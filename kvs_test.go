/* SPDX-License-Identifier: Apache-2.0 OR MIT
 *
 * SPDX-FileCopyrightText: Copyright 2022 Micron Technology, Inc.
 */

package hse

import (
	"testing"
)

const (
	kvsTestKvsName = "hse-go-kvs-test-kvs"
)

var kvsTestKvs *Kvs

func TestKvsKeyOperations(t *testing.T) {
	if err := kvsTestKvs.Put([]byte("key"), []byte("value"), 0); err != nil {
		t.Fatalf("failed to put key: %s", err)
	}

	value, _, err := kvsTestKvs.Get([]byte("key"), 0)
	if err != nil {
		t.Fatalf("failed to get key: %s", err)
	}

	if string(value) != "value" {
		t.Fatalf("value that was retrieved does not match what was inserted (%s)", value)
	}

	if err = kvsTestKvs.Delete([]byte("key"), 0); err != nil {
		t.Fatalf("failed to delete key: %s", err)
	}

	kvsTestKvs.Put([]byte("key1"), []byte("value1"), 0)
	kvsTestKvs.Put([]byte("key2"), []byte("value2"), 0)

	if err = kvsTestKvs.PrefixDelete([]byte("key"), 0); err != nil {
		t.Fatalf("failed to delete key* prefix: %s", err)
	}

	value, _, err = kvsTestKvs.Get([]byte("key1"), 0)
	if err != nil {
		t.Fatalf("failed to get key1: %s", err)
	}
	if value != nil {
		t.Fatalf("value1 was not deleted in prefix delete: %s", err)
	}

	value, _, err = kvsTestKvs.Get([]byte("key2"), 0)
	if err != nil {
		t.Fatalf("failed to get key2: %s", err)
	}
	if value != nil {
		t.Fatalf("value2 was not deleted in prefix delete: %s", err)
	}
}

func TestPrefixDelete(t *testing.T) {

}
