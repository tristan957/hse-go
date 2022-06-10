// SPDX-License-Identifier: Apache-2.0
//
// Copyright (C) 2022 Micron Technology, Inc. All rights reserved.

package hse

import (
	"fmt"
	"os"
	"syscall"
	"testing"
)

const (
	kvdbName        = "hse-go-test"
	kvdbTestKvsName = "kvdb-test"
)

var kvdb *Kvdb
var kvdbTestKvs *Kvs

func makeAndOpenKvs(kvsName string, p *Params) (k *Kvs) {
	err := kvdb.KvsMake(kvsName, p)
	if err != nil && err != syscall.EEXIST {
		fmt.Fprintf(os.Stderr, "failed to make kvs: %s\n", err)
		os.Exit(1)
	}
	if err == syscall.EEXIST {
		kvdb.KvsDrop(kvsName)
		kvdb.KvsMake(kvsName, p)
	}
	kvs, err := kvdb.KvsOpen(kvsName, p)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open kvs: %s\n", err)
		os.Exit(1)
	}

	return kvs
}

func TestMain(t *testing.M) {
	Init()
	defer Fini()

	p, err := NewParams()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create params: %s\n", err)
		os.Exit(1)
	}

	p.Set("kvs.pfx_len", "3")

	if err := KvdbMake(kvdbName, nil); err != nil && err != syscall.EEXIST {
		fmt.Fprintf(os.Stderr, "failed to make kvdb: %s\n", err)
		os.Exit(1)
	}
	kvdb, err = KvdbOpen(kvdbName, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open kvdb: %s\n", err)
		os.Exit(1)
	}
	defer kvdb.Close()

	kvdbTestKvs = makeAndOpenKvs(kvdbTestKvsName, p)
	defer kvdbTestKvs.Close()
	defer kvdb.KvsDrop(kvsTestKvsName)

	kvsTestKvs = makeAndOpenKvs(kvsTestKvsName, p)
	defer kvsTestKvs.Close()
	defer kvdb.KvsDrop(kvsTestKvsName)

	cursorTestKvs = makeAndOpenKvs(cursorTestKvsName, p)
	defer cursorTestKvs.Close()
	defer kvdb.KvsDrop(cursorTestKvsName)

	names, err := kvdb.Names()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to get kvs names: %s\n", err)
	}
	if len(names) != 3 {
		fmt.Fprintf(os.Stderr, "incorrect number of kvs names: %d\n", len(names))
	}

	rc := t.Run()

	os.Exit(rc)
}

func TestSync(t *testing.T) {
	if err := kvdb.Sync(); err != nil {
		t.Fatalf("failed to sync kvdb: %s", err)
	}
}

func TestFlush(t *testing.T) {
	if err := kvdb.Flush(); err != nil {
		t.Fatalf("failed to flush kvdb: %s", err)
	}
}

// func TestCompact(t *testing.T) {
// 	for i := 0; i < 5000; i++ {
// 		data := []byte(strconv.FormatInt(0, 2))
// 		kvdbTestKvs.Put(data, data)
// 	}

// 	if err := kvdb.Compact(CompactSampLwm); err != nil {
// 		t.Fatalf("failed to compact kvdb: %s", err)
// 	}

// 	status, err := kvdb.CompactStatus()
// 	if err != nil {
// 		t.Fatalf("failed to get compact status: %s", err)
// 	}
// 	if !status.Active {
// 		t.Fatal("compaction not active")
// 	}

// 	if err = kvdb.Compact(CompactCancel); err != nil {
// 		t.Fatalf("failed to cancel kvdb compaction: %s", err)
// 	}

// 	status, err = kvdb.CompactStatus()
// 	if err != nil {
// 		t.Fatalf("failed to get compact status: %s", err)
// 	}
// 	if status.Canceled {
// 		t.Fatal("compaction not canceled")
// 	}
// }
