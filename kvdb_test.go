/* SPDX-License-Identifier: Apache-2.0 OR MIT
 *
 * SPDX-FileCopyrightText: Copyright 2022 Micron Technology, Inc.
 */

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

type params struct {
	Cparams []string
	Rparams []string
}

func (p *params) SetCparams(params ...string) {
	p.Cparams = params
}

func (p *params) SetRparams(params ...string) {
	p.Rparams = params
}

func makeAndOpenKvs(kvsName string, p params) (k *Kvs) {
	err := kvdb.KvsCreate(kvsName, p.Cparams...)
	if err != nil && err != syscall.EEXIST {
		fmt.Fprintf(os.Stderr, "failed to make kvs: %s\n", err)
		os.Exit(1)
	}
	if err == syscall.EEXIST {
		kvdb.KvsDrop(kvsName)
		kvdb.KvsCreate(kvsName, p.Cparams...)
	}
	kvs, err := kvdb.KvsOpen(kvsName, p.Rparams...)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open kvs: %s\n", err)
		os.Exit(1)
	}

	return kvs
}

func TestMain(t *testing.M) {
	var kvsParams params

	kvsParams.SetCparams("prefix.length=3")

	Init()
	defer Fini()

	if err := KvdbCreate(kvdbName); err != nil && err != syscall.EEXIST {
		fmt.Fprintf(os.Stderr, "failed to make kvdb: %s\n", err)
		os.Exit(1)
	}
	kvdb, err := KvdbOpen(kvdbName, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open kvdb: %s\n", err)
		os.Exit(1)
	}
	defer kvdb.Close()

	kvdbTestKvs = makeAndOpenKvs(kvdbTestKvsName, kvsParams)
	defer kvdbTestKvs.Close()
	defer kvdb.KvsDrop(kvsTestKvsName)

	kvsTestKvs = makeAndOpenKvs(kvsTestKvsName, kvsParams)
	defer kvsTestKvs.Close()
	defer kvdb.KvsDrop(kvsTestKvsName)

	cursorTestKvs = makeAndOpenKvs(cursorTestKvsName, kvsParams)
	defer cursorTestKvs.Close()
	defer kvdb.KvsDrop(cursorTestKvsName)

	names, err := kvdb.KvsNames()
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
	if err := kvdb.Sync(); err != nil {
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
