// SPDX-License-Identifier: Apache-2.0
//
// Copyright (C) 2020 Micron Technology, Inc. All rights reserved.

package hse

/*
#cgo LDFLAGS: -lhse_kvdb
#include <hse/hse.h>
*/
import "C"
import "syscall"

var (
	KvdbVersionString string
	KvdbVersionTag    string
	KvdbVersionSHA    string
)

func init() {
	KvdbVersionString = C.GoString(C.hse_kvdb_version_string())
	KvdbVersionTag = C.GoString(C.hse_kvdb_version_tag())
	KvdbVersionSHA = C.GoString(C.hse_kvdb_version_sha())
}

// hseErrToErrno converts an hse_err_t to an errno
func hseErrToErrno(err C.ulong) error {
	return syscall.Errno(C.hse_err_to_errno(err))
}
