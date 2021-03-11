// SPDX-License-Identifier: Apache-2.0
//
// Copyright (C) 2020 Micron Technology, Inc. All rights reserved.

package hse

/*
#include <hse/hse.h>
*/
import "C"
import "syscall"

var (
	// KvdbVersionString is a human readable version string of HSE
	KvdbVersionString string
	// KvdbVersionTag is the current Git tag of HSE
	KvdbVersionTag string
	// KvdbVersionSHA is the current Git SHA of HSE
	KvdbVersionSHA string
)

func init() {
	KvdbVersionString = C.GoString(C.hse_kvdb_version_string())
	KvdbVersionTag = C.GoString(C.hse_kvdb_version_tag())
	KvdbVersionSHA = C.GoString(C.hse_kvdb_version_sha())
}

// hseErrToErrno converts an hse_err_t to a syscall.Errno
func hseErrToErrno(err C.ulong) error {
	return syscall.Errno(C.hse_err_to_errno(err))
}
