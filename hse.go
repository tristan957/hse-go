// SPDX-License-Identifier: Apache-2.0
//
// Copyright (C) 2022 Micron Technology, Inc. All rights reserved.

package hse

/*
#include <hse/hse.h>
*/
import "C"
import (
	"syscall"
	"unsafe"
)

// VersionString is a string representing the HSE version.
var VersionString string

const (
	// VersionMajor is the major version of HSE.
	VersionMajor uint = C.HSE_VERSION_MAJOR
	// VersionMinor is the minor version of HSE.
	VersionMinor uint = C.HSE_VERSION_MINOR
	// VersionPatch is the Patch version of HSE.
	VersionPatch uint = C.HSE_VERSION_PATCH
)

func init() {
	VersionString = C.GoString(C.CString(C.HSE_VERSION_STRING))
}

type cparams struct {
	buf unsafe.Pointer
	ptr **C.char
	len C.size_t
}

func (p cparams) Ptr() **C.char {
	return p.ptr
}

func (p cparams) Len() C.size_t {
	return p.len
}

func newCParams(params []string) *cparams {
	if params == nil {
		return &cparams{}
	}

	paramsLen := len(params)

	buf := C.malloc(C.size_t(paramsLen) * C.size_t(unsafe.Sizeof(unsafe.Pointer(nil))))
	slice := (*[1 << 30]*C.char)(buf)
	ptr := &slice[0]

	for i, param := range params {
		slice[i] = C.CString(param)
	}

	return &cparams{
		buf: buf,
		ptr: ptr,
		len: C.size_t(paramsLen),
	}
}

func (p *cparams) free() {
	if p.len == 0 {
		return
	}

	slice := (*[1 << 30]*C.char)(p.buf)

	for i := C.size_t(0); i < p.len; i++ {
		C.free(unsafe.Pointer(slice[i]))
	}

	C.free(p.buf)
}

// hseErrToErrno converts an hse_err_t to a syscall.Errno
func hseErrToErrno(err C.ulong) error {
	return syscall.Errno(C.hse_err_to_errno(err))
}

// Init initializes the HSE KVDB subsystem
//
// This function initializes a range of different internal HSE structures. It
// must be called before any other HSE functions are used. It is not thread safe
// and is idempotent.
func Init(config string, params []string) error {
	configC := C.CString(config)
	defer C.free(unsafe.Pointer(configC))

	cparams := newCParams(params)
	defer cparams.free()

	err := C.hse_init(configC, cparams.Len(), cparams.Ptr())

	return hseErrToErrno(err)
}

// Fini shuts down the HSE KVDB subsystem
//
// This function cleanly finalizes a range of different internal HSE structures.
// It should be called prior to application exit and is not thread safe. After
// it is invoked (and even before it returns), calling any other HSE functions
// will result in undefined behavior. This function is not thread safe.
func Fini() {
	C.hse_fini()
}
