// SPDX-License-Identifier: Apache-2.0
//
// Copyright (C) 2020 Micron Technology, Inc. All rights reserved.

package hse

/*
#include <hse/hse.h>
*/
import "C"
import (
	"unsafe"
)

// Params represent Kvdb/Kvs configuration parameters
type Params struct {
	impl *C.struct_hse_params
}

// NewParams creates a Params object
//
// This function allocates an empty params object. This object can then be
// populated through Params.FromFile(), Params.FromString(), or via
// Params.set(). Usage of a given params object is not thread safe.
func NewParams() (*Params, error) {
	var p Params

	err := C.hse_params_create(&p.impl)
	if err != 0 {
		return nil, hseErrToErrno(err)
	}

	return &p, nil
}

// Destroy destroys a Params object
//
// This function frees a params object, whether empty or populated. After it is
// destroyed it may no longer be used.
func (p *Params) Destroy() {
	if p.impl == nil {
		return
	}

	C.hse_params_destroy(p.impl)

	p.impl = nil
}

// FromFile parses params from a file
//
// This function takes a filename and parses it, populating the supplied params
// object. If the file is not a valid params specification, the parsing will
// fail. Client applications can use the experimental function
// hse_params_err_exp() to get more information as to what problem occurred in
// processing the file. This function is not thread safe.
func (p *Params) FromFile(path string) error {
	pathC := C.CString(path)
	defer C.free(unsafe.Pointer(pathC))

	err := C.hse_params_from_file(p.impl, pathC)
	if err != 0 {
		return hseErrToErrno(err)
	}

	return nil
}

// FromString parses params from a string
//
// This function takes a string and parses it as YAML, populating the supplied
// params object. If the string is not a valid params specification, the parsing
// will fail. Client applications can use the experimental function
// hse_params_err_exp() to get more information as to what problem occurred in
// processing the string. This function is not thread safe.
func (p *Params) FromString(input string) error {
	inputC := C.CString(input)
	defer C.free(unsafe.Pointer(inputC))

	err := C.hse_params_from_string(p.impl, inputC)
	if err != 0 {
		return hseErrToErrno(err)
	}

	return nil
}

// Get gets configuration parameter
//
// Obtain the value the parameter denoted by "key" is set to in the params
// object. If the key is valid, then at most "buf_len"-1 bytes of the parameter
// setting will be copied into "buf". If "param_len" is non-NULL, then on return
// the referent will contain the length of the parameter value. This function is
// not thread safe.
func (p *Params) Get(key string) string {
	keyC := C.CString(key)
	defer C.free(unsafe.Pointer(keyC))

	// 256 is HP_DICT_LEN_MAX from hse_params.c
	buf := C.malloc(C.sizeof_char * 256)
	defer C.free(buf)

	value := C.hse_params_get(p.impl, keyC, (*C.char)(buf), 256, nil)

	return C.GoString(value)
}

// Set sets configuration parameter
//
// Set the parameter setting given by "key" to "value". If the "key" or "value"
// is invalid then the call will fail. Client applications can use the
// experimental function hse_params_err_exp() to get more information about what
// problem occurred.
//
// The following syntax is supported for keys:
//
//   kvdb.<param>           # param is set for the KVDB
//   kvs.<param>            # param is set for all KVSs in the KVDB
//   kvs.<kvs_name>.<param> # param is set for the named KVS
//
// This function is not thread safe.
func (p *Params) Set(key string, value string) error {
	keyC := C.CString(key)
	defer C.free(unsafe.Pointer(keyC))
	valueC := C.CString(value)
	defer C.free(unsafe.Pointer(valueC))

	err := C.hse_params_set(p.impl, keyC, valueC)
	if err != 0 {
		return hseErrToErrno(err)
	}

	return nil
}
