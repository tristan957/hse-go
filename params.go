// SPDX-License-Identifier: Apache-2.0
//
// Copyright (C) 2020 Micron Technology, Inc. All rights reserved.

package hse

// #include <hse/hse.h>
import "C"
import (
	"unsafe"
)

type Params struct {
	impl *C.struct_hse_params
}

func ParamsCreate() (*Params, error) {
	var p Params

	err := C.hse_params_create(&p.impl)
	if err != 0 {
		return nil, hseErrToErrno(err)
	}

	return &p, nil
}

func (p *Params) Destroy() {
	if p.impl == nil {
		return
	}

	C.hse_params_destroy(p.impl)

	p.impl = nil
}

func (p *Params) FromFile(path string) error {
	pathC := C.CString(path)
	defer C.free(unsafe.Pointer(pathC))

	err := C.hse_params_from_file(p.impl, pathC)
	if err != 0 {
		return hseErrToErrno(err)
	}

	return nil
}

func (p *Params) FromString(input string) error {
	inputC := C.CString(input)
	defer C.free(unsafe.Pointer(inputC))

	err := C.hse_params_from_string(p.impl, inputC)
	if err != 0 {
		return hseErrToErrno(err)
	}

	return nil
}

func (p *Params) Get(key string) string {
	keyC := C.CString(key)
	defer C.free(unsafe.Pointer(keyC))

	// 256 is HP_DICT_LEN_MAX from hse_params.c
	buf := C.malloc(C.sizeof_char * 256)
	defer C.free(buf)

	value := C.hse_params_get(p.impl, keyC, (*C.char)(buf), 256, nil)

	return C.GoString(value)
}

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
