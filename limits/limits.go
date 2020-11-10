// SPDX-License-Identifier: Apache-2.0
//
// Copyright (C) 2020 Micron Technology, Inc. All rights reserved.

package limits

// No way to reference macros in Go so have to redefine
const (
	// KvsCountMax is the maximum number of KVS's contained within one KVDB
	KvsCountMax   = 256
	KvsKLenMax    = 1344
	KvsVLenMax    = 1024 * 1024
	KvsMaxPfxLen  = 32
	KvsNameLenMax = 32
)
