// SPDX-License-Identifier: Apache-2.0
//
// Copyright (C) 2022 Micron Technology, Inc. All rights reserved.

package limits

// Cannot import values from C without throwing away the const
const (
	// KvsCountMax is the maximum number of KVS's contained within one KVDB
	KvsCountMax   = 256
	KvsKLenMax    = 1344
	KvsVLenMax    = 1024 * 1024
	KvsMaxPfxLen  = 32
	KvsNameLenMax = 32
)
