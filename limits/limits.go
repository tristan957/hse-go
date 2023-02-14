/* SPDX-License-Identifier: Apache-2.0 OR MIT
 *
 * SPDX-FileCopyrightText: Copyright 2022 Micron Technology, Inc.
 */

package limits

// #include <hse/limits.h>
import "C"

// Cannot import values from C without throwing away the const
const (
	// KvsCountMax is the maximum number of KVS's contained within one KVDB
	KVS_COUNT_MAX     uint = C.HSE_KVS_COUNT_MAX
	KVS_KEY_LEN_MAX   uint = C.HSE_KVS_KEY_LEN_MAX
	KVS_NAME_LEN_MAX  uint = C.HSE_KVS_NAME_LEN_MAX
	KVS_PFX_LEN_MAX   uint = C.HSE_KVS_PFX_LEN_MAX
	KVS_VALUE_LEN_MAX uint = C.HSE_KVS_VALUE_LEN_MAX
)
