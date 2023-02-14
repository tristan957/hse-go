<!--
SPDX-License-Identifier: Apache-2.0 OR MIT

SPDX-FileCopyrightText: Copyright 2022 Micron Technology, Inc.
-->

# HSE Go Bindings

[Heterogeneous-Memory Storage Engine](https://github.com/hse-project/hse)
bindings for Go.

## Dependencies

* HSE, cloned locally and built, or installed on your system including the
development package

## Installation

```shell
go get github.com/hse-project/hse-go
```

### From Build

```shell
go install
```

## Building

If you need to point Cython toward the HSE include directory or the shared
library, you can use `CGO_CFLAGS` and `CGO_LDFLAGS` respectively.

```shell
go build
# or
CGO_CFLAGS="-Ipath/to/include" CGO_LDFLAGS="-Lpath/to/search" go build
```

## Testing

Make sure you create an `mpool` called `hse-go-test` before running the tests.

```shell
go test
```
