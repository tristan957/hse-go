// SPDX-License-Identifier: Apache-2.0
//
// Copyright (C) 2020 Micron Technology, Inc. All rights reserved.

package hse

import (
	"testing"
)

func TestGetSet(t *testing.T) {
	p, err := NewParams()
	defer p.Destroy()
	if err != nil {
		t.Fatalf("failed to create params object: %s", err)
	}

	p.Set("kvs.pfx_len", "3")
	value := p.Get("kvs.pfx_len")
	if value != "3" {
		t.Fatalf("failed to get kvs.pfx_len")
	}
}

func TestFromString(t *testing.T) {
	p, err := NewParams()
	defer p.Destroy()
	if err != nil {
		t.Fatalf("failed to create params object: %s", err)
	}

	config := `api_version: 1
kvs:
  pfx_len: 7`

	if err := p.FromString(config); err != nil {
		t.Fatalf("failed to set params from string: %s", err)
	}

	value := p.Get("kvs.pfx_len")
	if value != "7" {
		t.Fatalf("failed to get kvs.pfx_len")
	}
}

func TestFromFile(t *testing.T) {
	p, err := NewParams()
	defer p.Destroy()
	if err != nil {
		t.Fatalf("failed to create params object: %s", err)
	}

	if err := p.FromFile("testdata/config_test.yaml"); err != nil {
		t.Fatalf("failed to set params from file: %s", err)
	}

	value := p.Get("kvs.pfx_len")
	if value != "7" {
		t.Fatalf("failed to get kvs.pfx_len")
	}
}
