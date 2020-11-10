// SPDX-License-Identifier: Apache-2.0
//
// Copyright (C) 2020 Micron Technology, Inc. All rights reserved.

package hse

import (
	"fmt"
	"io"
	"testing"
)

const (
	cursorTestKvsName = "hse-go-cursor-test-kvs"
)

var cursorTestKvs *Kvs

func TestSeek(t *testing.T) {
	for i := 1; i <= 5; i++ {
		cursorTestKvs.Put([]byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("value%d", i)))
	}

	c, err := cursorTestKvs.CursorCreate(nil)
	if err != nil {
		t.Fatalf("failed to create cursor: %s", err)
	}
	defer c.Destroy()

	found, err := c.Seek([]byte("key3"))
	if err != nil {
		t.Fatalf("failed to seek to key3: %s", err)
	}
	if string(found) != "key3" {
		t.Fatal("found key after seek is not key3")
	}

	key, value, err := c.Read()
	if err != nil && err != io.EOF {
		t.Fatalf("failed to read cursor: %s", err)
	}
	if string(key) != "key3" || string(value) != "value3" {
		t.Fatalf("unexpected key/value pair from read, expected (key3, value3), got (%s, %s)", string(key), string(value))
	}

	c.Read()
	c.Read()
	_, _, err = c.Read()
	if err != io.EOF {
		t.Fatalf("failed to reach end of file: %s", err)
	}

	if err := c.Destroy(); err != nil {
		t.Fatalf("failed to destory cursor: %s", err)
	}
}

func TestSeekWithFilter(t *testing.T) {
	for i := 1; i <= 5; i++ {
		cursorTestKvs.Put([]byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("value%d", i)))
	}

	c, err := cursorTestKvs.CursorCreate([]byte("key"))
	if err != nil {
		t.Fatalf("failed to create cursor: %s", err)
	}
	defer c.Destroy()

	_, err = c.Seek([]byte("key3"))
	if err != nil {
		t.Fatalf("failed to seek to key3: %s", err)
	}

	key, value, err := c.Read()
	if err != nil && err != io.EOF {
		t.Fatalf("failed to read cursor: %s", err)
	}
	if string(key) != "key3" || string(value) != "value3" {
		t.Fatalf("unexpected key/value pair from read, expected (key3, value3), got (%s, %s)", string(key), string(value))
	}

	c.Read()
	c.Read()
	_, _, err = c.Read()
	if err != io.EOF {
		t.Fatalf("failed to reach end of file: %s", err)
	}

	if err := c.Destroy(); err != nil {
		t.Fatalf("failed to destory cursor: %s", err)
	}
}

func TestUpdate(t *testing.T) {
	for i := 1; i <= 5; i++ {
		cursorTestKvs.Put([]byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("value%d", i)))
	}

	c, err := cursorTestKvs.CursorCreate(nil)
	if err != nil {
		t.Fatalf("failed to create cursor: %s", err)
	}
	defer c.Destroy()

	cursorTestKvs.Put([]byte("key6"), []byte("value6"))

	for i := 0; ; i++ {
		_, _, err = c.Read()
		if err != nil && err != io.EOF {
			t.Fatalf("failed to read from cursor: %s", err)
		}
		if err == io.EOF {
			break
		}
	}

	if err = c.Update(); err != nil {
		t.Fatalf("failed to update cursor: %s", err)
	}

	key, value, err := c.Read()
	if err != nil {
		t.Fatalf("failed to read cursor: %s", err)
	}
	if string(key) != "key6" || string(value) != "value6" {
		t.Fatalf("unexpected key/value pair from read, expected (key6, value6), got (%s, %s)", string(key), string(value))
	}

	if err := c.Destroy(); err != nil {
		t.Fatalf("failed to destory cursor: %s", err)
	}
}
