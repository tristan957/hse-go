/* SPDX-License-Identifier: Apache-2.0 OR MIT
 *
 * SPDX-FileCopyrightText: Copyright 2022 Micron Technology, Inc.
 */

package hse

import (
	"fmt"
	"testing"
)

const (
	cursorTestKvsName = "cursor-test"
)

var cursorTestKvs *Kvs

func resetCursorTestKvs() {
	for i := 1; i <= 5; i++ {
		cursorTestKvs.Put([]byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("value%d", i)), 0)
	}
}

func TestSeek(t *testing.T) {
	testSeek := func(filter []byte) {
		resetCursorTestKvs()

		c, err := cursorTestKvs.CreateCursor(filter, 0)
		if err != nil {
			t.Fatalf("failed to create cursor: %s", err)
		}
		defer c.Destroy()

		found, err := c.Seek([]byte("key3"), 0)
		if err != nil {
			t.Fatalf("failed to seek to key3: %s", err)
		}
		if string(found) != "key3" {
			t.Fatal("found key after seek is not key3")
		}

		key, value, err := c.Read(0)
		if err != nil {
			t.Fatalf("failed to read cursor: %s", err)
		}
		if string(key) != "key3" || string(value) != "value3" {
			t.Fatalf("unexpected key/value pair from read, expected (key3, value3), got (%s, %s)", string(key), string(value))
		}

		c.Read(0)
		c.Read(0)
		_, _, err = c.Read(0)
		if err != nil {
			t.Fatalf("failed to reach end of file: %s", err)
		}
		if !c.Eof() {
			t.Fatal("failed to reach end of file")
		}

		if err := c.Destroy(); err != nil {
			t.Fatalf("failed to destory cursor: %s", err)
		}
	}

	testSeek(nil)
	testSeek([]byte("key3"))
}

func TestSeekRange(t *testing.T) {
	testSeekRange := func(filter []byte) {
		resetCursorTestKvs()

		c, err := cursorTestKvs.CreateCursor(filter, 0)
		if err != nil {
			t.Fatalf("failed to create cursor: %s", err)
		}
		defer c.Destroy()

		found, err := c.SeekRange([]byte("key0"), []byte("key3"), 0)
		if err != nil {
			t.Fatalf("failed to seek range: %s", err)
		}
		if string(found) != "key0" {
			t.Fatal("found key after seek range is not key0")
		}

		key, value, err := c.Read(0)
		if err != nil {
			t.Fatalf("failed to read cursor: %s", err)
		}
		if string(key) != "key0" || string(value) != "value0" {
			t.Fatalf("unexpected key/value pair from read, expected (key0, value0), got (%s, %s)", string(key), string(value))
		}

		c.Read(0)
		c.Read(0)
		key, value, err = c.Read(0)
		if err != nil {
			t.Fatalf("failed to read cursor: %s", err)
		}
		if string(key) != "key3" || string(value) != "value3" {
			t.Fatalf("unexpected key/value pair from read, expected (key3, value3), got (%s, %s)", string(key), string(value))
		}
		c.Read(0)
		if !c.Eof() {
			t.Fatal("failed to reach end of file")
		}

		if err := c.Destroy(); err != nil {
			t.Fatalf("failed to destory cursor: %s", err)
		}
	}

	testSeekRange(nil)
	testSeekRange([]byte("key"))
}

func TestUpdate(t *testing.T) {
	resetCursorTestKvs()

	c, err := cursorTestKvs.CreateCursor(nil, 0)
	if err != nil {
		t.Fatalf("failed to create cursor: %s", err)
	}
	defer c.Destroy()

	cursorTestKvs.Put([]byte("key6"), []byte("value6"), 0)

	for i := 0; ; i++ {
		_, _, err = c.Read(0)
		if err != nil {
			t.Fatalf("failed to read from cursor: %s", err)
		}
		if c.Eof() {
			break
		}
	}

	if err = c.UpdateView(0); err != nil {
		t.Fatalf("failed to update cursor: %s", err)
	}

	key, value, err := c.Read(0)
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

func TestReverse(t *testing.T) {
	resetCursorTestKvs()

	c, err := cursorTestKvs.CreateCursor(nil, CURSOR_CREATE_REV)
	if err != nil {
		t.Fatalf("failed to create cursor: %s", err)
	}

	for i := 4; i >= 0; i-- {
		key, value, err := c.Read(0)
		if err != nil {
			t.Fatalf("failed to read cursor: %s", err)
		}
		if string(key) != fmt.Sprintf("key%d", i) || string(value) != fmt.Sprintf("value%d", i) {
			t.Fatalf("unexpected key/value pair from read, expected (key%d, value%d), got (%s, %s)", i, i, string(key), string(value))
		}
	}
}

func TestType2(t *testing.T) {
	resetCursorTestKvs()

	txn := kvdb.NewTransaction()

	cursorTestKvs.Put([]byte("key5"), []byte("value5"), 0)
	cursorTestKvs.Put([]byte("key6"), []byte("value6"), &PutOptions{Txn: txn})

	c, err := cursorTestKvs.NewCursor(nil, &CursorOptions{Txn: txn})
	if err != nil {
		t.Fatalf("failed to create cursor: %s", err)
	}

	for i := 0; i < 5; i++ {
		c.Read()
	}
	c.Read()
	if !c.Eof() {
		t.Fatal("failed to reach end of file")
	}
}

func TestUpdateToType2(t *testing.T) {
	resetCursorTestKvs()

	txn := kvdb.NewTransaction()

	cursorTestKvs.Put([]byte("key5"), []byte("value5"), 0)
	cursorTestKvs.Put([]byte("key6"), []byte("value6"), &PutOptions{Txn: txn})

	c, err := cursorTestKvs.CreateCursor(nil, 0)
	if err != nil {
		t.Fatalf("failed to create cursor: %s", err)
	}
	if err = c.UpdateView(0); err != nil {
		t.Fatalf("failed to update cursor: %s", err)
	}

	for i := 0; i < 5; i++ {
		c.Read(0)
	}
	c.Read(0)
	if !c.Eof() {
		t.Fatal("failed to reach end of file")
	}
}

func TestType3(t *testing.T) {
	resetCursorTestKvs()

	txn := kvdb.NewTransaction()

	cursorTestKvs.Put([]byte("key5"), []byte("value5"), 0)
	cursorTestKvs.Put([]byte("key6"), []byte("value6"), &PutOptions{Txn: txn})

	c, err := cursorTestKvs.CreateCursor(nil, &CursorOptions{BindTxn: true, Txn: txn})
	if err != nil {
		t.Fatalf("failed to create cursor: %s", err)
	}

	for i := 0; i < 5; i++ {
		c.Read(0)
	}
	key, value, err := c.Read(0)
	if err != nil {
		t.Fatalf("failed to read cursor: %s", err)
	}
	if string(key) != "key6" || string(value) != "value6" {
		t.Fatalf("unexpected key/value pair from read, expected (key6, value6), got (%s, %s)", string(key), string(value))
	}
	c.Read()
	if !c.Eof() {
		t.Fatal("failed to reach end of file")
	}
}

func TestUpdateToType3(t *testing.T) {
	resetCursorTestKvs()

	txn := kvdb.NewTransaction()

	cursorTestKvs.Put([]byte("key5"), []byte("value5"), 0)
	cursorTestKvs.Put([]byte("key6"), []byte("value6"), &PutOptions{Txn: txn})

	c, err := cursorTestKvs.CreateCursor(nil, 0)
	if err != nil {
		t.Fatalf("failed to create cursor: %s", err)
	}
	if err = c.Update(&CursorOptions{BindTxn: true, Txn: txn}); err != nil {
		t.Fatalf("failed to update cursor: %s", err)
	}

	for i := 0; i < 5; i++ {
		c.Read(0)
	}
	key, value, err := c.Read(0)
	if err != nil {
		t.Fatalf("failed to read cursor: %s", err)
	}
	if string(key) != "key6" || string(value) != "value6" {
		t.Fatalf("unexpected key/value pair from read, expected (key6, value6), got (%s, %s)", string(key), string(value))
	}
	c.Read()
	if !c.Eof() {
		t.Fatal("failed to reach end of file")
	}
}
