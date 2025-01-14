// Copyright (c) 2013, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package mem

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/syndtr/goleveldb/leveldb/storage"
)

func TestMemStorage(t *testing.T) {
	m := NewMemStorage()

	l, err := m.Lock()
	if err != nil {
		t.Fatal("storage lock failed(1): ", err)
	}
	_, err = m.Lock()
	if err == nil {
		t.Fatal("expect error for second storage lock attempt")
	} else {
		t.Logf("storage lock got error: %s (expected)", err)
	}
	l.Unlock()
	_, err = m.Lock()
	if err != nil {
		t.Fatal("storage lock failed(2): ", err)
	}

	w, err := m.Create(storage.FileDesc{storage.TypeTable, 1})
	if err != nil {
		t.Fatal("Storage.Create: ", err)
	}
	w.Write([]byte("abc"))
	w.Close()
	if fds, _ := m.List(storage.TypeAll); len(fds) != 1 {
		t.Fatal("invalid GetFiles len")
	}
	buf := new(bytes.Buffer)
	r, err := m.Open(storage.FileDesc{storage.TypeTable, 1})
	if err != nil {
		t.Fatal("Open: got error: ", err)
	}
	buf.ReadFrom(r)
	r.Close()
	if got := buf.String(); got != "abc" {
		t.Fatalf("Read: invalid value, want=abc got=%s", got)
	}
	if _, err := m.Open(storage.FileDesc{storage.TypeTable, 1}); err != nil {
		t.Fatal("Open: got error: ", err)
	}
	if _, err := m.Open(storage.FileDesc{storage.TypeTable, 1}); err == nil {
		t.Fatal("expecting error")
	}
	m.Remove(storage.FileDesc{storage.TypeTable, 1})
	if fds, _ := m.List(storage.TypeAll); len(fds) != 0 {
		t.Fatal("invalid GetFiles len", len(fds))
	}
	if _, err := m.Open(storage.FileDesc{storage.TypeTable, 1}); err == nil {
		t.Fatal("expecting error")
	}
}

func TestMemStorageRename(t *testing.T) {
	fd1 := storage.FileDesc{Type: storage.TypeTable, Num: 1}
	fd2 := storage.FileDesc{Type: storage.TypeTable, Num: 2}

	m := NewMemStorage()
	w, err := m.Create(fd1)
	if err != nil {
		t.Fatalf("Storage.Create: %v", err)
	}

	fmt.Fprintf(w, "abc")
	w.Close()

	rd, err := m.Open(fd1)
	if err != nil {
		t.Fatalf("Storage.Open(%v): %v", fd1, err)
	}
	rd.Close()

	fds, err := m.List(storage.TypeAll)
	if err != nil {
		t.Fatalf("Storage.List: %v", err)
	}
	for _, fd := range fds {
		if !storage.FileDescOk(fd) {
			t.Errorf("Storage.List -> storage.FileDescOk(%q)", fd)
		}
	}

	err = m.Rename(fd1, fd2)
	if err != nil {
		t.Fatalf("Storage.Rename: %v", err)
	}

	rd, err = m.Open(fd2)
	if err != nil {
		t.Fatalf("Storage.Open(%v): %v", fd2, err)
	}
	rd.Close()

	fds, err = m.List(storage.TypeAll)
	if err != nil {
		t.Fatalf("Storage.List: %v", err)
	}
	for _, fd := range fds {
		if !storage.FileDescOk(fd) {
			t.Errorf("Storage.List -> storage.FileDescOk(%q)", fd)
		}
	}
}
