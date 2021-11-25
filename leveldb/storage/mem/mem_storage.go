// Copyright (c) 2013, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package mem

import (
	"bytes"
	"os"
	"sync"

	"github.com/syndtr/goleveldb/leveldb/storage"
)

const typeShift = 4

// Verify at compile-time that typeShift is large enough to cover all storage.FileType
// values by confirming that 0 == 0.
var _ [0]struct{} = [storage.TypeAll >> typeShift]struct{}{}

type memStorageLock struct {
	ms *memStorage
}

func (lock *memStorageLock) Unlock() {
	ms := lock.ms
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if ms.slock == lock {
		ms.slock = nil
	}
}

// memStorage is a memory-backed storage.
type memStorage struct {
	mu    sync.Mutex
	slock *memStorageLock
	files map[uint64]*memFile
	meta  storage.FileDesc
}

// NewMemStorage returns a new memory-backed storage implementation.
func NewMemStorage() storage.Storage {
	return &memStorage{
		files: make(map[uint64]*memFile),
	}
}

func (ms *memStorage) Lock() (storage.Locker, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if ms.slock != nil {
		return nil, storage.ErrLocked
	}
	ms.slock = &memStorageLock{ms: ms}
	return ms.slock, nil
}

func (*memStorage) Log(str string) {}

func (ms *memStorage) SetMeta(fd storage.FileDesc) error {
	if !storage.FileDescOk(fd) {
		return storage.ErrInvalidFile
	}

	ms.mu.Lock()
	ms.meta = fd
	ms.mu.Unlock()
	return nil
}

func (ms *memStorage) GetMeta() (storage.FileDesc, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if ms.meta.Zero() {
		return storage.FileDesc{}, os.ErrNotExist
	}
	return ms.meta, nil
}

func (ms *memStorage) List(ft storage.FileType) ([]storage.FileDesc, error) {
	ms.mu.Lock()
	var fds []storage.FileDesc
	for x := range ms.files {
		fd := unpackFile(x)
		if fd.Type&ft != 0 {
			fds = append(fds, fd)
		}
	}
	ms.mu.Unlock()
	return fds, nil
}

func (ms *memStorage) Open(fd storage.FileDesc) (storage.Reader, error) {
	if !storage.FileDescOk(fd) {
		return nil, storage.ErrInvalidFile
	}

	ms.mu.Lock()
	defer ms.mu.Unlock()
	if m, exist := ms.files[packFile(fd)]; exist {
		if m.open {
			return nil, storage.ErrFileOpen
		}
		m.open = true
		return &memReader{Reader: bytes.NewReader(m.Bytes()), ms: ms, m: m}, nil
	}
	return nil, os.ErrNotExist
}

func (ms *memStorage) Create(fd storage.FileDesc) (storage.Writer, error) {
	if !storage.FileDescOk(fd) {
		return nil, storage.ErrInvalidFile
	}

	x := packFile(fd)
	ms.mu.Lock()
	defer ms.mu.Unlock()
	m, exist := ms.files[x]
	if exist {
		if m.open {
			return nil, storage.ErrFileOpen
		}
		m.Reset()
	} else {
		m = &memFile{}
		ms.files[x] = m
	}
	m.open = true
	return &memWriter{memFile: m, ms: ms}, nil
}

func (ms *memStorage) Remove(fd storage.FileDesc) error {
	if !storage.FileDescOk(fd) {
		return storage.ErrInvalidFile
	}

	x := packFile(fd)
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if _, exist := ms.files[x]; exist {
		delete(ms.files, x)
		return nil
	}
	return os.ErrNotExist
}

func (ms *memStorage) Rename(oldfd, newfd storage.FileDesc) error {
	if !storage.FileDescOk(oldfd) || !storage.FileDescOk(newfd) {
		return storage.ErrInvalidFile
	}
	if oldfd == newfd {
		return nil
	}

	oldx := packFile(oldfd)
	newx := packFile(newfd)
	ms.mu.Lock()
	defer ms.mu.Unlock()
	oldm, exist := ms.files[oldx]
	if !exist {
		return os.ErrNotExist
	}
	newm, exist := ms.files[newx]
	if (exist && newm.open) || oldm.open {
		return storage.ErrFileOpen
	}
	delete(ms.files, oldx)
	ms.files[newx] = oldm
	return nil
}

func (*memStorage) Close() error { return nil }

type memFile struct {
	bytes.Buffer
	open bool
}

type memReader struct {
	*bytes.Reader
	ms     *memStorage
	m      *memFile
	closed bool
}

func (mr *memReader) Close() error {
	mr.ms.mu.Lock()
	defer mr.ms.mu.Unlock()
	if mr.closed {
		return storage.ErrClosed
	}
	mr.m.open = false
	return nil
}

type memWriter struct {
	*memFile
	ms     *memStorage
	closed bool
}

func (*memWriter) Sync() error { return nil }

func (mw *memWriter) Close() error {
	mw.ms.mu.Lock()
	defer mw.ms.mu.Unlock()
	if mw.closed {
		return storage.ErrClosed
	}
	mw.memFile.open = false
	return nil
}

func packFile(fd storage.FileDesc) uint64 {
	return uint64(fd.Num)<<typeShift | uint64(fd.Type)
}

func unpackFile(x uint64) storage.FileDesc {
	return storage.FileDesc{storage.FileType(x) & storage.TypeAll, int64(x >> typeShift)}
}
