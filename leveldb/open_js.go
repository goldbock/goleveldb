// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

//go:build js
// +build js

package leveldb

import (
	"errors"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/storage/mem"
)

// OpenFile opens or creates a DB for the given path.
// The DB will be created if not exist, unless ErrorIfMissing is true.
// Also, if ErrorIfExist is true and the DB exist OpenFile will returns
// os.ErrExist error.
//
// OpenFile uses standard file-system backed storage implementation as
// described in the leveldb/storage package.
//
// OpenFile will return an error with type of ErrCorrupted if corruption
// detected in the DB. Use errors.IsCorrupted to test whether an error is
// due to corruption. Corrupted DB can be recovered with Recover function.
//
// The returned DB instance is safe for concurrent use.
// The DB must be closed after use, by calling Close method.
func OpenFile(path string, o *opt.Options) (db *DB, err error) {
	return mem.NewMemStorage()
}

// RecoverFile recovers and opens a DB with missing or corrupted manifest files
// for the given path. It will ignore any manifest files, valid or not.
// The DB must already exist or it will returns an error.
// Also, Recover will ignore ErrorIfMissing and ErrorIfExist options.
//
// RecoverFile uses standard file-system backed storage implementation as described
// in the leveldb/storage package.
//
// The returned DB instance is safe for concurrent use.
// The DB must be closed after use, by calling Close method.
func RecoverFile(path string, o *opt.Options) (db *DB, err error) {
	return nil, errors.New("recoverFile not supported in JS environment")
}
