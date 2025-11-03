package main

/*
#cgo LDFLAGS: -L${SRCDIR} -Wl,-rpath,${SRCDIR}
#cgo LDFLAGS: -lslatedb_go
#include <stdlib.h>
#include <stdint.h>
*/
import "C"

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"unsafe"

	"github.com/dgraph-io/badger/v4"
	slatedb "slatedb.io/slatedb-go"
)

type kvStore interface {
	Close() error
	Set(key, value []byte) error
	Get(key []byte) ([]byte, error)
	Delete(key []byte) error
	Iterate(prefix []byte, fn func(k, v []byte) error) error
	Sync() error
	Apply(ops []operation) error
}

type operation struct {
	op    byte
	key   []byte
	value []byte
}

var (
	handleMu  sync.RWMutex
	handles           = make(map[uintptr]kvStore)
	nextID    uintptr = 1
	errorMu   sync.Mutex
	lastError string
)

func setError(err error) C.int {
	errorMu.Lock()
	defer errorMu.Unlock()
	if err != nil {
		lastError = err.Error()
		return -1
	}
	lastError = ""
	return 0
}

func storeHandle(store kvStore) uintptr {
	handleMu.Lock()
	defer handleMu.Unlock()
	id := nextID
	nextID++
	handles[id] = store
	return id
}

func getHandle(id uintptr) (kvStore, error) {
	handleMu.RLock()
	defer handleMu.RUnlock()
	store, ok := handles[id]
	if !ok {
		return nil, errors.New("invalid handle")
	}
	return store, nil
}

func deleteHandle(id uintptr) {
	handleMu.Lock()
	defer handleMu.Unlock()
	delete(handles, id)
}

//export Open
func Open(path *C.char, inMemory C.int) C.uintptr_t {
	store, err := openStore(C.GoString(path), inMemory != 0)
	if err != nil {
		setError(err)
		return 0
	}

	setError(nil)
	return C.uintptr_t(storeHandle(store))
}

type badgerStore struct {
	db *badger.DB
}

func (s *badgerStore) Close() error { return s.db.Close() }

func (s *badgerStore) Set(key, value []byte) error {
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, value)
	})
}

func (s *badgerStore) Get(key []byte) ([]byte, error) {
	var result []byte
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			result = append([]byte(nil), val...)
			return nil
		})
	})
	return result, err
}

func (s *badgerStore) Delete(key []byte) error {
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})
}

func (s *badgerStore) Iterate(prefix []byte, fn func(k, v []byte) error) error {
	return s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		it := txn.NewIterator(opts)
		defer it.Close()

		doIter := func(item *badger.Item) error {
			key := item.KeyCopy(nil)
			return item.Value(func(val []byte) error {
				return fn(key, append([]byte(nil), val...))
			})
		}

		if len(prefix) == 0 {
			for it.Rewind(); it.Valid(); it.Next() {
				if err := doIter(it.Item()); err != nil {
					return err
				}
			}
			return nil
		}

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			if err := doIter(it.Item()); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *badgerStore) Sync() error { return s.db.Sync() }

func (s *badgerStore) Apply(ops []operation) error {
	return s.db.Update(func(txn *badger.Txn) error {
		for _, op := range ops {
			switch op.op {
			case 0:
				if err := txn.Set(op.key, op.value); err != nil {
					return err
				}
			case 1:
				if err := txn.Delete(op.key); err != nil {
					if errors.Is(err, badger.ErrKeyNotFound) {
						continue
					}
					return err
				}
			default:
				return errors.New("unknown operation code")
			}
		}
		return nil
	})
}

type slateStore struct {
	db *slatedb.DB
}

func (s *slateStore) Close() error { return s.db.Close() }

func (s *slateStore) Set(key, value []byte) error {
	return s.db.Put(key, value)
}

func (s *slateStore) Get(key []byte) ([]byte, error) {
	value, err := s.db.Get(key)
	if err != nil {
		return nil, err
	}
	return value, nil
}

func (s *slateStore) Delete(key []byte) error {
	return s.db.Delete(key)
}

func (s *slateStore) Iterate(prefix []byte, fn func(k, v []byte) error) error {
	start, end := prefixRange(prefix)
	iter, err := s.db.Scan(start, end)
	if err != nil {
		return err
	}
	defer iter.Close()

	for {
		kv, err := iter.Next()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if len(prefix) > 0 && !bytes.HasPrefix(kv.Key, prefix) {
			continue
		}
		if err := fn(append([]byte(nil), kv.Key...), append([]byte(nil), kv.Value...)); err != nil {
			return err
		}
	}
}

func (s *slateStore) Sync() error { return s.db.Flush() }

func (s *slateStore) Apply(ops []operation) error {
	batch, err := slatedb.NewWriteBatch()
	if err != nil {
		return err
	}
	defer batch.Close()

	for _, op := range ops {
		switch op.op {
		case 0:
			if err := batch.Put(op.key, op.value); err != nil {
				return err
			}
		case 1:
			if err := batch.Delete(op.key); err != nil {
				return err
			}
		default:
			return errors.New("unknown operation code")
		}
	}

	return s.db.Write(batch)
}

type slateOpenConfig struct {
	Path  string               `json:"path"`
	Store *slatedb.StoreConfig `json:"store,omitempty"`
}

func openStore(path string, inMemory bool) (kvStore, error) {
	trimmed := strings.TrimSpace(path)
	if strings.HasPrefix(strings.ToLower(trimmed), "slatedb:") {
		return openSlate(trimmed)
	}
	return openBadger(trimmed, inMemory)
}

func openBadger(path string, inMemory bool) (kvStore, error) {
	if !inMemory && path == "" {
		path = defaultDataDir("badger")
	}

	var opts badger.Options
	if inMemory || path == "" {
		opts = badger.DefaultOptions("").WithInMemory(true)
	} else {
		if err := os.MkdirAll(path, 0o755); err != nil {
			return nil, err
		}
		opts = badger.DefaultOptions(path)
	}

	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	return &badgerStore{db: db}, nil
}

func openSlate(raw string) (kvStore, error) {
	configPart := strings.TrimSpace(strings.TrimPrefix(raw, "slatedb:"))
	if strings.HasPrefix(configPart, "//") {
		configPart = configPart[2:]
	}

	var cfg slateOpenConfig
	switch {
	case configPart == "":
		cfg.Path = defaultDataDir("slatedb")
	case strings.HasPrefix(strings.TrimSpace(configPart), "{"):
		if err := json.Unmarshal([]byte(configPart), &cfg); err != nil {
			return nil, err
		}
	default:
		cfg.Path = configPart
	}

	if cfg.Path == "" {
		cfg.Path = defaultDataDir("slatedb")
	}

	if err := os.MkdirAll(cfg.Path, 0o755); err != nil {
		return nil, err
	}

	storeCfg := cfg.Store
	if storeCfg == nil {
		storeCfg = &slatedb.StoreConfig{Provider: slatedb.ProviderLocal}
	} else if storeCfg.Provider == "" {
		storeCfg.Provider = slatedb.ProviderLocal
	}

	db, err := slatedb.Open(cfg.Path, storeCfg, nil)
	if err != nil {
		return nil, err
	}
	return &slateStore{db: db}, nil
}

func defaultDataDir(name string) string {
	if name == "" {
		name = "data"
	}
	return filepath.Join("data", name)
}

//export Close
func Close(handle C.uintptr_t) C.int {
	db, err := getHandle(uintptr(handle))
	if err != nil {
		return setError(err)
	}
	if err := db.Close(); err != nil {
		return setError(err)
	}
	deleteHandle(uintptr(handle))
	return setError(nil)
}

//export Set
func Set(handle C.uintptr_t, key *C.char, keyLen C.int, value *C.char, valueLen C.int) C.int {
	store, err := getHandle(uintptr(handle))
	if err != nil {
		return setError(err)
	}
	gotKey := C.GoBytes(unsafe.Pointer(key), keyLen)
	gotValue := C.GoBytes(unsafe.Pointer(value), valueLen)
	err = store.Set(gotKey, gotValue)
	return setError(err)
}

//export Get
func Get(handle C.uintptr_t, key *C.char, keyLen C.int, valueLen *C.int) *C.char {
	store, err := getHandle(uintptr(handle))
	if err != nil {
		setError(err)
		return nil
	}
	gotKey := C.GoBytes(unsafe.Pointer(key), keyLen)

	data, err := store.Get(gotKey)
	if err != nil {
		setError(err)
		return nil
	}

	size := len(data)
	if size == 0 {
		buf := C.malloc(1)
		if buf == nil {
			setError(errors.New("malloc failed"))
			return nil
		}
		*valueLen = 0
		setError(nil)
		return (*C.char)(buf)
	}

	buf := C.malloc(C.size_t(size))
	if buf == nil {
		setError(errors.New("malloc failed"))
		return nil
	}

	copy(((*[1 << 30]byte)(unsafe.Pointer(buf)))[:size:size], data)
	*valueLen = C.int(size)
	setError(nil)
	return (*C.char)(buf)
}

//export Delete
func Delete(handle C.uintptr_t, key *C.char, keyLen C.int) C.int {
	store, err := getHandle(uintptr(handle))
	if err != nil {
		return setError(err)
	}
	gotKey := C.GoBytes(unsafe.Pointer(key), keyLen)
	err = store.Delete(gotKey)
	return setError(err)
}

//export Sync
func Sync(handle C.uintptr_t) C.int {
	store, err := getHandle(uintptr(handle))
	if err != nil {
		return setError(err)
	}
	return setError(store.Sync())
}

//export Scan
func Scan(handle C.uintptr_t, prefix *C.char, prefixLen C.int, resultLen *C.int) *C.char {
	store, err := getHandle(uintptr(handle))
	if err != nil {
		setError(err)
		return nil
	}

	var pref []byte
	if prefixLen > 0 {
		pref = C.GoBytes(unsafe.Pointer(prefix), prefixLen)
	}

	var buffer []byte
	err = store.Iterate(pref, func(k, v []byte) error {
		buffer = appendEntry(buffer, k, v)
		return nil
	})
	if err != nil {
		setError(err)
		return nil
	}

	if len(buffer) == 0 {
		*resultLen = 0
		setError(nil)
		return nil
	}

	mem := C.malloc(C.size_t(len(buffer)))
	if mem == nil {
		setError(errors.New("malloc failed"))
		return nil
	}

	copy(((*[1 << 30]byte)(unsafe.Pointer(mem)))[:len(buffer):len(buffer)], buffer)
	*resultLen = C.int(len(buffer))
	setError(nil)
	return (*C.char)(mem)
}

func appendEntry(buf []byte, key, value []byte) []byte {
	var tmp [4]byte
	binary.LittleEndian.PutUint32(tmp[:], uint32(len(key)))
	buf = append(buf, tmp[:]...)
	binary.LittleEndian.PutUint32(tmp[:], uint32(len(value)))
	buf = append(buf, tmp[:]...)
	buf = append(buf, key...)
	buf = append(buf, value...)
	return buf
}

func decodeOperations(data []byte) ([]operation, error) {
	var ops []operation
	offset := 0
	for offset < len(data) {
		op := data[offset]
		offset++

		if offset+4 > len(data) {
			return nil, errors.New("malformed operation key length")
		}
		keyLen := binary.LittleEndian.Uint32(data[offset : offset+4])
		offset += 4
		if offset+int(keyLen) > len(data) {
			return nil, errors.New("malformed operation key")
		}
		key := append([]byte(nil), data[offset:offset+int(keyLen)]...)
		offset += int(keyLen)

		switch op {
		case 0:
			if offset+4 > len(data) {
				return nil, errors.New("malformed operation value length")
			}
			valLen := binary.LittleEndian.Uint32(data[offset : offset+4])
			offset += 4
			if offset+int(valLen) > len(data) {
				return nil, errors.New("malformed operation value")
			}
			value := append([]byte(nil), data[offset:offset+int(valLen)]...)
			offset += int(valLen)
			ops = append(ops, operation{op: op, key: key, value: value})
		case 1:
			ops = append(ops, operation{op: op, key: key})
		default:
			return nil, errors.New("unknown operation code")
		}
	}
	return ops, nil
}

func prefixRange(prefix []byte) ([]byte, []byte) {
	if len(prefix) == 0 {
		return nil, nil
	}
	start := append([]byte(nil), prefix...)
	end := nextPrefix(prefix)
	return start, end
}

func nextPrefix(prefix []byte) []byte {
	end := append([]byte(nil), prefix...)
	for i := len(end) - 1; i >= 0; i-- {
		end[i]++
		if end[i] != 0 {
			return end
		}
	}
	return nil
}

//export Apply
func Apply(handle C.uintptr_t, ops *C.char, opsLen C.int) C.int {
	store, err := getHandle(uintptr(handle))
	if err != nil {
		return setError(err)
	}

	data := C.GoBytes(unsafe.Pointer(ops), opsLen)
	decoded, err := decodeOperations(data)
	if err != nil {
		return setError(err)
	}

	return setError(store.Apply(decoded))
}

//export LastError
func LastError() *C.char {
	errorMu.Lock()
	defer errorMu.Unlock()
	if lastError == "" {
		return nil
	}
	return C.CString(lastError)
}

//export FreeCString
func FreeCString(str *C.char) {
	if str != nil {
		C.free(unsafe.Pointer(str))
	}
}

//export FreeBuffer
func FreeBuffer(buf *C.char) {
	if buf != nil {
		C.free(unsafe.Pointer(buf))
	}
}

func main() {}
