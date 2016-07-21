// Copyright 2016 Netflix, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package lmdbh

import (
	"encoding/binary"
	"log"
	"os"
	"sync"
	"time"

	"github.com/bmatsuo/lmdb-go/lmdb"
	"github.com/netflix/rend/common"
	"github.com/netflix/rend/handlers"
)

type entry struct {
	exptime uint32
	flags   uint32
	data    []byte
}

func (e entry) expired() bool {
	return e.exptime != 0 && e.exptime < uint32(time.Now().Unix())
}

func entryToBuf(e entry) []byte {
	// If this changes, make sure to update the GAT function below
	// The GAT function directly overwrites the exptime field
	buf := make([]byte, 8+len(e.data))
	binary.BigEndian.PutUint32(buf[0:4], e.exptime)
	binary.BigEndian.PutUint32(buf[4:8], e.flags)
	copy(buf[8:], e.data)
	return buf
}

func bufToEntry(b []byte) entry {
	e := entry{
		exptime: binary.BigEndian.Uint32(b[0:4]),
		flags:   binary.BigEndian.Uint32(b[4:8]),
		data:    make([]byte, len(b)-8),
	}

	copy(e.data, b[8:])

	return e
}

func decode(err error) error {
	if err == nil {
		return err
	}

	if oe, ok := err.(*lmdb.OpError); ok {
		switch oe.Errno {
		case lmdb.KeyExist: //MDB_KEYEXIST
			return common.ErrKeyExists
		case lmdb.NotFound: //MDB_NOTFOUND
			return common.ErrKeyNotFound
		//case lmdb.PageNotFound: //MDB_PAGE_NOTFOUND
		//case lmdb.Corrupted: //MDB_CORRUPTED
		//case lmdb.Panic: //MDB_PANIC
		//case lmdb.VersionMismatch: //MDB_VERSION_MISMATCH
		//case lmdb.Invalid: //MDB_INVALID
		//case lmdb.MapFull: //MDB_MAP_FULL
		//case lmdb.DBsFull: //MDB_DBS_FULL
		//case lmdb.ReadersFull: //MDB_READERS_FULL
		//case lmdb.TLSFull: //MDB_TLS_FULL
		//case lmdb.TxnFull: //MDB_TXN_FULL
		//case lmdb.CursorFull: //MDB_CURSOR_FULL
		//case lmdb.PageFull: //MDB_PAGE_FULL
		//case lmdb.MapResized: //MDB_MAP_RESIZED
		//case lmdb.Incompatible: //MDB_INCOMPATIBLE
		//case lmdb.BadRSlot: //MDB_BAD_RSLOT
		//case lmdb.BadTxn: //MDB_BAD_TXN
		//case lmdb.BadValSize: //MDB_BAD_VALSIZE
		//case lmdb.BadDBI: //MDB_BAD_DBI
		// not sure is these should go here or if the return could be these
		//case syscall.EINVAL:
		//case syscall.EACCES:
		default:
			log.Println(err.Error)
		}
	}

	return err
}

type Handler struct {
	env *lmdb.Env
	dbi lmdb.DBI
}

var once = &sync.Once{}
var singleton *Handler

func reaper(env *lmdb.Env, dbi lmdb.DBI) {
	for {
		<-time.After(30 * time.Second)
		start := time.Now()
		log.Printf("[REAPER] Reaper started at %v\n", start)

		err := env.View(func(txn *lmdb.Txn) error {
			txn.RawRead = true
			cur, err := txn.OpenCursor(dbi)
			if err != nil {
				return err
			}

			stats, err := txn.Stat(dbi)
			if err != nil {
				return err
			}
			log.Printf("[REAPER] Items before: %d", stats.Entries)

			// reap all items whose TTL has passed
			for {
				key, buf, err := cur.Get(nil, nil, lmdb.Next)
				if err != nil {
					if lmdb.IsNotFound(err) {
						break
					}
					return err
				}

				e := entry{
					exptime: binary.BigEndian.Uint32(buf[0:4]),
				}
				if e.expired() {
					// Mini update transaction here to avoid blocking other writers
					err = env.Update(func(t *lmdb.Txn) error {
						// double check the expire time after getting txn lock
						buf, err := t.Get(dbi, key)
						if err != nil {
							return err
						}
						e := entry{
							exptime: binary.BigEndian.Uint32(buf[0:4]),
						}
						if e.expired() {
							return t.Del(dbi, key, nil)
						}
						return nil
					})

					if de := decode(err); de != nil && de != common.ErrKeyNotFound {
						return err
					}
				}
			}

			return nil
		})

		if err != nil {
			log.Printf("[REAPER] Error while reaping: %v\n", err.Error())
		}

		err = env.View(func(txn *lmdb.Txn) error {
			stats, err := txn.Stat(dbi)
			if err != nil {
				return err
			}
			log.Printf("[REAPER] Items after: %d\n", stats.Entries)
			return nil
		})

		if err != nil {
			log.Printf("[REAPER] Error while reaping: %v\n", err.Error())
		}

		end := time.Now()
		durms := float64(end.UnixNano()-start.UnixNano()) / 1000000.0
		log.Printf("[REAPER] Reaper ended at %v and took %vms to run\n", end, durms)
	}
}

func New(path string, size int64) handlers.HandlerConst {
	return func() (handlers.Handler, error) {
		once.Do(func() {
			// initialize the LMDB environment and DB
			env, err := lmdb.NewEnv()
			if err != nil {
				panic(err)
			}

			// apply size limit, one DB only
			if err := env.SetMapSize(size); err != nil {
				panic(err)
			}
			if err := env.SetMaxDBs(1); err != nil {
				panic(err)
			}

			// Create the db dir if it doesn't already exist
			fs, err := os.Stat(path)
			if err != nil {
				if os.IsNotExist(err) {
					if err := os.MkdirAll(path, 0774); err != nil {
						panic(err)
					}
				} else {
					panic(err)
				}
			}

			// Don't correct for a file already existing, let the user deal with it.
			if fs != nil && !fs.IsDir() {
				panic("Rend LMDB path exists and is a file")
			}

			if err := env.Open(path, 0, 0664); err != nil {
				panic(err)
			}

			var dbi lmdb.DBI
			err = env.Update(func(txn *lmdb.Txn) (err error) {
				dbi, err = txn.CreateDBI("rendb")
				return
			})
			if err != nil {
				panic(err)
			}

			singleton = &Handler{
				env: env,
				dbi: dbi,
			}

			go reaper(env, dbi)
		})

		return singleton, nil
	}
}

func (h *Handler) Set(cmd common.SetRequest) error {
	var exptime uint32
	if cmd.Exptime > 0 {
		exptime = uint32(time.Now().Unix()) + cmd.Exptime
	}
	e := entry{
		exptime: exptime,
		flags:   cmd.Flags,
		data:    cmd.Data,
	}

	buf := entryToBuf(e)

	err := h.env.Update(func(txn *lmdb.Txn) error {
		return txn.Put(h.dbi, cmd.Key, buf, 0)
	})

	return decode(err)
}

func (h *Handler) Add(cmd common.SetRequest) error {
	var exptime uint32
	if cmd.Exptime > 0 {
		exptime = uint32(time.Now().Unix()) + cmd.Exptime
	}
	e := entry{
		exptime: exptime,
		flags:   cmd.Flags,
		data:    cmd.Data,
	}

	buf := entryToBuf(e)

	err := h.env.Update(func(txn *lmdb.Txn) error {
		return txn.Put(h.dbi, cmd.Key, buf, lmdb.NoOverwrite)
	})

	return decode(err)
}

func (h *Handler) Replace(cmd common.SetRequest) error {
	var exptime uint32
	if cmd.Exptime > 0 {
		exptime = uint32(time.Now().Unix()) + cmd.Exptime
	}
	e := entry{
		exptime: exptime,
		flags:   cmd.Flags,
		data:    cmd.Data,
	}

	buf := entryToBuf(e)

	err := h.env.Update(func(txn *lmdb.Txn) error {
		if _, err := txn.Get(h.dbi, cmd.Key); err != nil {
			return err
		}

		return txn.Put(h.dbi, cmd.Key, buf, 0)
	})

	return decode(err)
}

func (h *Handler) Append(cmd common.SetRequest) error {
	err := h.env.Update(func(txn *lmdb.Txn) error {
		buf, err := txn.Get(h.dbi, cmd.Key)
		if err != nil {
			return err
		}

		prev := bufToEntry(buf)

		e := entry{
			exptime: prev.exptime,
			flags:   prev.flags,
			data:    append(prev.data, cmd.Data...),
		}

		buf = entryToBuf(e)

		return txn.Put(h.dbi, cmd.Key, buf, 0)
	})

	return decode(err)
}

func (h *Handler) Prepend(cmd common.SetRequest) error {
	err := h.env.Update(func(txn *lmdb.Txn) error {
		buf, err := txn.Get(h.dbi, cmd.Key)
		if err != nil {
			return err
		}

		prev := bufToEntry(buf)

		e := entry{
			exptime: prev.exptime,
			flags:   prev.flags,
			data:    append(cmd.Data, prev.data...),
		}

		buf = entryToBuf(e)

		return txn.Put(h.dbi, cmd.Key, buf, 0)
	})

	return decode(err)
}

func (h *Handler) Get(cmd common.GetRequest) (<-chan common.GetResponse, <-chan error) {
	dataOut := make(chan common.GetResponse, len(cmd.Keys))
	errorOut := make(chan error, 1)
	go realHandleGet(h, cmd, dataOut, errorOut)
	return dataOut, errorOut
}

func realHandleGet(h *Handler, cmd common.GetRequest, dataOut chan common.GetResponse, errorOut chan error) {
	err := h.env.View(func(txn *lmdb.Txn) error {
		for idx, key := range cmd.Keys {
			buf, err := txn.Get(h.dbi, key)
			if de := decode(err); de != nil {
				if de == common.ErrKeyNotFound {
					dataOut <- common.GetResponse{
						Miss:   true,
						Quiet:  cmd.Quiet[idx],
						Opaque: cmd.Opaques[idx],
						Key:    key,
					}
					continue
				} else {
					return de
				}
			}

			e := bufToEntry(buf)

			if e.expired() {
				dataOut <- common.GetResponse{
					Miss:   true,
					Quiet:  cmd.Quiet[idx],
					Opaque: cmd.Opaques[idx],
					Key:    key,
				}
				continue
			}

			dataOut <- common.GetResponse{
				Miss:   false,
				Quiet:  cmd.Quiet[idx],
				Opaque: cmd.Opaques[idx],
				Flags:  e.flags,
				Key:    key,
				Data:   e.data,
			}
		}
		return nil
	})

	if err != nil {
		errorOut <- err
	}

	close(dataOut)
	close(errorOut)
}

func (h *Handler) GetE(cmd common.GetRequest) (<-chan common.GetEResponse, <-chan error) {
	dataOut := make(chan common.GetEResponse, len(cmd.Keys))
	errorOut := make(chan error, 1)
	go realHandleGetE(h, cmd, dataOut, errorOut)
	return dataOut, errorOut
}

func realHandleGetE(h *Handler, cmd common.GetRequest, dataOut chan common.GetEResponse, errorOut chan error) {
	err := h.env.View(func(txn *lmdb.Txn) error {
		for idx, key := range cmd.Keys {
			buf, err := txn.Get(h.dbi, key)
			if de := decode(err); de != nil {
				if de == common.ErrKeyNotFound {
					dataOut <- common.GetEResponse{
						Miss:   true,
						Quiet:  cmd.Quiet[idx],
						Opaque: cmd.Opaques[idx],
						Key:    key,
					}
					continue
				} else {
					return de
				}
			}

			e := bufToEntry(buf)

			if e.expired() {
				dataOut <- common.GetEResponse{
					Miss:   true,
					Quiet:  cmd.Quiet[idx],
					Opaque: cmd.Opaques[idx],
					Key:    key,
				}
				continue
			}

			dataOut <- common.GetEResponse{
				Miss:    false,
				Quiet:   cmd.Quiet[idx],
				Opaque:  cmd.Opaques[idx],
				Exptime: e.exptime,
				Flags:   e.flags,
				Key:     key,
				Data:    e.data,
			}
		}
		return nil
	})

	if err != nil {
		errorOut <- err
	}

	close(dataOut)
	close(errorOut)
}

func (h *Handler) GAT(cmd common.GATRequest) (common.GetResponse, error) {
	var e entry

	err := h.env.Update(func(txn *lmdb.Txn) error {
		buf, err := txn.Get(h.dbi, cmd.Key)
		if err != nil {
			return err
		}

		e = bufToEntry(buf)

		// If the item is expired, proactively delete it
		if e.expired() {
			return txn.Del(h.dbi, cmd.Key, nil)
		}

		// set the new expiration time
		exptime := uint32(time.Now().Unix()) + cmd.Exptime
		binary.BigEndian.PutUint32(buf[0:4], exptime)

		return txn.Put(h.dbi, cmd.Key, buf, 0)
	})

	if de := decode(err); de != nil {
		if de == common.ErrKeyNotFound {
			return common.GetResponse{
				Miss:   true,
				Opaque: cmd.Opaque,
				Key:    cmd.Key,
			}, nil
		} else {
			return common.GetResponse{}, de
		}
	}

	return common.GetResponse{
		Miss:   false,
		Opaque: cmd.Opaque,
		Flags:  e.flags,
		Key:    cmd.Key,
		Data:   e.data,
	}, nil
}

func (h *Handler) Delete(cmd common.DeleteRequest) error {
	err := h.env.Update(func(txn *lmdb.Txn) error {
		return txn.Del(h.dbi, cmd.Key, nil)
	})

	return decode(err)
}

func (h *Handler) Touch(cmd common.TouchRequest) error {
	err := h.env.Update(func(txn *lmdb.Txn) error {
		buf, err := txn.Get(h.dbi, cmd.Key)
		if err != nil {
			return err
		}

		// set the new expiration time
		exptime := uint32(time.Now().Unix()) + cmd.Exptime
		binary.BigEndian.PutUint32(buf[0:4], exptime)

		return txn.Put(h.dbi, cmd.Key, buf, 0)
	})

	return decode(err)
}

func (h *Handler) Close() error {
	// Singleton means don't close until the program shuts down
	return nil
}
