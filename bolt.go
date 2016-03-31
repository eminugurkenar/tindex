package tindex

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/boltdb/bolt"
)

func init() {

	if _, ok := timelineStores["bolt"]; ok {
		panic("bolt timeline store initialized twice")
	}
	timelineStores["bolt"] = newBoltTimelineStore
}

type iteratorStoreFunc func(k uint64) (Iterator, error)

func (s iteratorStoreFunc) get(k uint64) (Iterator, error) {
	return s(k)
}

type boltTimelineStore struct {
	db *bolt.DB
}

var (
	bucketSnapshot = []byte("snapshot")
	bucketDiff     = []byte("diff")
)

func newBoltTimelineStore(path string) (timelineStore, error) {
	if err := os.MkdirAll(path, 0777); err != nil {
		return nil, err
	}
	db, err := bolt.Open(filepath.Join(path, "series.db"), 0666, nil)
	if err != nil {
		return nil, err
	}
	s := &boltTimelineStore{
		db: db,
	}
	err = db.Update(func(tx *bolt.Tx) error {
		if _, err = tx.CreateBucketIfNotExists(bucketSnapshot); err != nil {
			return err
		}
		if _, err = tx.CreateBucketIfNotExists(bucketDiff); err != nil {
			return err
		}
		return nil
	})
	return s, err
}

func (s *boltTimelineStore) Close() error {
	return s.db.Close()
}

func (s *boltTimelineStore) Begin(writeable bool) (timelineTx, error) {
	tx, err := s.db.Begin(writeable)
	if err != nil {
		return nil, err
	}
	return &boltTimelineTx{
		Tx:        tx,
		snapshots: tx.Bucket(bucketSnapshot),
		diffs:     tx.Bucket(bucketDiff),
	}, nil
}

type boltTimelineTx struct {
	*bolt.Tx

	snapshots *bolt.Bucket
	diffs     *bolt.Bucket
}

func (tl *boltTimelineTx) Instant(t time.Time) (Iterator, error) {
	ts := t.UnixNano() / int64(time.Millisecond)
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(ts))

	return &boltTimelineIterator{
		base: newPlainListIterator([]uint64{}),
		diffs: &boltTimelineDiffIterator{
			min: make([]byte, 8),
			max: buf,
			c:   tl.diffs.Cursor(),
		},
	}, nil
}

func (tl *boltTimelineTx) Range(start, end time.Time) (Iterator, error) {
	tstart := encodeUint64(uint64(start.UnixNano() / int64(time.Millisecond)))
	tend := encodeUint64(uint64(end.UnixNano() / int64(time.Millisecond)))

	return &boltTimelineIterator{
		base: &boltTimelineIterator{
			base: newPlainListIterator([]uint64{}),
			diffs: &boltTimelineDiffIterator{
				max: tstart,
				c:   tl.diffs.Cursor(),
			},
		},
		diffs: &boltTimelineDiffIterator{
			min: tstart,
			max: tend,
			c:   tl.diffs.Cursor(),
		},
		discardDels: true,
	}, nil
}

type diffState byte

const (
	diffStateAdd = 1
	diffStateDel = 2
)

func (tl *boltTimelineTx) SetDiff(t time.Time, state diffState, ids ...uint64) error {
	ts := t.UnixNano() / int64(time.Millisecond)

	for _, id := range ids {
		buf := make([]byte, 16)
		binary.BigEndian.PutUint64(buf, id)
		binary.BigEndian.PutUint64(buf[8:], uint64(ts))

		if err := tl.diffs.Put(buf, []byte{byte(state)}); err != nil {
			return err
		}
	}

	return nil
}

type boltTimelineDiffIterator struct {
	c        *bolt.Cursor
	min, max []byte

	k, v []byte
}

func (tl *boltTimelineDiffIterator) next() (uint64, bool, error) {
	var (
		last  []byte
		exist bool
	)
	for {
		if tl.k == nil {
			if last != nil {
				break
			}
			return 0, false, io.EOF
		}
		// Check whether we reached a new ID.
		if last != nil && !bytes.Equal(last[:8], tl.k[:8]) {
			break
		}

		// Check whether the following diffs for the current ID are behind the max
		// timestamp. If so, skip to the next ID and the last exist state is the
		// state of ID at max.
		if bytes.Compare(tl.k[8:], tl.max) > 0 {
			buf := make([]byte, 16)
			copy(buf[8:], tl.min)
			binary.BigEndian.PutUint64(buf, binary.BigEndian.Uint64(tl.k[:8])+1)

			tl.k, tl.v = tl.c.Seek(buf)
			// If we were already reading a value, return it first.
			if last != nil {
				break
			}
		} else if bytes.Compare(tl.k[8:], tl.min) < 0 {
			buf := make([]byte, 16)
			copy(buf[8:], tl.min)
			copy(buf[:8], tl.k[:8])

			tl.k, tl.v = tl.c.Seek(buf)
		} else {
			// A new diff within our time window. Evaluate most recent state
			// and advance.
			exist = tl.v[0] == byte(diffStateAdd)
			last = tl.k
			tl.k, tl.v = tl.c.Next()
		}
	}
	return decodeUint64(last[:8]), exist, nil
}

func (tl *boltTimelineDiffIterator) seek(v uint64) (uint64, bool, error) {
	buf := make([]byte, 16)
	copy(buf[8:], tl.min)
	binary.BigEndian.PutUint64(buf, v)
	tl.k, tl.v = tl.c.Seek(buf)
	return tl.next()
}

type boltTimelineIterator struct {
	base  Iterator
	diffs *boltTimelineDiffIterator

	discardDels bool

	bv, dv uint64
	be, de error
	ds     bool
}

func (tl *boltTimelineIterator) Close() error {
	return nil
}

func (tl *boltTimelineIterator) Next() (uint64, error) {
	var x uint64
	for {
		if tl.be == io.EOF && tl.de == io.EOF {
			return 0, io.EOF
		}
		if tl.be != nil {
			if tl.be == io.EOF {
				if tl.ds {
					x = tl.dv
					tl.dv, tl.ds, tl.de = tl.diffs.next()
					break
				}
				// Deletion of something not in the base, nothing to do.
				tl.dv, tl.ds, tl.de = tl.diffs.next()
				continue
			}
			return 0, tl.be
		}
		if tl.de != nil {
			if tl.de == io.EOF {
				x = tl.bv
				tl.bv, tl.be = tl.base.Next()
				break
			}
			return 0, tl.de
		}
		if tl.bv > tl.dv {
			if tl.ds {
				x = tl.dv
				tl.dv, tl.ds, tl.de = tl.diffs.next()
				break
			}
			// Deletion of something not in the base, nothing to do.
			tl.dv, tl.ds, tl.de = tl.diffs.next()
			continue
		}
		// The next diff is larger than the base. Emit everything that's
		// in the base.
		if tl.dv > tl.bv {
			x = tl.bv
			tl.bv, tl.be = tl.base.Next()
			break
		}

		// diff affects current base head.
		if tl.discardDels || tl.ds {
			x = tl.bv
			tl.dv, tl.ds, tl.de = tl.diffs.next()
			tl.bv, tl.be = tl.base.Next()
			break
		} else {
			tl.dv, tl.ds, tl.de = tl.diffs.next()
			tl.bv, tl.be = tl.base.Next()
			// Skip as the base entry was deleted.
			continue
		}
	}
	return x, nil
}

func (tl *boltTimelineIterator) Seek(v uint64) (uint64, error) {
	tl.bv, tl.be = tl.base.Seek(v)
	tl.dv, tl.ds, tl.de = tl.diffs.seek(v)
	return tl.Next()
}
