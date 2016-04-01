package tindex

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/boltdb/bolt"
)

// A Timeline marks sets as active or inactive at different points in time.
// It provides iterators on set IDs for instants or ranges in time.
type Timeline interface {
	// Returns an iterator of IDs that were valid at the given time.
	Instant(ts time.Time) (Iterator, error)
	// Returns an iterator of document IDs that were valid at some point
	// for the duration after the start time.
	Range(ts time.Time, dur time.Duration) (Iterator, error)
	// Mark the IDs as active for the given timestamp.
	Active(ts time.Time, ids ...uint64) error
	// Mark the IDs as inactive for the given timestamp.
	Inactive(ts time.Time, ids ...uint64) error
	// Close the timeline store.
	Close() error
}

// timelineStore implements the Timeline interface using BoltDB.
type timelineStore struct {
	db       *bolt.DB
	postings Postings
}

var bktDiffs = []byte("diffs")

func NewTimeline(path string, postings Postings) (Timeline, error) {
	if err := os.MkdirAll(path, 0777); err != nil {
		return nil, err
	}
	db, err := bolt.Open(filepath.Join(path, "diffs.db"), 0666, nil)
	if err != nil {
		return nil, err
	}
	err = db.Update(func(tx *bolt.Tx) error {
		if _, err = tx.CreateBucketIfNotExists(bktDiffs); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return &timelineStore{
		db:       db,
		postings: postings,
	}, nil
}

func (tl *timelineStore) Close() error {
	return tl.db.Close()
}

func (tl *timelineStore) Instant(ts time.Time) (Iterator, error) {
	tx, err := tl.db.Begin(false)
	if err != nil {
		return nil, err
	}
	diffs := tx.Bucket(bktDiffs)
	if diffs == nil {
		return nil, fmt.Errorf("bucket %q not found", bktDiffs)
	}
	return &boltTimelineIterator{
		base: newPlainListIterator([]uint64{}),
		diffs: &boltTimelineDiffIterator{
			min: make([]byte, 8),
			max: encodeUint64(timestamp(ts)),
			c:   diffs.Cursor(),
		},
		close: tx.Rollback,
	}, nil
}

func (tl *timelineStore) Range(ts time.Time, d time.Duration) (Iterator, error) {
	tx, err := tl.db.Begin(false)
	if err != nil {
		return nil, err
	}
	diffs := tx.Bucket(bktDiffs)
	if diffs == nil {
		return nil, fmt.Errorf("bucket %q not found", bktDiffs)
	}
	return &boltTimelineIterator{
		base: &boltTimelineIterator{
			base: newPlainListIterator([]uint64{}),
			diffs: &boltTimelineDiffIterator{
				max: encodeUint64(timestamp(ts)),
				c:   diffs.Cursor(),
			},
		},
		diffs: &boltTimelineDiffIterator{
			min: encodeUint64(timestamp(ts)),
			max: encodeUint64(timestamp(ts) + duration(d)),
			c:   diffs.Cursor(),
		},
		discardDels: true,
		close:       tx.Rollback,
	}, nil
}

const (
	diffActive   = byte(1)
	diffInactive = byte(2)
)

func (tl *timelineStore) Active(t time.Time, ids ...uint64) error {
	return tl.setDiff(t, diffActive, ids...)
}

func (tl *timelineStore) Inactive(t time.Time, ids ...uint64) error {
	return tl.setDiff(t, diffInactive, ids...)
}
func (tl *timelineStore) setDiff(t time.Time, dt byte, ids ...uint64) error {
	return tl.db.Update(func(tx *bolt.Tx) error {
		diffs := tx.Bucket(bktDiffs)
		if diffs == nil {
			return fmt.Errorf("bucket %q not found", bktDiffs)
		}
		var (
			ts  = timestamp(t)
			val = []byte{dt}
			buf = make([]byte, 16)
		)

		for _, id := range ids {
			binary.BigEndian.PutUint64(buf, id)
			binary.BigEndian.PutUint64(buf[8:], ts)

			if err := diffs.Put(buf, val); err != nil {
				return err
			}
		}
		return nil
	})
}

// timestamp returns the time as a uint64 in milliseconds.
func timestamp(t time.Time) uint64 {
	return uint64(t.UnixNano() / int64(time.Millisecond))
}

func duration(d time.Duration) uint64 {
	return uint64(d / time.Millisecond)
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
			exist = tl.v[0] == byte(diffActive)
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

	close func() error
}

func (tl *boltTimelineIterator) Close() error {
	if tl.close == nil {
		return nil
	}
	return tl.close()
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
