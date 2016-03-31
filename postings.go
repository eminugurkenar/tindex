package tindex

import (
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"

	"github.com/boltdb/bolt"
)

var (
	bucketPostings = []byte("postings")
	bucketSkiplist = []byte("skiplist")
)

// Postings provides read and append access to a set of postings lists.
type Postings interface {
	// Get an Iterator on the postings list associated with k.
	Iter(k uint64) (Iterator, error)
	// Append the ids to the postings list associated with k.
	// The given IDs must be sorted and strictly greater than
	// the last ID in the postings list.
	Append(PostingsBatches) error
	// Close the postings store.
	Close() error
}

func NewPostings(path string) (Postings, error) {
	if err := os.MkdirAll(path, 0777); err != nil {
		return nil, err
	}
	db, err := bolt.Open(filepath.Join(path, "postings.db"), 0666, nil)
	if err != nil {
		return nil, err
	}
	s := &postingsStore{
		db: db,
	}
	err = db.Update(func(tx *bolt.Tx) error {
		if _, err = tx.CreateBucketIfNotExists(bucketPostings); err != nil {
			return err
		}
		if _, err = tx.CreateBucketIfNotExists(bucketSkiplist); err != nil {
			return err
		}
		return nil
	})
	return s, err
}

// PostingsBatch is a set of IDs to be appended to the postings list
// for the given Key.
type PostingsBatches map[uint64][]uint64

// postingsStore implements the Postings interface based on BoltDB.
type postingsStore struct {
	db *bolt.DB
}

// Close implements the Postings interface.
func (p *postingsStore) Close() error {
	return p.db.Close()
}

// Iter implements the Postings interface.
func (p *postingsStore) Iter(k uint64) (Iterator, error) {
	tx, err := p.db.Begin(false)
	if err != nil {
		return nil, err
	}

	skiplist := tx.Bucket(bucketSkiplist)
	if skiplist == nil {
		return nil, fmt.Errorf("Bucket %q missing", bucketSkiplist)
	}
	postings := tx.Bucket(bucketPostings)
	if postings == nil {
		return nil, fmt.Errorf("Bucket %q missing", bucketPostings)
	}

	b := skiplist.Bucket(encodeUint64(k))
	if b == nil {
		return nil, errNotFound
	}

	it := &skippingIterator{
		skiplist: &boltSkiplistCursor{
			k:   k,
			c:   b.Cursor(),
			bkt: b,
		},
		iterators: iteratorStoreFunc(func(k uint64) (Iterator, error) {
			data := postings.Get(encodeUint64(k))
			if data == nil {
				return nil, errNotFound
			}
			// TODO(fabxc): for now, offset is zero, pages have no header
			// and are always delta encoded.
			return newPageDelta(data).cursor(), nil
		}),
		close: func() error {
			return tx.Rollback()
		},
	}

	return it, nil
}

// Append implements the Postings interface.
func (p *postingsStore) Append(batches PostingsBatches) error {
	return p.db.Update(func(tx *bolt.Tx) error {
		sl := tx.Bucket(bucketSkiplist)
		if sl == nil {
			return fmt.Errorf("Bucket %q missing", bucketSkiplist)
		}
		ps := tx.Bucket(bucketPostings)
		if ps == nil {
			return fmt.Errorf("Bucket %q missing", bucketPostings)
		}

		// TODO(fabxc): that could be parallelized?
		for k, ids := range batches {
			if err := postingsAppend(sl, ps, k, ids...); err != nil {
				return err
			}
		}
		return nil
	})
}

// postingsAppend a set of monotonically increasing IDs to the postings list
// of the given key. The first ID must be strictly greater than the last
// ID in the postings list.
func postingsAppend(skiplist, postings *bolt.Bucket, key uint64, ids ...uint64) error {
	if len(ids) == 0 {
		return nil
	}
	b, err := skiplist.CreateBucketIfNotExists(encodeUint64(key))
	if err != nil {
		return err
	}
	sl := &boltSkiplistCursor{
		k:   key,
		c:   b.Cursor(),
		bkt: b,
	}

	createPage := func(id uint64) (uint64, page, error) {
		pid, err := postings.NextSequence()
		if err != nil {
			return 0, nil, err
		}
		pg := newPageDelta(make([]byte, pageSize))
		if err := pg.init(id); err != nil {
			return 0, nil, err
		}
		return pid, pg, sl.append(id, pid)
	}

	var (
		pg  page       // Page we are currently appending to.
		pc  pageCursor // Its cursor.
		pid uint64     // Its ID.
	)
	// Get the most recent page. If none exist, the entire postings list is new.
	_, pid, err = sl.seek(math.MaxUint64)
	if err != nil {
		if err != io.EOF {
			return err
		}
		// No most recent page for the key exists. The postings list is new and
		// we have to allocate a new page ID for it.
		if pid, pg, err = createPage(ids[0]); err != nil {
			return err
		}
		pc = pg.cursor()
		ids = ids[1:]
	} else {
		// Load the most recent page.
		pdata := postings.Get(encodeUint64(pid))
		if pdata == nil {
			return fmt.Errorf("page with ID %q does not exist", pid)
		}

		// The byte slice is mmaped from bolt. We have to copy it to make modifications.
		pdatac := make([]byte, len(pdata))
		copy(pdatac, pdata)

		pg = newPageDelta(pdatac)
		pc = pg.cursor()
	}

	for i := 0; i < len(ids); i++ {
		if err = pc.append(ids[i]); err == errPageFull {
			// We couldn't append to the page because it was full.
			// Store away the old page...
			if err := postings.Put(encodeUint64(pid), pg.data()); err != nil {
				return err
			}
			// ... and allocate a new page.
			if pid, pg, err = createPage(ids[i]); err != nil {
				return err
			}
			pc = pg.cursor()
		} else if err != nil {
			return err
		}
	}

	// Save the last page we have written to.
	return postings.Put(encodeUint64(pid), pg.data())
}

// boltSkiplistCursor implements the skiplistCurosr interface.
//
// TODO(fabxc): benchmark the overhead of a bucket per key.
// It might be more performant to have all skiplists in the same bucket.
//
// 	20k keys, ~10 skiplist entries avg -> 200k keys, 1 bucket vs 20k buckets, 10 keys
//
type boltSkiplistCursor struct {
	// k is currently unused. If the bucket holds entries for more than
	// just a single key, it will be necessary.
	k   uint64
	c   *bolt.Cursor
	bkt *bolt.Bucket
}

func (s *boltSkiplistCursor) next() (uint64, uint64, error) {
	db, pb := s.c.Next()
	if db == nil {
		return 0, 0, io.EOF
	}
	return decodeUint64(db), decodeUint64(pb), nil
}

func (s *boltSkiplistCursor) seek(k uint64) (uint64, uint64, error) {
	db, pb := s.c.Seek(encodeUint64(k))
	if db == nil {
		db, pb = s.c.Last()
		if db == nil {
			return 0, 0, io.EOF
		}
	}
	did, pid := decodeUint64(db), decodeUint64(pb)

	if did > k {
		// If the found entry is behind the seeked ID, try the previous
		// entry if it exists. The page it points to contains the range of k.
		dbp, pbp := s.c.Prev()
		if dbp != nil {
			did, pid = decodeUint64(dbp), decodeUint64(pbp)
		} else {
			// We skipped before the first entry. The cursor is now out of
			// state and subsequent calls to Next() will return nothing.
			// Reset it to the first position.
			s.c.First()
		}
	}
	return did, pid, nil
}

func (s *boltSkiplistCursor) append(d, p uint64) error {
	k, _ := s.c.Last()

	if k != nil && decodeUint64(k) >= uint64(d) {
		return errOutOfOrder
	}

	return s.bkt.Put(encodeUint64(d), encodeUint64(p))
}
