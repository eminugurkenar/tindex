package tindex

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/boltdb/bolt"
)

var (
	bucketLabelToID  = []byte("label_to_id")
	bucketIDToLabel  = []byte("id_to_label")
	bucketSeriesToID = []byte("series_to_id")
	bucketIDToSeries = []byte("id_to_series")

	bucketPostings = []byte("postings")
	bucketSkiplist = []byte("skiplist")
)

func init() {
	if _, ok := seriesStores["bolt"]; ok {
		panic("bolt series store initialized twice")
	}
	seriesStores["bolt"] = newBoltSeriesStore

	if _, ok := postingsStores["bolt"]; ok {
		panic("bolt postings store initialized twice")
	}
	postingsStores["bolt"] = newBoltPostingsStore

	if _, ok := timelineStores["bolt"]; ok {
		panic("bolt timeline store initialized twice")
	}
	timelineStores["bolt"] = newBoltTimelineStore
}

type boltPostingsStore struct {
	db *bolt.DB
}

func newBoltPostingsStore(path string) (postingsStore, error) {
	if err := os.MkdirAll(path, 0777); err != nil {
		return nil, err
	}
	db, err := bolt.Open(filepath.Join(path, "postings.db"), 0666, nil)
	if err != nil {
		return nil, err
	}
	s := &boltPostingsStore{
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

func (s *boltPostingsStore) Close() error {
	return s.db.Close()
}

func (s *boltPostingsStore) Begin(writeable bool) (postingsTx, error) {
	tx, err := s.db.Begin(writeable)
	if err != nil {
		return nil, err
	}
	return &boltPostingsTx{
		Tx:       tx,
		skiplist: tx.Bucket(bucketSkiplist),
		postings: tx.Bucket(bucketPostings),
	}, nil
}

type boltPostingsTx struct {
	*bolt.Tx

	skiplist *bolt.Bucket
	postings *bolt.Bucket
}

type iteratorStoreFunc func(k uint64) (iterator, error)

func (s iteratorStoreFunc) get(k uint64) (iterator, error) {
	return s(k)
}

func (p *boltPostingsTx) iter(k uint64) (iterator, error) {
	b := p.skiplist.Bucket(encodeUint64(k))
	if b == nil {
		return nil, errNotFound
	}
	fmt.Println("skiplist")
	bit := &boltSkiplistCursor{
		k:   k,
		c:   b.Cursor(),
		bkt: b,
	}
	var ka, v uint64
	var err error
	for ka, v, err = bit.seek(k); err == nil; ka, v, err = bit.next() {
		fmt.Printf("| %v %v | ", ka, v)
	}
	fmt.Printf("%s\n", err)

	it := &skipIterator{
		skiplist: &boltSkiplistCursor{
			k:   k,
			c:   b.Cursor(),
			bkt: b,
		},
		iterators: iteratorStoreFunc(func(k uint64) (iterator, error) {
			data := p.postings.Get(encodeUint64(k))
			if data == nil {
				return nil, errNotFound
			}
			// TODO(fabxc): for now, offset is zero, pages have no header
			// and are always delta encoded.
			return newPageDelta(data).cursor(), nil
		}),
	}

	bla, err := it.iterators.get(2)
	if err != nil {
		panic(err)
	}
	fmt.Println("page 2")
	fmt.Println(expandIterator(bla))
	return it, nil
}

func (p *boltPostingsTx) append(k, id uint64) error {
	b, err := p.skiplist.CreateBucketIfNotExists(encodeUint64(k))
	if err != nil {
		return err
	}
	sl := &boltSkiplistCursor{
		k:   k,
		c:   b.Cursor(),
		bkt: b,
	}
	_, pid, err := sl.seek(math.MaxUint64)
	if err != nil {
		if err == io.EOF {
			pid, err = p.postings.NextSequence()
			if err != nil {
				return err
			}
		} else {
			return err
		}
	}

	var pg page
	pdata := p.postings.Get(encodeUint64(pid))
	if pdata == nil {
		// The page ID was newly allocated but the page doesn't exist yet.
		pg = newPageDelta(make([]byte, pageSize))
		if err := pg.init(id); err != nil {
			return err
		}
		if err := sl.append(id, pid); err != nil {
			return err
		}
	} else {
		// The byte slice is mmaped from bolt. We have to copy it.
		// TODO(fabxc): page-aligned storage wanted that allows writing directly.
		pdatac := make([]byte, len(pdata))
		copy(pdatac, pdata)
		pg = newPageDelta(pdatac)

		if err := pg.cursor().append(id); err != errPageFull {
			return err
		} else {
			// We couldn't append to the page because it was full.
			// Allocate a new page.
			pid, err = p.postings.NextSequence()
			if err != nil {
				return err
			}
			pg = newPageDelta(make([]byte, pageSize))
			if err := pg.init(id); err != nil {
				return err
			}
			if err := sl.append(id, pid); err != nil {
				return err
			}
		}
	}

	// Update the page in Bolt.
	return p.postings.Put(encodeUint64(pid), pg.data())
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

// boltSeriesStore implements a seriesStore based on Bolt.
type boltSeriesStore struct {
	db *bolt.DB
}

// newBoltSeriesStore initializes a Bolt-based seriesStore under path.
func newBoltSeriesStore(path string) (seriesStore, error) {
	if err := os.MkdirAll(path, 0777); err != nil {
		return nil, err
	}
	db, err := bolt.Open(filepath.Join(path, "series.db"), 0666, nil)
	if err != nil {
		return nil, err
	}
	s := &boltSeriesStore{
		db: db,
	}
	err = db.Update(func(tx *bolt.Tx) error {
		if _, err = tx.CreateBucketIfNotExists(bucketLabelToID); err != nil {
			return err
		}
		if _, err = tx.CreateBucketIfNotExists(bucketIDToLabel); err != nil {
			return err
		}
		if _, err = tx.CreateBucketIfNotExists(bucketSeriesToID); err != nil {
			return err
		}
		if _, err = tx.CreateBucketIfNotExists(bucketIDToSeries); err != nil {
			return err
		}
		return nil
	})
	return s, err
}

func (s *boltSeriesStore) Close() error {
	return s.db.Close()
}

// Begin implements the seriesStore interface.
func (s *boltSeriesStore) Begin(writeable bool) (seriesTx, error) {
	tx, err := s.db.Begin(writeable)
	if err != nil {
		return nil, err
	}
	return &boltSeriesTx{
		Tx:         tx,
		IDToSeries: tx.Bucket(bucketIDToSeries),
		seriesToID: tx.Bucket(bucketSeriesToID),
		IDToLabel:  tx.Bucket(bucketIDToLabel),
		labelToID:  tx.Bucket(bucketLabelToID),
	}, nil
}

// boltSeriesTx implements a seriesTx.
type boltSeriesTx struct {
	*bolt.Tx

	seriesToID, IDToSeries *bolt.Bucket
	labelToID, IDToLabel   *bolt.Bucket
}

// series implements the seriesTx interface.
func (s *boltSeriesTx) series(sid uint64) (map[string]string, error) {
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(buf, sid)

	series := s.IDToSeries.Get(buf[:n])
	if series == nil {
		return nil, errNotFound
	}
	var ids []uint64
	r := bytes.NewReader(series)
	for r.Len() > 0 {
		id, _, err := readUvarint(r)
		if err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}

	m := map[string]string{}

	for _, id := range ids {
		k, v, err := s.label(id)
		if err != nil {
			return nil, err
		}
		m[k] = v
	}
	return m, nil
}

// ensureSeries implements the seriesTx interface.
func (s *boltSeriesTx) ensureSeries(desc map[string]string) (uint64, seriesKey, bool, error) {
	// Ensure that all labels are persisted.
	var skey seriesKey
	for k, v := range desc {
		key, err := s.ensureLabel(k, v)
		if err != nil {
			return 0, nil, false, err
		}
		skey = append(skey, key)
	}
	sort.Sort(skey)

	// Check whether we have seen the series before.
	var sid uint64
	if sidb := s.seriesToID.Get(skey.bytes()); sidb != nil {
		sid, _ := binary.Uvarint(sidb)
		// TODO(fabxc): validate.
		return sid, skey, false, nil
	}

	// We haven't seen this series before, create a new ID.
	sid, err := s.IDToSeries.NextSequence()
	if err != nil {
		return 0, nil, false, err
	}
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(buf, sid)

	if err := s.IDToSeries.Put(buf[:n], skey.bytes()); err != nil {
		return 0, nil, false, err
	}
	if err := s.seriesToID.Put(skey.bytes(), buf[:n]); err != nil {
		return 0, nil, false, err
	}

	return sid, skey, true, nil
}

// label retrieves the key/value label associated with id.
func (s *boltSeriesTx) label(id uint64) (string, string, error) {
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(buf, id)
	label := s.IDToLabel.Get(buf[:n])
	if label == nil {
		return "", "", fmt.Errorf("label for ID %q not found", buf[:n])
	}
	p := bytes.Split(label, []byte{seperator})

	return string(p[0]), string(p[1]), nil
}

// ensureLabel returns a unique ID for the label. If the label was not seen
// before a new, monotonically increasing ID is returned.
func (s *boltSeriesTx) ensureLabel(key, val string) (uint64, error) {
	k := make([]byte, len(key)+len(val)+1)

	copy(k[:len(key)], []byte(key))
	k[len(key)] = seperator
	copy(k[len(key)+1:], []byte(val))

	var err error
	var id uint64
	if v := s.labelToID.Get(k); v != nil {
		id, _ = binary.Uvarint(v)
	} else {
		id, err = s.IDToLabel.NextSequence()
		if err != nil {
			return 0, err
		}

		buf := make([]byte, binary.MaxVarintLen64)
		n := binary.PutUvarint(buf, id)
		if err := s.labelToID.Put(k, buf[:n]); err != nil {
			return 0, err
		}
		if err := s.IDToLabel.Put(buf[:n], k); err != nil {
			return 0, err
		}
	}

	return id, nil
}

func (s *boltSeriesTx) labels(m Matcher) (ids []uint64, err error) {
	c := s.labelToID.Cursor()

	for lbl, id := c.Seek([]byte(m.Key())); bytes.HasPrefix(lbl, []byte(m.Key())); lbl, id = c.Next() {
		p := bytes.Split(lbl, []byte{seperator})
		if !m.Match(string(p[1])) {
			continue
		}
		x, _ := binary.Uvarint(id)
		ids = append(ids, x)
	}

	return ids, nil
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

func (tl *boltTimelineTx) Instant(t time.Time) (iterator, error) {
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

func (tl *boltTimelineTx) Range(start, end time.Time) (iterator, error) {
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
	base  iterator
	diffs *boltTimelineDiffIterator

	discardDels bool

	bv, dv uint64
	be, de error
	ds     bool
}

func (tl *boltTimelineIterator) next() (uint64, error) {
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
				tl.bv, tl.be = tl.base.next()
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
			tl.bv, tl.be = tl.base.next()
			break
		}

		// diff affects current base head.
		if tl.discardDels || tl.ds {
			x = tl.bv
			tl.dv, tl.ds, tl.de = tl.diffs.next()
			tl.bv, tl.be = tl.base.next()
			break
		} else {
			tl.dv, tl.ds, tl.de = tl.diffs.next()
			tl.bv, tl.be = tl.base.next()
			// Skip as the base entry was deleted.
			continue
		}
	}
	return x, nil
}

func (tl *boltTimelineIterator) seek(v uint64) (uint64, error) {
	tl.bv, tl.be = tl.base.seek(v)
	tl.dv, tl.ds, tl.de = tl.diffs.seek(v)
	return tl.next()
}
