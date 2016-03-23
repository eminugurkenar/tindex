package tsindex

import (
	// "bytes"
	"encoding/binary"
	// "fmt"
	"errors"
	"io"
	"math"

	"github.com/boltdb/bolt"
)

var bucketSkiplists = []byte("skiplists")

type postings struct {
	bkt       *bolt.Bucket
	pages     pageStore
	skiplists skiplistStore
}

func newPostings(bkt *bolt.Bucket) (*postings, error) {
	p := &postings{
		bkt: bkt,
	}
	if b, err := bkt.CreateBucketIfNotExists([]byte("pages")); err != nil {
		return nil, err
	} else {
		p.pages = &boltPageStore{bkt: b}
	}
	if b, err := bkt.CreateBucketIfNotExists(bucketSkiplists); err != nil {
		return nil, err
	} else {
		p.skiplists = &boltSkiplistStore{bkt: b}
	}
	return p, nil
}

func (p *postings) get(k pkey) (postingsCursor, error) {
	sl, err := p.skiplists.get(k)
	if err != nil {
		return nil, err
	}
	c := &skippingCursor{
		skiplist: sl,
		store:    p.pages,
	}
	return c, nil
}

type pgid uint64

func (id pgid) bytes() []byte { return encodeUint64(uint64(id)) }

// Page is a fixed-sized block of bytes.
type page struct {
	data []byte
}

// size returns the size of the given page.
func (p *page) size() int { return len(p.data) }

// A pageStore can get, store, and allocate pages.
type pageStore interface {
	// Get retrieves the page with the given page ID.
	get(pgid) (*page, error)
	// Put stores the page under the given page ID.
	put(pgid, *page) error
	// Delete the page with the given ID. The page ID may be
	// reused for newly allocated pages afterwards.
	delete(pgid) error
	// Allocate a new page and return its ID.
	allocate() (pgid, error)
}

// boltPageStore is a pageStore based on a single BoltDB bucket.
// It is scoped around the transaction of the bucket. The object is ephemeral
// and has to be recreated for each transaction.
type boltPageStore struct {
	bkt *bolt.Bucket
}

// get implements a pageStore.
func (ps *boltPageStore) get(id pgid) (*page, error) {
	p := ps.bkt.Get(id.bytes())
	if p == nil {
		return nil, errNotFound
	}
	return &page{p}, nil
}

// put implements a pageStore.
func (ps *boltPageStore) put(id pgid, p *page) error {
	return ps.bkt.Put(id.bytes(), p.data)
}

// delete implements a pageStore.
func (ps *boltPageStore) delete(id pgid) error {
	return ps.bkt.Delete(id.bytes())
}

// allocate implements a pageStore.
func (ps *boltPageStore) allocate() (pgid, error) {
	id, err := ps.bkt.NextSequence()
	return pgid(id), err
}

type postingsCursor interface {
	next() (docid, error)
	seek(docid) (docid, error)
	append(docid) error
}

type skippingCursor struct {
	skiplist skiplist
	store    pageStore

	curPage postingsCursor
}

func (c *skippingCursor) append(id docid) error {
	_, pid, err := c.skiplist.seek(docid(math.MaxUint64))
	if err != nil {
		return err
	}
	p, err := c.store.get(pid)
	if err != nil {
		if err == errNotFound {
			pid, err = c.store.allocate()
			if err != nil {
				return err
			}
			p, err = c.store.get(pid)
		} else {
			return err
		}
	}
	c.curPage = newPageDeltaCursor(p, 0)
	if err = c.curPage.append(id); err != nil {
		return err
	}

	return c.store.put(pid, p)
}

func (c *skippingCursor) seek(id docid) (docid, error) {
	_, pid, err := c.skiplist.seek(id)
	if err != nil {
		return 0, err
	}

	p, err := c.store.get(pid)
	if err != nil {
		return 0, err
	}
	c.curPage = newPageDeltaCursor(p, 0)

	return c.curPage.seek(id)
}

func (c *skippingCursor) next() (docid, error) {
	if c.curPage == nil {
		return c.seek(0)
	}

	id, err := c.curPage.next()
	if err == nil {
		return docid(id), nil
	}
	if err != io.EOF {
		return 0, err
	}
	_, pid, err := c.skiplist.next()
	if err != nil {
		return 0, err
	}
	p, err := c.store.get(pid)
	if err != nil {
		return 0, err
	}
	c.curPage = newPageDeltaCursor(p, 0)
	return c.curPage.next()
}

type pageDeltaCursor struct {
	*page

	pos, off int
	cur      docid
	fresh    bool
}

type docid uint64

func (d docid) bytes() []byte { return encodeUint64(uint64(d)) }

func newPageDeltaCursor(p *page, offset int) *pageDeltaCursor {
	return &pageDeltaCursor{
		page:  p,
		off:   offset,
		pos:   offset,
		fresh: true,
	}
}

var errPageFull = errors.New("page full")

func (p *pageDeltaCursor) append(id docid) error {
	_, err := p.seek(0)
	for ; err == nil; _, err = p.next() {
		// Consume.
	}
	if err != io.EOF {
		return err
	}
	if p.size()-p.pos < binary.MaxVarintLen64 {
		return errPageFull
	}
	n := binary.PutUvarint(p.data[p.pos:], uint64(id))
	p.pos += n

	return nil
}

func (p *pageDeltaCursor) seek(min docid) (docid, error) {
	p.pos = p.off
	v, err := p.next()
	for ; err == nil && v < min; v, err = p.next() {
		// Consume.
	}
	return p.cur, err
}

func (p *pageDeltaCursor) next() (docid, error) {
	var n int
	if p.fresh {
		id, n := binary.Uvarint(p.data[p.pos:])
		if n <= 0 {
			return 0, io.EOF
		}
		p.cur = docid(id)
		p.fresh = false

	} else {
		var dv int64
		dv, n = binary.Varint(p.data[p.pos:])
		if n <= 0 || dv == 0 {
			return 0, io.EOF
		}
		p.cur = docid(int64(p.cur) + dv)
	}
	p.pos += n

	return p.cur, nil
}
