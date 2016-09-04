package tindex

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"sync"

	"github.com/boltdb/bolt"
	"github.com/fabxc/pagebuf"
)

var (
	errOutOfOrder = errors.New("out of order")
	errNotFound   = errors.New("not found")
)

// Options for an Index.
type Options struct {
}

// DefaultOptions used for opening a new index.
var DefaultOptions = &Options{}

// Index is a fully persistent inverted index of documents with any number of fields
// that map to exactly one term.
type Index struct {
	pbuf *pagebuf.DB
	bolt *bolt.DB
	meta *meta

	rwlock sync.Mutex
}

// Open returns an index located in the given path. If none exists a new
// one is created.
func Open(path string, opts *Options) (*Index, error) {
	if opts == nil {
		opts = DefaultOptions
	}

	if err := os.MkdirAll(path, 0777); err != nil {
		return nil, err
	}

	bdb, err := bolt.Open(filepath.Join(path, "kv"), 0666, nil)
	if err != nil {
		return nil, err
	}
	pdb, err := pagebuf.Open(filepath.Join(path, "pb"), 0666, nil)
	if err != nil {
		return nil, err
	}
	ix := &Index{
		bolt: bdb,
		pbuf: pdb,
		meta: &meta{},
	}
	if err := ix.bolt.Update(ix.init); err != nil {
		return nil, err
	}
	return ix, nil
}

// Close closes the index.
func (ix *Index) Close() error {
	err0 := ix.pbuf.Close()
	err1 := ix.bolt.Close()
	if err0 != nil {
		return err0
	}
	return err1
}

var (
	bktMeta     = []byte("meta")
	bktTerms    = []byte("terms")
	bktTermIDs  = []byte("term_ids")
	bktDocIDs   = []byte("docs_ids")
	bktSkiplist = []byte("skiplist")

	keyMeta = []byte("meta")
)

func (ix *Index) init(tx *bolt.Tx) error {
	// Ensure all buckets exist. Any other index methods assume
	// that these buckets exist and may panic otherwise.
	for _, bn := range [][]byte{
		bktMeta, bktTerms, bktTermIDs, bktDocIDs, bktSkiplist,
	} {
		if _, err := tx.CreateBucketIfNotExists(bn); err != nil {
			return fmt.Errorf("create bucket %q failed: %s", string(bn), err)
		}
	}

	// Read the meta state if the index was already initialized.
	mbkt := tx.Bucket(bktMeta)
	if v := mbkt.Get(keyMeta); v != nil {
		if err := ix.meta.read(v); err != nil {
			return fmt.Errorf("decoding meta failed: %s", err)
		}
	} else {
		// Index not initialized yet, set up meta information.
		ix.meta = &meta{
			LastDocID:  1,
			LastTermID: 1,
		}
		v, err := ix.meta.bytes()
		if err != nil {
			return fmt.Errorf("encoding meta failed: %s", err)
		}
		if err := mbkt.Put(keyMeta, v); err != nil {
			return fmt.Errorf("creating meta failed: %s", err)
		}
	}

	return nil
}

// Search returns an iterator over all document IDs that match all
// provided matchers.
func (ix *Index) Search(matchers ...Matcher) (Iterator, error) {
	kvtx, err := ix.bolt.Begin(false)
	if err != nil {
		return nil, err
	}
	pbtx, err := ix.pbuf.Begin(false)
	if err != nil {
		return nil, err
	}

	bterms := kvtx.Bucket(bktTerms)
	bskiplist := kvtx.Bucket(bktSkiplist)

	// The union of iterators for a single matcher are merged.
	// The merge iterators of each matcher are then intersected.
	its := make([]Iterator, 0, len(matchers))

	for _, m := range matchers {
		tids, err := ix.termsForMatcher(bterms, m)
		if err != nil {
			return nil, err
		}
		mits := make([]Iterator, 0, len(tids))

		for _, t := range tids {
			it, err := ix.postingsIter(bskiplist, pbtx, t)
			if err != nil {
				return nil, err
			}
			mits = append(mits, it)
		}

		its = append(its, Merge(mits...))
	}
	return &closeWrapIterator{
		close: func() error {
			kvtx.Rollback()
			pbtx.Rollback()
			return nil
		},
		Iterator: Intersect(its...),
	}, nil
}

type closeWrapIterator struct {
	Iterator
	close func() error
}

func (it *closeWrapIterator) Close() error {
	return it.close()
}

// postingsIter returns an iterator over the postings list of term t.
func (ix *Index) postingsIter(skiplist *bolt.Bucket, pbtx *pagebuf.Tx, t termid) (Iterator, error) {
	b := skiplist.Bucket(t.bytes())
	if b == nil {
		return nil, errNotFound
	}

	it := &skippingIterator{
		skiplist: &boltSkiplistCursor{
			k:   uint64(t),
			c:   b.Cursor(),
			bkt: b,
		},
		iterators: iteratorStoreFunc(func(k uint64) (Iterator, error) {
			data, err := pbtx.Get(k)
			if err != nil {
				return nil, errNotFound
			}
			// TODO(fabxc): for now, offset is zero, pages have no header
			// and are always delta encoded.
			return newPageDelta(data).cursor(), nil
		}),
		close: func() error { return nil },
	}

	return it, nil
}

func (ix *Index) termsForMatcher(b *bolt.Bucket, m Matcher) (termids, error) {
	// If there's no bucket for the field, we match zero terms.
	if b = b.Bucket([]byte(m.Key())); b == nil {
		return nil, nil
	}

	// TODO(fabxc): We scan the entire term value range for the field. Improvide this by direct
	// and prefixed seeks depending on the matcher.
	var ids termids
	err := b.ForEach(func(k, v []byte) error {
		if m.Match(string(k)) {
			ids = append(ids, termid(binary.BigEndian.Uint64(v)))
		}
		return nil
	})
	return ids, err
}

// DocTerms retrieves the terms for the document with the given ID.
func (ix *Index) DocTerms(id uint64) (Terms, error) {
	tx, err := ix.bolt.Begin(false)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	bdocs := tx.Bucket(bktDocIDs)

	v := bdocs.Get(docid(id).bytes())
	if v == nil {
		return nil, errNotFound
	}
	var tids termids
	tids.read(v)

	b := tx.Bucket(bktTermIDs)
	terms := make(Terms, len(tids))
	for i, t := range tids {
		// TODO(fabxc): is this encode/decode cycle here worth the space savings?
		// If we stored plain uint64s we can just pass the slice back in.
		v := b.Get(t.bytes())
		if v == nil {
			return nil, fmt.Errorf("term not found")
		}
		terms[i].read(v)
	}
	return terms, nil
}

// Delete removes all documents in the iterator from the index.
// It returns the number of deleted documents.
func (ix *Index) Delete(it Iterator) (int, error) {
	panic("not implemented")
}

// Batch starts a new batch against the index.
func (ix *Index) Batch() (*Batch, error) {
	// Lock writes so we can safely pre-allocate term and doc IDs.
	ix.rwlock.Lock()

	tx, err := ix.bolt.Begin(false)
	if err != nil {
		return nil, err
	}
	b := &Batch{
		ix:       ix,
		tx:       tx,
		meta:     &meta{},
		docs:     map[docid]*Doc{},
		postings: postingsBatch{},
	}
	*b.meta = *ix.meta
	return b, nil
}

// meta contains information about the state of the index.
type meta struct {
	LastDocID  docid
	LastTermID termid
}

// read initilizes the meta from a byte slice.
func (m *meta) read(b []byte) error {
	return gob.NewDecoder(bytes.NewReader(b)).Decode(m)
}

// bytes returns a byte slice representation of the meta.
func (m *meta) bytes() ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(m); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Doc is a document that can be stored in an index.
type Doc struct {
	// Mapping of document fields to single terms.
	Terms Terms
	// Pointer to document data.
	Ptr uint64
}

// Terms is a sortable list of terms.
type Terms []Term

func (t Terms) Len() int      { return len(t) }
func (t Terms) Swap(i, j int) { t[i], t[j] = t[j], t[i] }

func (t Terms) Less(i, j int) bool {
	if t[i].Field < t[j].Field {
		return true
	}
	if t[i].Field > t[j].Field {
		return false
	}
	return t[i].Val < t[j].Val
}

// Term is a term for the specified field.
type Term struct {
	Field, Val string
}

// read populates the term with the field name and value encoded in b.
func (t *Term) read(b []byte) error {
	c := bytes.SplitN(b, []byte{0xff}, 2)
	if len(c) != 2 {
		return fmt.Errorf("invalid term")
	}
	t.Field = string(c[0])
	t.Val = string(c[1])
	return nil
}

// bytes returns a byte slice representation of the term.
func (t *Term) bytes() []byte {
	b := make([]byte, 0, len(t.Field)+1+len(t.Val))
	b = append(b, []byte(t.Field)...)
	b = append(b, 0xff)
	return append(b, []byte(t.Val)...)
}

// Matcher checks whether a value for a key satisfies a check condition.
type Matcher interface {
	Key() string
	Match(value string) bool
}

// EqualMatcher matches exactly one value for a particular label.
type EqualMatcher struct {
	key, val string
}

func NewEqualMatcher(key, val string) *EqualMatcher {
	return &EqualMatcher{key: key, val: val}
}

func (m *EqualMatcher) Key() string         { return m.key }
func (m *EqualMatcher) Match(s string) bool { return m.val == s }

// RegexpMatcher matches labels for the fixed key for which the value
// matches a regular expression.
type RegexpMatcher struct {
	key string
	re  *regexp.Regexp
}

func NewRegexpMatcher(key string, expr string) (*RegexpMatcher, error) {
	re, err := regexp.Compile(expr)
	if err != nil {
		return nil, err
	}
	return &RegexpMatcher{key: key, re: re}, nil
}

func (m *RegexpMatcher) Key() string         { return m.key }
func (m *RegexpMatcher) Match(s string) bool { return m.re.MatchString(s) }

// Batch collects multiple indexing actions and allows to apply them
// to the persistet index all at once for improved performance.
type Batch struct {
	ix   *Index
	tx   *bolt.Tx
	meta *meta

	docs     map[docid]*Doc
	postings postingsBatch
}

type docid uint64

func (d docid) bytes() []byte {
	return encodeUint64(uint64(d))
}

type termid uint64

func (t termid) bytes() []byte {
	return encodeUint64(uint64(t))
}

type termids []termid

func (t termids) Len() int           { return len(t) }
func (t termids) Swap(i, j int)      { t[i], t[j] = t[j], t[i] }
func (t termids) Less(i, j int) bool { return t[i] < t[j] }

// read reads a sequence of uvarints from b and appends them
// to the term IDs.
func (t termids) read(b []byte) {
	for len(b) > 0 {
		k, n := binary.Uvarint(b)
		t = append(t, termid(k))
		b = b[n:]
	}
}

// bytes encodes the term IDs as a sequence of uvarints.
func (t termids) bytes() []byte {
	b := make([]byte, len(t)*binary.MaxVarintLen64)
	n := 0
	for _, x := range t {
		n += binary.PutUvarint(b[n:], uint64(x))
	}
	return b[:n]
}

// postingsBatch is a set of IDs to be appended to the postings list for a term.
type postingsBatch map[termid][]docid

// Index ensures the document is present in the index and returns its newly
// created ID. The ID only becomes valid after the batch has been
// executed successfully.
func (b *Batch) Index(d *Doc) uint64 {
	b.meta.LastDocID++
	b.docs[b.meta.LastDocID] = d
	return uint64(b.meta.LastDocID)
}

// Commit executes the batched indexing against the underlying index.
func (b *Batch) Commit() error {
	defer b.ix.rwlock.Unlock()
	// Close read transaction to open a write transaction. The outer rwlock
	// stil guards against intermittend writes between switching.
	if err := b.tx.Rollback(); err != nil {
		return err
	}
	b.ix.bolt.Update(func(tx *bolt.Tx) error {
		for _, d := range b.docs {
			// Sort the term IDs and store them.
			tids, err := b.ensureTerms(tx, d.Terms)
			if err != nil {
				return err
			}
			sort.Sort(tids)

			if err := b.addDoc(tx, d, tids); err != nil {
				return err
			}
		}
		pbtx, err := b.ix.pbuf.Begin(true)
		if err != nil {
			return err
		}
		if err := b.writePostingsBatch(tx, pbtx); err != nil {
			pbtx.Rollback()
			return err
		}
		if err := pbtx.Commit(); err != nil {
			return err
		}
		return b.updateMeta(tx)
	})
	return nil
}

// Rollback drops all changes applied in the batch.
func (b *Batch) Rollback() error {
	b.ix.rwlock.Unlock()
	return b.tx.Rollback()
}

func (b *Batch) addDoc(tx *bolt.Tx, d *Doc, tids termids) error {
	// Allocate a new document ID and map it to the terms.
	b.meta.LastDocID++
	idb := b.meta.LastDocID.bytes()

	ibkt := tx.Bucket(bktDocIDs)
	if err := ibkt.Put(idb, tids.bytes()); err != nil {
		return err
	}

	// Add document to postings batch for each term.
	for _, t := range tids {
		b.postings[t] = append(b.postings[t], b.meta.LastDocID)
	}
	return nil
}

// writePostings adds the postings batch to the index.
func (b *Batch) writePostingsBatch(kvtx *bolt.Tx, pbtx *pagebuf.Tx) error {
	skiplist := kvtx.Bucket(bktSkiplist)

	// createPage allocates a new delta-encoded page starting with id as its first entry.
	createPage := func(id docid) (page, error) {
		pg := newPageDelta(make([]byte, pageSize-pagebuf.PageHeaderSize))
		if err := pg.init(uint64(id)); err != nil {
			return nil, err
		}
		return pg, nil
	}

	for t, ids := range b.postings {
		b, err := skiplist.CreateBucketIfNotExists(t.bytes())
		if err != nil {
			return err
		}
		sl := &boltSkiplistCursor{
			k:   uint64(t),
			c:   b.Cursor(),
			bkt: b,
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
			if pg, err = createPage(ids[0]); err != nil {
				return err
			}
			pc = pg.cursor()
			ids = ids[1:]
		} else {
			// Load the most recent page.
			pdata, err := pbtx.Get(pid)
			if pdata == nil {
				return fmt.Errorf("error getting page for ID %q: %s", pid, err)
			}

			pdatac := make([]byte, len(pdata))
			// The byte slice is mmaped from bolt. We have to copy it to make modifications.
			// pdatac := make([]byte, len(pdata))
			copy(pdatac, pdata)

			pg = newPageDelta(pdatac)
			pc = pg.cursor()
		}

		var lastID docid
		for i := 0; i < len(ids); i++ {
			lastID = ids[i]
			if err = pc.append(uint64(ids[i])); err == errPageFull {
				// We couldn't append to the page because it was full.
				// Store away the old page...
				if pid == 0 {
					// The page was new.
					pid, err = pbtx.Add(pg.data())
					if err != nil {
						return err
					}
					if err := sl.append(uint64(ids[i]), pid); err != nil {
						return err
					}
				} else {
					if err = pbtx.Set(pid, pg.data()); err != nil {
						return err
					}
				}

				// ... and allocate a new page.
				pid = 0
				if pg, err = createPage(ids[i]); err != nil {
					return err
				}
				pc = pg.cursor()
			} else if err != nil {
				return err
			}
		}
		// Save the last page we have written to.
		if pid == 0 {
			// The page was new.
			pid, err = pbtx.Add(pg.data())
			if err != nil {
				return err
			}
			if err := sl.append(uint64(lastID), pid); err != nil {
				return err
			}
		} else {
			if err = pbtx.Set(pid, pg.data()); err != nil {
				return err
			}
		}
	}
	return nil
}

// updateMeta updates the index's meta information based on the changes
// applied with the batch.
func (b *Batch) updateMeta(tx *bolt.Tx) error {
	b.ix.meta = b.meta
	bkt := tx.Bucket([]byte(bktMeta))
	if bkt == nil {
		return fmt.Errorf("meta bucket not found")
	}
	v, err := b.ix.meta.bytes()
	if err != nil {
		return fmt.Errorf("error encoding meta: %s", err)
	}
	return bkt.Put([]byte(keyMeta), v)
}

func (b *Batch) ensureTerms(tx *bolt.Tx, terms Terms) (termids, error) {
	tbkt := tx.Bucket(bktTerms)
	ibkt := tx.Bucket(bktTermIDs)

	res := make(termids, 0, len(terms))
	for _, t := range terms {
		// If the entire field is new, create a bucket for it.
		fbkt, err := tbkt.CreateBucketIfNotExists([]byte(t.Field))
		if err != nil {
			return nil, fmt.Errorf("creating field bucket %q failed: %s", t.Field, err)
		}
		idb := fbkt.Get([]byte(t.Val))
		if idb != nil {
			res = append(res, termid(decodeUint64(idb)))
			continue
		}
		// The term for the field is new. Get the next ID and create a
		// bi-directional mapping for it.
		b.meta.LastTermID++
		bid := encodeUint64(uint64(b.meta.LastTermID))

		if err := fbkt.Put([]byte(t.Val), bid); err != nil {
			return nil, fmt.Errorf("setting term failed: %s", err)
		}
		if err := ibkt.Put(bid, t.bytes()); err != nil {
			return nil, fmt.Errorf("setting term failed: %s", err)
		}

		res = append(res, b.meta.LastTermID)
	}
	return res, nil
}
