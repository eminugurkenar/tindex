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
	pdb, err := pagebuf.Open(filepath.Join(path, "pb"), 0666, &pagebuf.Options{
		PageSize: pageSize,
	})
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
	bktDocs     = []byte("docs")
	bktTerms    = []byte("terms")
	bktTermIDs  = []byte("term_ids")
	bktSkiplist = []byte("skiplist")

	keyMeta = []byte("meta")
)

func (ix *Index) init(tx *bolt.Tx) error {
	// Ensure all buckets exist. Any other index methods assume
	// that these buckets exist and may panic otherwise.
	for _, bn := range [][]byte{
		bktMeta, bktTerms, bktTermIDs, bktDocs, bktSkiplist,
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
			LastDocID:  0,
			LastTermID: 0,
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

// Querier starts a new query session against the index.
func (ix *Index) Querier() (*Querier, error) {
	kvtx, err := ix.bolt.Begin(false)
	if err != nil {
		return nil, err
	}
	pbtx, err := ix.pbuf.Begin(false)
	if err != nil {
		kvtx.Rollback()
		return nil, err
	}
	return &Querier{
		kvtx:        kvtx,
		pbtx:        pbtx,
		termBkt:     kvtx.Bucket(bktTerms),
		skiplistBkt: kvtx.Bucket(bktSkiplist),
	}, nil
}

// Querier encapsulates the index for several queries.
type Querier struct {
	kvtx *bolt.Tx
	pbtx *pagebuf.Tx

	termBkt     *bolt.Bucket
	skiplistBkt *bolt.Bucket
}

// Close closes the underlying index transactions.
func (q *Querier) Close() error {
	err0 := q.pbtx.Rollback()
	err1 := q.kvtx.Rollback()
	if err0 != nil {
		return err0
	}
	return err1
}

// Search returns an iterator over all document IDs that match all
// provided matchers.
func (q *Querier) Search(m Matcher) (Iterator, error) {
	tids := q.termsForMatcher(m)
	its := make([]Iterator, 0, len(tids))
	fmt.Println("tids", tids)
	for _, t := range tids {
		it, err := q.postingsIter(t)
		if err != nil {
			return nil, err
		}
		its = append(its, it)
	}

	if len(its) == 0 {
		return nil, nil
	}
	return Merge(its...), nil
}

// postingsIter returns an iterator over the postings list of term t.
func (q *Querier) postingsIter(t termid) (Iterator, error) {
	b := q.skiplistBkt.Bucket(t.bytes())
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
			data, err := q.pbtx.Get(k)
			if err != nil {
				return nil, errNotFound
			}
			// TODO(fabxc): for now, offset is zero, pages have no header
			// and are always delta encoded.
			return newPageDelta(data).cursor(), nil
		}),
	}

	return it, nil
}

func (q *Querier) termsForMatcher(m Matcher) termids {
	c := q.termBkt.Cursor()
	pref := append([]byte(m.Key()), 0xff)

	var ids termids
	// TODO(fabxc): We scan the entire term value range for the field. Improvide this by direct
	// and prefixed seeks depending on the matcher.
	for k, v := c.Seek(pref); bytes.HasPrefix(k, pref); k, v = c.Next() {
		if m.Match(string(k[len(pref):])) {
			ids = append(ids, newTermID(v))
		}
	}
	return ids
}

// Doc returns the document with the given ID.
func (ix *Index) Doc(id DocID) (Terms, error) {
	tx, err := ix.bolt.Begin(false)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	bdocs := tx.Bucket(bktDocs)

	v := bdocs.Get(id.bytes())
	if v == nil {
		return nil, errNotFound
	}
	l, n := binary.Uvarint(v)
	tids := newTermIDs(v[n : n+int(l)])

	b := tx.Bucket(bktTermIDs)
	terms := make(Terms, len(tids))
	for i, t := range tids {
		// TODO(fabxc): is this encode/decode cycle here worth the space savings?
		// If we stored plain uint64s we can just pass the slice back in.
		v := b.Get(t.bytes())
		if v == nil {
			return nil, fmt.Errorf("term not found")
		}
		term, err := newTerm(v)
		if err != nil {
			return nil, err
		}
		terms[i] = term
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
		ix:        ix,
		tx:        tx,
		meta:      &meta{},
		termBkt:   tx.Bucket(bktTerms),
		termidBkt: tx.Bucket(bktTermIDs),
		terms:     map[Term]*batchTerm{},
	}
	*b.meta = *ix.meta
	return b, nil
}

// meta contains information about the state of the index.
type meta struct {
	LastDocID  DocID
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
	ID   DocID
	Body Body // Document data or pointer.
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

func newTerm(b []byte) (t Term, e error) {
	c := bytes.SplitN(b, []byte{0xff}, 2)
	if len(c) != 2 {
		return t, fmt.Errorf("invalid term")
	}
	t.Field = string(c[0])
	t.Val = string(c[1])
	return t, nil
}

// bytes returns a byte slice representation of the term.
func (t *Term) bytes() []byte {
	b := make([]byte, 0, len(t.Field)+1+len(t.Val))
	b = append(b, []byte(t.Field)...)
	b = append(b, 0xff)
	return append(b, []byte(t.Val)...)
}

// Body holds a document's content or pointer to it.
type Body []byte

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

	termBkt   *bolt.Bucket
	termidBkt *bolt.Bucket

	docs  []*batchDoc
	terms map[Term]*batchTerm
}

type batchDoc struct {
	id    DocID
	terms termids
	body  Body
}

type batchTerm struct {
	id   termid  // zero if term has not been added yet
	docs []DocID // documents to be indexed for the term
}

// DocID is a unique identifier for a document.
type DocID uint64

func newDocID(b []byte) DocID {
	return DocID(decodeUint64(b))
}

func (d DocID) bytes() []byte {
	return encodeUint64(uint64(d))
}

type termid uint64

func newTermID(b []byte) termid {
	return termid(decodeUint64(b))
}

func (t termid) bytes() []byte {
	return encodeUint64(uint64(t))
}

type termids []termid

func (t termids) Len() int           { return len(t) }
func (t termids) Swap(i, j int)      { t[i], t[j] = t[j], t[i] }
func (t termids) Less(i, j int) bool { return t[i] < t[j] }

// newTermIDs reads a sequence of uvarints from b and appends them
// to the term IDs.
func newTermIDs(b []byte) (t termids) {
	for len(b) > 0 {
		k, n := binary.Uvarint(b)
		t = append(t, termid(k))
		b = b[n:]
	}
	return t
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

// Add adds a new document and returns a unique ID for it.
// The ID only becomes valid after the batch has been committed successfully.
func (b *Batch) Add(body Body) DocID {
	b.meta.LastDocID++
	b.docs = append(b.docs, &batchDoc{
		id:    b.meta.LastDocID,
		body:  body,
		terms: make(termids, 0, 10),
	})
	return b.meta.LastDocID
}

// Index adds index entries for the document ID for the given terms.
// The caller has to ensure that no document with a smaller ID has been
// indexed for the same term before.
func (b *Batch) Index(id DocID, terms Terms) {
	// Subtract last document ID before this batch was started.
	d := b.docs[id-b.ix.meta.LastDocID-1]
	for _, t := range terms {
		tb := b.terms[t]
		// Populate term if necessary and allocate a new ID if it
		// hasn't been created in the database before.
		if tb == nil {
			tb = &batchTerm{docs: make([]DocID, 0, 1024)}
			b.terms[t] = tb

			if idb := b.termBkt.Get(t.bytes()); idb != nil {
				tb.id = termid(decodeUint64(idb))
			} else {
				b.meta.LastTermID++
				tb.id = b.meta.LastTermID
			}
		}
		d.terms = append(d.terms, tb.id)

		// Add the document ID for later appending to the inverted index.
		tb.docs = append(tb.docs, id)
	}
}

// Commit executes the batched indexing against the underlying index.
func (b *Batch) Commit() error {
	defer b.ix.rwlock.Unlock()
	// Close read transaction to open a write transaction. The outer rwlock
	// stil guards against intermittend writes between switching.
	if err := b.tx.Rollback(); err != nil {
		return err
	}
	err := b.ix.bolt.Update(func(tx *bolt.Tx) error {
		for _, d := range b.docs {
			if err := b.addDoc(tx, d.id, d.body, d.terms); err != nil {
				return err
			}
		}
		// Add newly allocated terms.
		termBkt := tx.Bucket(bktTerms)
		termidBkt := tx.Bucket(bktTermIDs)

		for t, tb := range b.terms {
			if tb.id > b.ix.meta.LastTermID {
				bid := encodeUint64(uint64(tb.id))
				tby := t.bytes()

				if err := termBkt.Put(tby, bid); err != nil {
					return fmt.Errorf("setting term failed: %s", err)
				}
				if err := termidBkt.Put(bid, tby); err != nil {
					return fmt.Errorf("setting term failed: %s", err)
				}
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
	return err
}

// Rollback drops all changes applied in the batch.
func (b *Batch) Rollback() error {
	b.ix.rwlock.Unlock()
	return b.tx.Rollback()
}

func (b *Batch) addDoc(tx *bolt.Tx, id DocID, body Body, tids termids) error {
	idb := id.bytes()

	ibkt := tx.Bucket(bktDocs)
	sort.Sort(tids)

	tidsb := tids.bytes()

	v := make([]byte, len(tidsb)+binary.MaxVarintLen32+len(body))
	n := binary.PutUvarint(v, uint64(len(tidsb)))
	copy(v[n:], tidsb)
	copy(v[n+len(tidsb):], body)

	return ibkt.Put(idb, v)
}

// writePostings adds the postings batch to the index.
func (b *Batch) writePostingsBatch(kvtx *bolt.Tx, pbtx *pagebuf.Tx) error {
	skiplist := kvtx.Bucket(bktSkiplist)

	// createPage allocates a new delta-encoded page starting with id as its first entry.
	createPage := func(id DocID) (page, error) {
		pg := newPageDelta(make([]byte, pageSize-pagebuf.PageHeaderSize))
		if err := pg.init(id); err != nil {
			return nil, err
		}
		return pg, nil
	}

	for _, tb := range b.terms {
		ids := tb.docs

		b, err := skiplist.CreateBucketIfNotExists(tb.id.bytes())
		if err != nil {
			return err
		}
		sl := &boltSkiplistCursor{
			k:   uint64(tb.id),
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

		for i := 0; i < len(ids); i++ {
			if err = pc.append(ids[i]); err == errPageFull {
				// We couldn't append to the page because it was full.
				// Store away the old page...
				if pid == 0 {
					// The page was new.
					pid, err = pbtx.Add(pg.data())
					if err != nil {
						return err
					}
					first, err := pc.Seek(0)
					if err != nil {
						return err
					}
					if err := sl.append(first, pid); err != nil {
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
			first, err := pc.Seek(0)
			if err != nil {
				return err
			}
			if err := sl.append(first, pid); err != nil {
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
