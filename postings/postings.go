package postings

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/edsrzf/mmap-go"

	"github.com/fabxc/tsindex/postings/skiptable"
)

type Key uint32

type Value uint32

type Pair struct {
	Value Value
	Ptr   uint64
}

type Set []Pair

type Postings interface {
	Get(k Key, from, to Value) (Set, error)
	Set(k Key, v Value, ptr uint64) error
	Sync() error
	Close() error
}

const indexBlockSize = 4096

type postings struct {
	skipTable *skiptable.SkipTable
	nextPage  int
	data      mmap.MMap
	file      *os.File
}

const indexFilename = "index"
const initialIndexFilesize = 1 << 15

func New(dir string) (Postings, error) {
	if err := os.Mkdir(filepath.Join(dir, "skiptable"), 0777); err != nil {
		return nil, err
	}
	st, err := skiptable.New(filepath.Join(dir, "skiptable"), skiptable.DefaultOpts)
	if err != nil {
		return nil, err
	}
	var f *os.File
	fn := filepath.Join(dir, indexFilename)

	_, err = os.Stat(fn)
	if os.IsNotExist(err) {
		f, err = os.Create(fn)
		if err != nil {
			return nil, err
		}
		if err = f.Truncate(initialIndexFilesize); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	} else {
		f, err = os.OpenFile(fn, os.O_RDWR, 0666)
		if err != nil {
			return nil, fmt.Errorf("error opening file %q: %s", fn, err)
		}
	}

	// TODO(fabxc): verify file is padded to page size.

	data, err := mmap.Map(f, mmap.RDWR, 0)
	if err != nil {
		return nil, fmt.Errorf("mmap error: %s", err)
	}

	return &postings{
		skipTable: st,
		data:      data,
		file:      f,
	}, nil
}

func (p *postings) allocatePage(k Key, v Value) (uint32, error) {
	fmt.Println("alloc")

	stat, err := p.file.Stat()
	if err != nil {
		return 0, err
	}
	// Grow file.
	if int64(p.nextPage*indexBlockSize) > stat.Size() {
		fmt.Println("grow file")
		if err := p.data.Flush(); err != nil {
			return 0, err
		}
		if err := p.data.Unmap(); err != nil {
			return 0, err
		}
		if err := p.file.Truncate(stat.Size() * 2); err != nil {
			return 0, err
		}
		data, err := mmap.Map(p.file, mmap.RDWR, 0)
		if err != nil {
			return 0, err
		}
		p.data = data
	}

	offset := uint32(p.nextPage)
	if err := p.skipTable.Store(skiptable.Key(k), skiptable.Value(v), offset); err != nil {
		return 0, fmt.Errorf("skip table store: %s", err)
	}
	p.nextPage++

	return offset, nil
}

// page returns the raw page at offset.
func (p *postings) page(offset uint32) []byte {
	return []byte(p.data[offset*indexBlockSize : (offset+1)*indexBlockSize])
}

func (p *postings) Set(k Key, v Value, ptr uint64) error {
	off, err := p.skipTable.Offset(skiptable.Key(k), skiptable.Value(v))
	if err != nil {
		return fmt.Errorf("skip table: %s", err)
	}

	a, err := newPageAppender(p.page(off), false)
	if err != nil {
		return err
	}
	err = a.append(v, ptr)
	if err != errPageFull {
		return err
	}
	// The page couldn't fit the sample. Allocate a new page.
	// If that one fails too, propagate the error.
	off, err2 := p.allocatePage(k, v)
	if err2 != nil {
		return err2
	}
	a, err = newPageAppender(p.page(off), true)
	if err != nil {
		return err
	}
	return a.append(v, ptr)
}

func (p *postings) Get(k Key, from, to Value) (Set, error) {
	offsets, err := p.skipTable.RangeOffsets(skiptable.Key(k), skiptable.Value(from), skiptable.Value(to))
	if err != nil {
		return nil, fmt.Errorf("skip table: %s", err)
	}
	var pages [][]byte
	for _, off := range offsets {
		pages = append(pages, p.page(off))
	}

	return newIndexReader(pages).findAll(from, to)
}

func (p *postings) Sync() error {
	if err := p.skipTable.Sync(); err != nil {
		return err
	}
	if err := p.data.Flush(); err != nil {
		return err
	}
	if err := p.file.Sync(); err != nil {
		return err
	}

	return nil
}

func (p *postings) Close() error {
	if err := p.Sync(); err != nil {
		return err
	}
	if err := p.skipTable.Close(); err != nil {
		return err
	}
	if err := p.data.Unmap(); err != nil {
		return err
	}
	if err := p.file.Close(); err != nil {
		return err
	}

	return nil
}

// Intersection returns an intersection set of all input sets.
func Intersect(sets ...Set) Set {
	if len(sets) == 0 {
		return nil
	}

	s1 := sets[0]
	for _, s2 := range sets[1:] {
		s1 = intersect(s1, s2)
	}
	return s1
}

func intersect(s1, s2 Set) Set {
	var result Set
	var i, j int
	for {
		if i >= len(s1) || j >= len(s2) {
			break
		}
		if s1[i].Value < s2[j].Value {
			i++
		} else if s2[j].Value < s1[i].Value {
			j++
		} else {
			result = append(result, s2[j])
			j++
			i++
		}
	}
	return result
}

type pageEncoding int

const (
	pageEncodingUint32 = pageEncoding(iota)
	pageEncodingVarint
	pageEncodingDelta
)

// A pageReader reads value/pointer pairs from an index page.
type pageReader interface {
	// find returns the offset associated with the given value.
	// The bool is true iff the value was found.
	find(v Value) (pointer uint64, found bool)
	// findRange returns all offset/value pairs with the value
	// range of min and max.
	findRange(min, max Value) Set
}

// newPageReader returns a pageReader and its encoding based on the
// given page byte slice.
func newPageReader(p []byte) (pageReader, pageEncoding, error) {
	page := bytes.NewBuffer(p)

	enc, err := binary.ReadUvarint(page)
	if err != nil {
		fmt.Errorf("reading encoding failed: %s", err)
	}

	switch pageEncoding(enc) {
	case pageEncodingUint32:
		return newPageReaderUint32(page.Bytes()), pageEncodingUint32, nil
	}

	return nil, 0, fmt.Errorf("unknown encoding %q", enc)
}

// A pageReader that stores value/pointer pairs as simple sequences of
// uint32 and uint64 byte slices.
type pageReaderUint32 struct {
	page []byte
}

func newPageReaderUint32(page []byte) *pageReaderUint32 {
	return &pageReaderUint32{
		page: page,
	}
}

// find implements the pageReader interface.
func (p *pageReaderUint32) find(value Value) (uint64, bool) {
	// The first value is guaranteed to exist and may also be zero.
	lastVal := Value(binary.BigEndian.Uint32(p.page[:4]))
	if lastVal == value {
		return binary.BigEndian.Uint64(p.page[4:12]), true
	}

	for i := 12; i < len(p.page); i += 12 {
		val := Value(binary.BigEndian.Uint32(p.page[i : i+4]))
		if val <= lastVal {
			break
		}
		if val < value {
			lastVal = val
			continue
		}
		if val == value {
			return binary.BigEndian.Uint64(p.page[i+4 : i+12]), true
		}
		// The index values are monotonically increasing. The value we are
		// searching for won't follow.
		if val > value {
			break
		}
	}
	return 0, false
}

// findRange implements the pageReader interface.
func (p *pageReaderUint32) findRange(min, max Value) Set {
	var set Set
	// The first value is guaranteed to exist and may also be zero.
	lastVal := Value(binary.BigEndian.Uint32(p.page[:4]))
	if lastVal >= min {
		// The first value was already beyond the range.
		if lastVal > max {
			return set
		}
		set = append(set, Pair{
			Value: lastVal,
			Ptr:   binary.BigEndian.Uint64(p.page[4:12]),
		})
	}

	i := 12
	for ; i < len(p.page); i += 12 {
		val := Value(binary.BigEndian.Uint32(p.page[i : i+4]))
		if val <= lastVal {
			// The index ended.
			return set
		}
		if val < min {
			lastVal = val
			continue
		}
		if val <= max {
			// First value in range found. Add the offset to our results and continue
			// with the second loop.
			set = append(set, Pair{
				Value: val,
				Ptr:   binary.BigEndian.Uint64(p.page[i+4 : i+12]),
			})
			break
		}
		// The min/max interval didn't include any value.
		return set
	}

	for ; i < len(p.page); i += 12 {
		val := Value(binary.BigEndian.Uint32(p.page[i : i+4]))
		if val <= lastVal {
			break
		}
		if val > max {
			break
		}
		set = append(set, Pair{
			Value: val,
			Ptr:   binary.BigEndian.Uint64(p.page[i+4 : i+12]),
		})
	}

	return set
}

// indexReader reads value/pointer pairs from index pages.
type indexReader struct {
	pages [][]byte
}

func newIndexReader(pages [][]byte) *indexReader {
	return &indexReader{
		pages: pages,
	}
}

var ErrNotFound = errors.New("not found")

// find returns the pointer associated with value. If the value is
// not in the index ErrNotFound is returned.
func (r *indexReader) find(value Value) (uint64, error) {
	for _, page := range r.pages {
		pr, _, err := newPageReader(page)
		if err != nil {
			return 0, err
		}
		if ptr, ok := pr.find(value); ok {
			return ptr, nil
		}
	}
	return 0, ErrNotFound
}

// findAll returns the set of all value/pointer pairs in the index
// that are within the value range of min and max.
func (r *indexReader) findAll(min, max Value) (set Set, err error) {
	for _, page := range r.pages {
		pr, _, err := newPageReader(page)
		if err != nil {
			return nil, err
		}
		set = append(set, pr.findRange(min, max)...)
	}
	return set, nil
}

// pageAppender can append values to a page.
type pageAppender interface {
	append(Value, uint64) error
}

// newPageAppender returns a pageAppender for the page's encoding. The fresh
// flag indicates whether the underlying page was newly created.
func newPageAppender(p []byte, fresh bool) (pageAppender, error) {
	page := bytes.NewBuffer(p)

	enc, err := binary.ReadUvarint(page)
	if err != nil {
		fmt.Errorf("reading encoding failed: %s", err)
	}

	switch pageEncoding(enc) {
	case pageEncodingUint32:
		return newPageAppenderUint32(page.Bytes(), fresh), nil
	}

	return nil, fmt.Errorf("unknown encoding %q", enc)
}

type pageAppenderUint32 struct {
	page  []byte
	fresh bool
}

var errPageFull = errors.New("page full")

func newPageAppenderUint32(p []byte, fresh bool) *pageAppenderUint32 {
	return &pageAppenderUint32{
		page:  p,
		fresh: fresh,
	}
}

// append implements the pageAppender interface.
func (a *pageAppenderUint32) append(value Value, ptr uint64) error {
	if a.fresh {
		if len(a.page) < 12 {
			return errPageFull
		}
		binary.BigEndian.PutUint32(a.page[:4], uint32(value))
		binary.BigEndian.PutUint64(a.page[4:12], ptr)

		a.fresh = false
		return nil
	}
	// The first value is guaranteed to exist and may also be zero.
	lastVal := binary.BigEndian.Uint32(a.page[:4])

	i := 12
	// Skip through all values that are already set.
	for ; i < len(a.page); i += 12 {
		val := binary.BigEndian.Uint32(a.page[i : i+4])
		if val <= lastVal {
			break
		}
	}
	if i+12 > len(a.page) {
		return errPageFull
	}
	binary.BigEndian.PutUint32(a.page[i:i+4], uint32(value))
	binary.BigEndian.PutUint64(a.page[i+4:i+12], ptr)

	return nil
}
