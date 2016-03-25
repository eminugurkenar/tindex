package tindex

import (
	// "bytes"
	"encoding/binary"
	"errors"
	"path/filepath"
	"regexp"
	// "sort"
	"fmt"
	// "time"
)

var (
	errOutOfOrder = errors.New("out of order")
	errNotFound   = errors.New("not found")
)

type Index interface {
	// Instant(m []Matcher, at time.Time) ([]uint64, error)
	// Range(m []Matcher, from, to time.Time) ([]uint64, error)

	Series(id uint64) (map[string]string, error)
	EnsureSeries(labels map[string]string) (uint64, error)

	// See(id uint64, ts uint64) error
	// Unsee(id uint64, ts uint64) error

	Close() error
	Sync() error
}

type Options struct {
	SeriesStore   string
	PostingsStore string
}

var DefaultOptions = &Options{
	SeriesStore:   "bolt",
	PostingsStore: "bolt",
}

func Open(path string, opts *Options) (Index, error) {
	// Use default options if none are provided.
	if opts == nil {
		opts = DefaultOptions
	}
	createSeriesStore, ok := seriesStores[opts.SeriesStore]
	if !ok {
		return nil, fmt.Errorf("unknown series store %q", opts.SeriesStore)
	}
	createPostingsStore, ok := postingsStores[opts.PostingsStore]
	if !ok {
		return nil, fmt.Errorf("unknown postings store %q", opts.PostingsStore)
	}

	ss, err := createSeriesStore(filepath.Join(path, "series", opts.SeriesStore))
	if err != nil {
		return nil, err
	}
	ps, err := createPostingsStore(filepath.Join(path, "postings", opts.PostingsStore))
	if err != nil {
		return nil, err
	}

	ix := &index{
		seriesStore:   ss,
		postingsStore: ps,
	}
	return ix, nil
}

// seperator is a byte that cannot occur in a valid UTF-8 sequence. It can thus
// be used to mark boundaries between serialized strings.
const seperator = byte('\xff')

// func (ix *index) Instant(ms []Matcher, ts time.Time) ([]uint64, error) {
// 	tx, err := ix.seriesStore.Begin(false)
// 	if err != nil {
// 		return nil, err
// 	}
// 	lids, err := tx.labels(ms...)
// 	if err != nil {
// 		return nil, err
// 	}

// 	var cursors []postingsCursor
// 	for _, k := range lids {
// 		c, err := ix.postings.get(k)
// 		if err != nil {
// 			return nil, err
// 		}
// 		cursors = append(cursors, c)
// 	}

// 	series = expandCursor(Intersect(cursors...))

// 	return series, err
// }

// Constructors for registered stores.
var (
	seriesStores   = map[string]func(path string) (seriesStore, error){}
	postingsStores = map[string]func(path string) (postingsStore, error){}
)

// A seriesStore can start a read or write seriesTx.
type seriesStore interface {
	Begin(writeable bool) (seriesTx, error)
	Close() error
}

// Tx is a generic interface for transactions.
type Tx interface {
	Commit() error
	Rollback() error
}

// seriesStore stores series descriptors under unique and montonically increasing IDs.
type seriesTx interface {
	Tx
	// get the series descriptor for the series with the given id.
	series(id uint64) (map[string]string, error)
	// ensure stores the series discriptor with a montonically increasing,
	// unique ID. If the series descriptor was already stored, the ID is returned.
	ensureSeries(desc map[string]string) (uint64, seriesKey, error)
	// labels returns label keys of labels for the given matcher.
	labels(Matcher) ([]uint64, error)
}

// A postingsStore can start a postingsTx on postings lists.
type postingsStore interface {
	Begin(writable bool) (postingsTx, error)
	Close() error
}

// A postingsTx is a transactions on postings lists associated with a key.
type postingsTx interface {
	Tx
	// iter returns a new iterator on the postings list for k.
	iter(k uint64) (iterator, error)
	// append adds the ID to the end of the postings list for k. The ID must
	// be strictly larger than the last value in the list.
	append(k, id uint64) error
}

// index implements the Index interface.
type index struct {
	opts *Options

	seriesStore   seriesStore
	postingsStore postingsStore
}

func (ix *index) Close() error {
	if err := ix.seriesStore.Close(); err != nil {
		return err
	}
	return ix.postingsStore.Close()
}

func (ix *index) Sync() error {
	return nil
}

// Series implements the Index interface.
func (ix *index) Series(id uint64) (map[string]string, error) {
	tx, err := ix.seriesStore.Begin(false)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	return tx.series(id)
}

// Series implements the Index interface.
func (ix *index) EnsureSeries(labels map[string]string) (sid uint64, err error) {
	stx, err := ix.seriesStore.Begin(true)
	if err != nil {
		return 0, err
	}
	sid, skey, err := stx.ensureSeries(labels)
	if err != nil {
		stx.Rollback()
		return 0, err
	}
	stx.Commit()

	// TODO(fabxc): skip this step is series is not new.
	ptx, err := ix.postingsStore.Begin(true)
	if err != nil {
		return sid, err
	}

	for _, k := range skey {
		if err := ptx.append(k, sid); err != nil {
			ptx.Rollback()
			return sid, err
		}
	}
	return sid, ptx.Commit()
}

type labelSet map[string]string

func (ls labelSet) m() map[string]string {
	return map[string]string(ls)
}

type seriesKey []uint64

func (k seriesKey) Len() int           { return len(k) }
func (k seriesKey) Swap(i, j int)      { k[i], k[j] = k[j], k[i] }
func (k seriesKey) Less(i, j int) bool { return k[i] < k[j] }

func (k seriesKey) bytes() []byte {
	b := make([]byte, len(k)*binary.MaxVarintLen64)
	n := 0
	for _, j := range k {
		n += binary.PutUvarint(b[n:], j)
	}
	return b[:n]
}

// Matcher checks whether a value for a key satisfies a check condition.
type Matcher interface {
	Key() string
	Match(value string) bool
}

type EqualMatcher struct {
	key, val string
}

func NewEqualMatcher(key, val string) *EqualMatcher {
	return &EqualMatcher{key: key, val: val}
}

func (m *EqualMatcher) Key() string         { return m.key }
func (m *EqualMatcher) Match(s string) bool { return m.val == s }

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
