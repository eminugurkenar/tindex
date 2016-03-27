package tindex

import (
	// "bytes"
	"encoding/binary"
	"errors"
	"path/filepath"
	"regexp"
	// "sort"
	"fmt"
	"time"
)

var (
	errOutOfOrder = errors.New("out of order")
	errNotFound   = errors.New("not found")
)

type Index interface {
	Instant(at time.Time, ms ...Matcher) ([]uint64, error)
	// Range(m []Matcher, from, to time.Time) ([]uint64, error)

	Series(id uint64) (map[string]string, error)
	EnsureSeries(labels map[string]string) (uint64, error)

	See(ts time.Time, id uint64) error
	Unsee(ts time.Time, id uint64) error

	Close() error
	Sync() error
}

type Options struct {
	SeriesStore   string
	PostingsStore string
	TimelineStore string
}

var DefaultOptions = &Options{
	SeriesStore:   "bolt",
	PostingsStore: "bolt",
	TimelineStore: "bolt",
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
	createTimelineStore, ok := timelineStores[opts.TimelineStore]
	if !ok {
		return nil, fmt.Errorf("unknown timeline store %q", opts.TimelineStore)
	}

	ss, err := createSeriesStore(filepath.Join(path, "series", opts.SeriesStore))
	if err != nil {
		return nil, err
	}
	ps, err := createPostingsStore(filepath.Join(path, "postings", opts.PostingsStore))
	if err != nil {
		return nil, err
	}
	ts, err := createTimelineStore(filepath.Join(path, "timeline", opts.TimelineStore))
	if err != nil {
		return nil, err
	}

	ix := &index{
		seriesStore:   ss,
		postingsStore: ps,
		timelineStore: ts,
	}
	return ix, nil
}

// seperator is a byte that cannot occur in a valid UTF-8 sequence. It can thus
// be used to mark boundaries between serialized strings.
const seperator = byte('\xff')

// Constructors for registered stores.
var (
	seriesStores   = map[string]func(path string) (seriesStore, error){}
	postingsStores = map[string]func(path string) (postingsStore, error){}
	timelineStores = map[string]func(path string) (timelineStore, error){}
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
	ensureSeries(desc map[string]string) (uint64, seriesKey, bool, error)
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

type timelineStore interface {
	Begin(writable bool) (timelineTx, error)
	Close() error
}

type timelineTx interface {
	Tx
	// Returns an iterator of document IDs that were valid at the given time.
	Instant(t time.Time) (iterator, error)
	// Returns an iterator of document IDs that were valid at some point
	// during the given range.
	Range(start, end time.Time) (iterator, error)

	See(t time.Time, ids ...uint64) error
	Unsee(t time.Time, ids ...uint64) error
}

// index implements the Index interface.
type index struct {
	opts *Options

	seriesStore   seriesStore
	postingsStore postingsStore
	timelineStore timelineStore
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
	sid, skey, created, err := stx.ensureSeries(labels)
	if err != nil {
		stx.Rollback()
		return 0, err
	}
	if !created {
		stx.Rollback()
		return sid, nil
	}
	if err := stx.Commit(); err != nil {
		return 0, err
	}

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

func (ix *index) See(ts time.Time, id uint64) error {
	tx, err := ix.timelineStore.Begin(true)
	if err != nil {
		return err
	}
	if err := tx.See(ts, id); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (ix *index) Unsee(ts time.Time, id uint64) error {
	tx, err := ix.timelineStore.Begin(true)
	if err != nil {
		return err
	}
	if err := tx.Unsee(ts, id); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (ix *index) Instant(ts time.Time, ms ...Matcher) ([]uint64, error) {
	stx, err := ix.seriesStore.Begin(false)
	if err != nil {
		return nil, err
	}
	defer stx.Rollback()

	ptx, err := ix.postingsStore.Begin(false)
	if err != nil {
		return nil, err
	}
	defer ptx.Rollback()

	var its []iterator
	for _, m := range ms {
		ids, err := stx.labels(m)
		if err != nil {
			return nil, err
		}
		var mits []iterator
		for _, id := range ids {
			it, err := ptx.iter(id)
			if err != nil {
				return nil, err
			}
			mits = append(mits, it)
		}
		its = append(its, merge(mits...))
	}

	ttx, err := ix.timelineStore.Begin(false)
	if err != nil {
		return nil, err
	}
	defer ttx.Rollback()

	it, err := ttx.Instant(ts)
	if err != nil {
		return nil, err
	}
	its = append(its, it)

	res, err := expandIterator(intersect(its...))
	return res, err
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
