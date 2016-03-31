package tindex

import (
	"errors"
	"fmt"
	"path/filepath"
	"time"
)

var (
	errOutOfOrder = errors.New("out of order")
	errNotFound   = errors.New("not found")
)

type Index interface {
	Instant(at time.Time, ms ...Matcher) ([]uint64, error)
	Range(from, to time.Time, ms ...Matcher) ([]uint64, error)

	Sets(id ...uint64) ([]Set, error)
	EnsureSets(ls ...Set) ([]uint64, error)

	See(ts time.Time, id uint64) error
	Unsee(ts time.Time, id uint64) error

	Close() error
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
	createPostingsStore, ok := postingsStores[opts.PostingsStore]
	if !ok {
		return nil, fmt.Errorf("unknown postings store %q", opts.PostingsStore)
	}
	createTimelineStore, ok := timelineStores[opts.TimelineStore]
	if !ok {
		return nil, fmt.Errorf("unknown timeline store %q", opts.TimelineStore)
	}

	ps, err := createPostingsStore(filepath.Join(path, "postings", opts.PostingsStore))
	if err != nil {
		return nil, err
	}
	ts, err := createTimelineStore(filepath.Join(path, "timeline", opts.TimelineStore))
	if err != nil {
		return nil, err
	}
	ls, err := NewLabels(filepath.Join(path, "labels"))
	if err != nil {
		return nil, err
	}
	lss, err := NewLabelSets(filepath.Join(path, "label_sets"), ls)
	if err != nil {
		return nil, err
	}

	ix := &index{
		labels:        ls,
		labelSets:     lss,
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
	postingsStores = map[string]func(path string) (postingsStore, error){}
	timelineStores = map[string]func(path string) (timelineStore, error){}
)

// Tx is a generic interface for transactions.
type Tx interface {
	Commit() error
	Rollback() error
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

	SetDiff(t time.Time, s diffState, ids ...uint64) error
}

// index implements the Index interface.
type index struct {
	opts *Options

	labelSets     LabelSets
	labels        Labels
	postingsStore postingsStore
	timelineStore timelineStore
}

func (ix *index) Close() error {
	if err := ix.labelSets.Close(); err != nil {
		return err
	}
	if err := ix.labels.Close(); err != nil {
		return err
	}
	return ix.postingsStore.Close()
}

func (ix *index) Sync() error {
	return nil
}

// Series implements the Index interface.
func (ix *index) Sets(id ...uint64) ([]Set, error) {
	return ix.labelSets.Get(id...)
}

// Series implements the Index interface.
func (ix *index) EnsureSets(sets ...Set) (ids []uint64, err error) {
	ids, skeys, err := ix.labelSets.Ensure(sets...)
	if err != nil {
		return nil, err
	}

	ptx, err := ix.postingsStore.Begin(true)
	if err != nil {
		return ids, err
	}

	for i, id := range ids {
		if skeys[i] == nil {
			continue
		}

		for _, k := range skeys[i] {
			if err := ptx.append(k, id); err != nil {
				ptx.Rollback()
				return ids, err
			}
		}
	}

	return ids, ptx.Commit()
}

func (ix *index) See(ts time.Time, id uint64) error {
	tx, err := ix.timelineStore.Begin(true)
	if err != nil {
		return err
	}
	if err := tx.SetDiff(ts, diffStateAdd, id); err != nil {
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
	if err := tx.SetDiff(ts, diffStateDel, id); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (ix *index) Instant(ts time.Time, ms ...Matcher) ([]uint64, error) {
	ptx, err := ix.postingsStore.Begin(false)
	if err != nil {
		return nil, err
	}
	defer ptx.Rollback()

	var its []iterator
	for _, m := range ms {
		ids, err := ix.labels.Search(m)
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

	fmt.Println(expandIterator(intersect(its...)))

	it, err := ttx.Instant(ts)
	if err != nil {
		return nil, err
	}
	its = append(its, it)

	res, err := expandIterator(intersect(its...))
	return res, err
}

func (ix *index) Range(start, end time.Time, ms ...Matcher) ([]uint64, error) {
	ptx, err := ix.postingsStore.Begin(false)
	if err != nil {
		return nil, err
	}
	defer ptx.Rollback()

	var its []iterator
	for _, m := range ms {
		ids, err := ix.labels.Search(m)
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

	it, err := ttx.Range(start, end)
	if err != nil {
		return nil, err
	}
	its = append(its, it)

	res, err := expandIterator(intersect(its...))
	return res, err
}
