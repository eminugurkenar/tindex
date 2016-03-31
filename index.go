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
	Instant(at time.Time, ms ...Matcher) (Iterator, error)
	Range(from, to time.Time, ms ...Matcher) (Iterator, error)

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

	createTimelineStore, ok := timelineStores[opts.TimelineStore]
	if !ok {
		return nil, fmt.Errorf("unknown timeline store %q", opts.TimelineStore)
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
	ps, err := NewPostings(filepath.Join(path, "postings"))
	if err != nil {
		return nil, err
	}

	ix := &index{
		labels:        ls,
		labelSets:     lss,
		postings:      ps,
		timelineStore: ts,
	}
	return ix, nil
}

// Constructors for registered stores.
var (
	timelineStores = map[string]func(path string) (timelineStore, error){}
)

// Tx is a generic interface for transactions.
type Tx interface {
	Commit() error
	Rollback() error
}

type timelineStore interface {
	Begin(writable bool) (timelineTx, error)
	Close() error
}

type timelineTx interface {
	Tx
	// Returns an iterator of document IDs that were valid at the given time.
	Instant(t time.Time) (Iterator, error)
	// Returns an iterator of document IDs that were valid at some point
	// during the given range.
	Range(start, end time.Time) (Iterator, error)

	SetDiff(t time.Time, s diffState, ids ...uint64) error
}

// index implements the Index interface.
type index struct {
	opts *Options

	labelSets     Sets
	labels        Labels
	postings      Postings
	timelineStore timelineStore
}

func (ix *index) Close() error {
	if err := ix.labelSets.Close(); err != nil {
		return err
	}
	if err := ix.labels.Close(); err != nil {
		return err
	}
	return ix.postings.Close()
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

	batches := PostingsBatches{}

	// The SetKey for existing sets are nil. New ones are returned in increasing
	// order. Thus, we can create batches in order of the IDs.
	for i, id := range ids {
		if skeys[i] == nil {
			continue
		}
		if _, ok := batches[id]; ok {
			return nil, fmt.Errorf("Batch for ID %q already exists", id)
		}
		batches[id] = append(batches[id], id)
	}

	return ids, ix.postings.Append(batches)
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

func (ix *index) matchIter(matchers ...Matcher) (Iterator, error) {
	// The union of iterators for a single matcher are merged.
	// The merge iterators of each matcher are then intersected.
	its := make([]Iterator, 0, len(matchers))

	for _, m := range matchers {
		keys, err := ix.labels.Search(m)
		if err != nil {
			return nil, err
		}
		mits := make([]Iterator, 0, len(keys))

		for _, k := range keys {
			it, err := ix.postings.Iter(k)
			if err != nil {
				return nil, err
			}
			mits = append(mits, it)
		}

		its = append(its, Merge(mits...))
	}

	return Intersect(its...), nil
}

func (ix *index) Instant(ts time.Time, ms ...Matcher) (Iterator, error) {
	mit, err := ix.matchIter(ms...)
	if err != nil {
		return nil, err
	}

	ttx, err := ix.timelineStore.Begin(false)
	if err != nil {
		return nil, err
	}
	defer ttx.Rollback()

	it, err := ttx.Instant(ts)

	return Intersect(mit, it), err
}

func (ix *index) Range(start, end time.Time, ms ...Matcher) (Iterator, error) {
	mit, err := ix.matchIter(ms...)
	if err != nil {
		return nil, err
	}

	ttx, err := ix.timelineStore.Begin(false)
	if err != nil {
		return nil, err
	}
	defer ttx.Rollback()

	it, err := ttx.Range(start, end)

	return Intersect(mit, it), err
}
