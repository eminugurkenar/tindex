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
	Sets(id ...uint64) ([]Set, error)
	EnsureSets(ls ...Set) ([]uint64, error)

	Active(ts time.Time, id ...uint64) error
	Inactive(ts time.Time, id ...uint64) error

	Instant(time.Time, ...Matcher) (Iterator, error)
	Range(time.Time, time.Duration, ...Matcher) (Iterator, error)

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

	tps, err := NewPostings(filepath.Join(path, "timeline"))
	if err != nil {
		return nil, err
	}
	tl, err := NewTimeline(filepath.Join(path, "timeline"), tps)
	if err != nil {
		return nil, err
	}

	ix := &index{
		labels:    ls,
		labelSets: lss,
		postings:  ps,
		timeline:  tl,
	}
	return ix, nil
}

// index implements the Index interface.
type index struct {
	opts *Options

	labelSets Sets
	labels    Labels
	postings  Postings
	timeline  Timeline
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
		for _, k := range skeys[i] {
			batches[k] = append(batches[k], id)
		}
	}

	return ids, ix.postings.Append(batches)
}

func (ix *index) Active(ts time.Time, ids ...uint64) error {
	return ix.timeline.Active(ts, ids...)
}

func (ix *index) Inactive(ts time.Time, ids ...uint64) error {
	return ix.timeline.Inactive(ts, ids...)
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
		return nil, fmt.Errorf("getting matchers failed: %s", err)
	}
	tlit, err := ix.timeline.Instant(ts)
	if err != nil {
		return nil, fmt.Errorf("getting timeline iterator failed: %s", err)
	}

	return Intersect(mit, tlit), nil
}

func (ix *index) Range(ts time.Time, dur time.Duration, ms ...Matcher) (Iterator, error) {
	mit, err := ix.matchIter(ms...)
	if err != nil {
		return nil, fmt.Errorf("getting matchers failed: %s", err)
	}

	tlit, err := ix.timeline.Range(ts, dur)
	if err != nil {
		return nil, fmt.Errorf("getting timeline iterator failed: %s", err)
	}

	return Intersect(tlit, mit), nil
}
