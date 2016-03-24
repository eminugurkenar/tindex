package tsindex

import (
	// "bytes"
	"encoding/binary"
	// "errors"
	// "path/filepath"
	"regexp"
	// "sort"
	// "time"
)

type Index interface {
	// Instant(m []Matcher, at time.Time) ([]uint64, error)
	// Range(m []Matcher, from, to time.Time) ([]uint64, error)

	Series(id uint64) (map[string]string, error)
	EnsureSeries(labels map[string]string) (uint64, error)

	// See(id uint64, ts uint64) error
	// Unsee(id uint64, ts uint64) error
}

type Options struct {
	SeriesStore string
}

var DefaultOptions = &Options{
	SeriesStore: "bolt",
}

func Open(path string, opts *Options) (Index, error) {
	// Use default options if none are provided.
	if opts == nil {
		opts = DefaultOptions
	}

	ss, err := seriesStores[opts.SeriesStore](path)
	if err != nil {
		return nil, err
	}

	ix := &index{
		seriesStore: ss,
	}
	return ix, nil
}

// seperator is a byte that cannot occur in a valid UTF-8 sequence. It can thus
// be used to mark boundaries between serialized strings.
const seperator = byte('\xff')

// func intersect(c1, c2 postingsCursor) []uint64 {
// 	var result []uint64
// 	v1, e1 := c1.seek(0)
// 	v2, e2 := c2.seek(0)
// 	for {
// 		if e1 != nil || e2 != nil {
// 			break
// 		}
// 		if v1 < v2 {
// 			v1, e1 = c1.seek(v2)
// 		} else if v2 < v1 {
// 			v2, e2 = c2.seek(v1)
// 		} else {
// 			result = append(result, uint64(v2))
// 			v1, e1 = c1.next()
// 			v2, e2 = c2.next()
// 		}
// 	}

// 	return result
// }

// func Intersect(cs ...postingsCursor) postingsCursor {
// 	return nil
// }

// func expandCursor(c postingsCursor) uint64 {
// 	return 0
// }

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

// Constructors for registeres seriesStores.
var seriesStores = map[string]func(path string) (seriesStore, error){}

// A seriesStore can start a read or write seriesTx.
type seriesStore interface {
	Begin(writeable bool) (seriesTx, error)
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

// index implements the Index interface.
type index struct {
	opts        *Options
	seriesStore seriesStore
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
	tx, err := ix.seriesStore.Begin(true)
	if err != nil {
		return 0, err
	}
	sid, _, err = tx.ensureSeries(labels)
	if err != nil {
		tx.Rollback()
		return 0, err
	}
	tx.Commit()

	// TODO(fabxc): add label IDs to postings lists.

	return sid, err
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
