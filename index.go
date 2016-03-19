package tsindex

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"regexp"
	"sort"

	"github.com/boltdb/bolt"
)

// type Index interface {
// 	// Retrieve a set of IDs which's documents match m at the given time.
// 	GetInstant(m []Matcher, ts uint64) (Set, error)
// 	// Retrieve a set of IDs within the from/to range that which's documents
// 	// match m.
// 	GetRange(m []Matcher, from, to uint64) (Set, error)
// 	// Set checks whether the name with the given dimensions as set for
// 	// time ts. It returns a true boolean iff the entry already existed
// 	Index(name string, labels map[string]string, id uint64) (bool, error)

// 	IndexTime(name string, labels map[string]string, ts uint64) error
// 	UindexTime(name string, labels map[string]string, ts uint64) error
// }

type Index struct {
	db *bolt.DB
}

type Options struct {
}

var DefaultOptions = &Options{}

const boltFile = "index.db"

func Open(path string, opts *Options) (*Index, error) {
	// Use default options if none are provided.
	if opts == nil {
		opts = DefaultOptions
	}

	db, err := bolt.Open(filepath.Join(path, boltFile), 0666, nil)
	if err != nil {
		return nil, err
	}
	tx, err := db.Begin(true)
	if err != nil {
		return nil, err
	}
	defer tx.Commit()

	for _, bn := range bucketNames {
		if _, err := tx.CreateBucketIfNotExists(bn); err != nil {
			return nil, err
		}
	}

	ix := &Index{
		db: db,
	}

	return ix, nil
}

var (
	bucketLabelsToID = []byte("labels_to_id")
	bucketIDToLabels = []byte("id_to_labels")
	bucketSeriesToID = []byte("series_to_id")
	bucketIDToSeries = []byte("id_to_series")

	bucketNames = [][]byte{
		bucketLabelsToID,
		bucketIDToLabels,
		bucketSeriesToID,
		bucketIDToSeries,
	}
)

// seperator is a byte that cannot occur in a valid UTF-8 sequence. It can thus
// be used to mark boundaries between serialized strings.
const seperator = byte('\xff')

func (ix *Index) IndexSeries(sid, ts uint64) error {
	return nil
}

func (ix *Index) UnindexSeries(sid, ts uint64) error {
	return nil
}

func (ix *Index) ensureLabels(labels map[string]string) ([]uint64, error) {
	// Serialize labels into byte keys.
	keys := make([][]byte, 0, len(labels))
	for ln, lv := range labels {
		k := make([]byte, len(ln)+len(lv)+1)

		copy(k[:len(ln)], []byte(ln))
		k[len(ln)] = seperator
		copy(k[len(ln)+1:], []byte(lv))

		keys = append(keys, k)
	}

	tx, err := ix.db.Begin(true)
	if err != nil {
		return nil, err
	}

	bl := tx.Bucket(bucketLabelsToID)
	bi := tx.Bucket(bucketIDToLabels)

	ids := make([]uint64, len(keys))
	for i, k := range keys {
		var id uint64
		if v := bl.Get(k); v != nil {
			id, _ = binary.Uvarint(v)
		} else {
			id, err = bl.NextSequence()
			if err != nil {
				return nil, err
			}
			buf := make([]byte, binary.MaxVarintLen64)
			n := binary.PutUvarint(buf, id)
			if err := bl.Put(k, buf[:n]); err != nil {
				tx.Rollback()
				return nil, err
			}
			if err := bi.Put(buf[:n], k); err != nil {
				tx.Rollback()
				return nil, err
			}
		}
		ids[i] = id
	}

	return ids, tx.Commit()
}

func (ix *Index) GetSeries(sid uint64) (map[string]string, error) {
	tx, err := ix.db.Begin(false)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(buf, sid)

	series := tx.Bucket(bucketIDToSeries).Get(buf[:n])
	if series == nil {
		return nil, fmt.Errorf("not found %v", buf[:n])
	}
	var ids []uint64
	var id uint64
	r := bytes.NewReader(series)
	for r.Len() > 0 {
		id, _, err = readUvarint(r)
		if err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}

	m := map[string]string{}

	bl := tx.Bucket(bucketIDToLabels)

	for _, id := range ids {
		buf := make([]byte, binary.MaxVarintLen64)
		n := binary.PutUvarint(buf, id)
		label := bl.Get(buf[:n])
		if label == nil {
			return nil, fmt.Errorf("not found")
		}
		p := bytes.Split(label, []byte{seperator})
		m[string(p[0])] = string(p[1])
	}

	return m, nil
}

func (ix *Index) EnsureSeries(labels map[string]string) (sid uint64, err error) {
	key, err := ix.ensureLabels(labels)
	if err != nil {
		return 0, err
	}
	skey := seriesKey(key)
	sort.Sort(skey)

	tx, err := ix.db.Begin(true)
	if err != nil {
		return 0, err
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		} else {
			tx.Commit()
		}
	}()
	b := tx.Bucket(bucketSeriesToID)

	if sidb := b.Get(skey.bytes()); sidb != nil {
		sid, _ := binary.Uvarint(sidb)
		return sid, nil
	}

	sid, err = b.NextSequence()
	if err != nil {
		return 0, err
	}
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(buf, sid)

	b = tx.Bucket(bucketIDToSeries)

	return sid, b.Put(buf[:n], skey.bytes())
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

// Sync all unpersisted changes to disk.
func (ix *Index) Sync() error {
	return ix.db.Sync()
}

// Close the index.
func (ix *Index) Close() error {
	return ix.db.Close()
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

func writeUvarint(w io.ByteWriter, x uint64) (i int, err error) {
	for x >= 0x80 {
		if err = w.WriteByte(byte(x) | 0x80); err != nil {
			return i, err
		}
		x >>= 7
		i++
	}
	if err = w.WriteByte(byte(x)); err != nil {
		return i, err
	}
	return i + 1, err
}

func writeVarint(w io.ByteWriter, x int64) (i int, err error) {
	ux := uint64(x) << 1
	if x < 0 {
		ux = ^ux
	}
	return writeUvarint(w, ux)
}

func readUvarint(r io.ByteReader) (uint64, int, error) {
	var (
		x uint64
		s uint
	)
	for i := 0; ; i++ {
		b, err := r.ReadByte()
		if err != nil {
			return x, i, err
		}
		if b < 0x80 {
			if i > 9 || i == 9 && b > 1 {
				return x, i + 1, errors.New("varint overflows a 64-bit integer")
			}
			return x | uint64(b)<<s, i + 1, nil
		}
		x |= uint64(b&0x7f) << s
		s += 7
	}
}

func readVarint(r io.ByteReader) (int64, int, error) {
	ux, n, err := readUvarint(r)
	x := int64(ux >> 1)
	if ux&1 != 0 {
		x = ^x
	}
	return x, n, err
}
