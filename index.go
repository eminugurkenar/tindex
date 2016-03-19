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

	if _, err := tx.CreateBucketIfNotExists(bucketLabels); err != nil {
		return nil, err
	}
	if _, err := tx.CreateBucketIfNotExists(bucketPostskip); err != nil {
		return nil, err
	}
	if _, err := tx.CreateBucketIfNotExists(bucketSeries); err != nil {
		return nil, err
	}

	ix := &Index{
		db: db,
	}

	return ix, nil
}

var (
	bucketLabels   = []byte("labels")
	bucketSeries   = []byte("series")
	bucketPostskip = []byte("postskip")
)

// seperator is a byte that cannot occur in a valid UTF-8 sequence. It can thus
// be used to mark boundaries between serialized strings.
const seperator = '\xff'

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

	b := tx.Bucket(bucketLabels)

	ids := make([]uint64, len(keys))
	for i, k := range keys {
		buf := make([]byte, binary.MaxVarintLen64)
		var id uint64
		if v := b.Get(k); v != nil {
			id, _ = binary.Uvarint(v)
		} else {
			id, err = b.NextSequence()
			if err != nil {
				return nil, err
			}
			n := binary.PutUvarint(buf, id)
			if err := b.Put(k, buf[:n]); err != nil {
				tx.Rollback()
				return nil, err
			}
		}
		ids[i] = id
	}

	return ids, tx.Commit()
}

func (ix *Index) GetSeries(key []byte) (map[string]string, error) {
	tx, err := ix.db.Begin(false)
	if err != nil {
		return nil, err
	}

	b := tx.Bucket(bucketSeries)
	v := b.Get(key)
	if v == nil {
		return nil, fmt.Errorf("not found")
	}

	m := map[string]string{}
	r := bytes.NewReader(v)

	var l uint64
	for r.Len() > 0 {
		l, _, err = readUvarint(r)
		if err != nil {
			break
		}
		buf := make([]byte, int(l))
		if _, err = r.Read(buf); err != nil {
			break
		}
		ln := string(buf)

		l, _, err = readUvarint(r)
		if err != nil {
			break
		}
		buf = make([]byte, int(l))
		if _, err = r.Read(buf); err != nil {
			break
		}
		lv := string(buf)

		m[ln] = lv
	}

	return m, err
}

func (ix *Index) EnsureSeries(labels map[string]string) (sskey []byte, err error) {
	key, err := ix.ensureLabels(labels)
	if err != nil {
		return nil, err
	}
	skey := seriesKey(key)
	sort.Sort(skey)
	fmt.Println(skey)

	tx, err := ix.db.Begin(true)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		} else {
			tx.Commit()
		}
	}()
	b := tx.Bucket(bucketSeries)

	if b.Get(skey.bytes()) != nil {
		return skey.bytes(), nil
	}

	buf := &bytes.Buffer{}
	for ln, lv := range labels {
		if _, err = writeUvarint(buf, uint64(len(ln))); err != nil {
			return nil, err
		}
		if _, err = buf.WriteString(ln); err != nil {
			return nil, err
		}
		if _, err = writeUvarint(buf, uint64(len(lv))); err != nil {
			return nil, err
		}
		if _, err = buf.WriteString(lv); err != nil {
			return nil, err
		}
	}
	return skey.bytes(), b.Put(skey.bytes(), buf.Bytes())
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
