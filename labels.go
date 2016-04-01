package tindex

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"regexp"

	"github.com/boltdb/bolt"
)

var (
	bktLabelIDs = []byte("label_ids")
	bktLabels   = []byte("labels")
)

type Pair struct {
	Key, Val string
}

// Labels stores and retrieves batches of label pairs and assigns them unique IDs.
type Labels interface {
	// Ensure stores the pairs if they were not stored already.
	// And returns their IDs in order of the given pairs.
	Ensure(pairs ...Pair) (ids []uint64, err error)
	// Get retrieves pairs for IDs in order of the provided IDs.
	Get(ids ...uint64) (pairs []Pair, err error)
	// Searched retrieves IDs for labels that satisfy the given Matcher.
	Search(m Matcher) (ids []uint64, err error)
	// Close the labels storage.
	Close() error
}

type labelsStore struct {
	db *bolt.DB
}

func NewLabels(path string) (Labels, error) {
	if err := os.MkdirAll(path, 0777); err != nil {
		return nil, err
	}
	db, err := bolt.Open(filepath.Join(path, "labels.db"), 0666, nil)
	if err != nil {
		return nil, err
	}
	err = db.Update(func(tx *bolt.Tx) error {
		if _, err = tx.CreateBucketIfNotExists(bktLabels); err != nil {
			return err
		}
		if _, err = tx.CreateBucketIfNotExists(bktLabelIDs); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return &labelsStore{db: db}, nil
}

var separator = byte('\xff')

func (s *labelsStore) Close() error {
	return s.db.Close()
}

func (s *labelsStore) Ensure(pairs ...Pair) (ids []uint64, err error) {
	err = s.db.Update(func(tx *bolt.Tx) error {
		var (
			blabels = tx.Bucket(bktLabels)
			bids    = tx.Bucket(bktLabelIDs)
		)
		if blabels == nil {
			return fmt.Errorf("labels bucket not found")
		}
		if bids == nil {
			return fmt.Errorf("ids bucket not found")
		}

		ids = make([]uint64, 0, len(pairs))

		for _, pair := range pairs {
			var id uint64

			k := make([]byte, len(pair.Key)+len(pair.Val)+1)

			copy(k[:len(pair.Key)], []byte(pair.Key))
			k[len(pair.Key)] = separator
			copy(k[len(pair.Key)+1:], []byte(pair.Val))

			if idb := blabels.Get(k); idb == nil {
				id, err = bids.NextSequence()
				if err != nil {
					return err
				}

				idb = make([]byte, binary.MaxVarintLen64)
				idb = idb[:binary.PutUvarint(idb, id)]

				if err := blabels.Put(k, idb); err != nil {
					return err
				}
				if err := bids.Put(idb, k); err != nil {
					return err
				}
			} else {
				id, _ = binary.Uvarint(idb)
			}

			ids = append(ids, id)
		}
		return nil
	})

	return ids, err
}

func (s *labelsStore) Get(ids ...uint64) (pairs []Pair, err error) {
	tx, err := s.db.Begin(false)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	bids := tx.Bucket(bktLabelIDs)
	if bids == nil {
		return nil, fmt.Errorf("labels bucket missing")
	}

	pairs = make([]Pair, 0, len(ids))
	for _, id := range ids {
		idb := make([]byte, binary.MaxVarintLen64)
		idb = idb[:binary.PutUvarint(idb, id)]

		label := bids.Get(idb)
		if label == nil {
			return nil, fmt.Errorf("label for ID %q not found", id)
		}
		p := bytes.Split(label, []byte{separator})

		pairs = append(pairs, Pair{
			Key: string(p[0]),
			Val: string(p[1]),
		})

	}
	return pairs, nil
}

func (s *labelsStore) Search(m Matcher) (ids []uint64, err error) {
	tx, err := s.db.Begin(false)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	blabels := tx.Bucket(bktLabels)
	if blabels == nil {
		return nil, fmt.Errorf("labels bucket missing")
	}

	c := blabels.Cursor()

	for lbl, idb := c.Seek([]byte(m.Key())); bytes.HasPrefix(lbl, []byte(m.Key())); lbl, idb = c.Next() {
		p := bytes.Split(lbl, []byte{separator})
		if !m.Match(string(p[1])) {
			continue
		}
		id, _ := binary.Uvarint(idb)
		ids = append(ids, id)
	}

	return ids, nil
}

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
