package tindex

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/boltdb/bolt"
)

// A set of labels with unique label names.
type Set map[string]string

// Sets stores sets of labels.
type Sets interface {
	// Ensure that the given label sets are registered in the storage.
	// Returns the IDs and SetKeys in order of the inserted sets.
	Ensure(sets ...Set) ([]uint64, []SetKey, error)
	// Get the label sets with the given IDs in order.
	Get(ids ...uint64) ([]Set, error)
	// Close the LabelSets storage.
	Close() error
}

var (
	bktLabelSets   = []byte("label_sets")
	bktLabelSetIDs = []byte("label_set_ids")
)

// NewLabelsSets returns a new Sets store based on BoltDB and the given Labels store.
func NewLabelSets(path string, labels Labels) (Sets, error) {
	if err := os.MkdirAll(path, 0777); err != nil {
		return nil, err
	}
	db, err := bolt.Open(filepath.Join(path, "labelsets.db"), 0666, nil)
	if err != nil {
		return nil, err
	}
	db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(bktLabelSets); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists(bktLabelSetIDs); err != nil {
			return err
		}
		return nil
	})

	return &labelSetsStore{
		db:     db,
		labels: labels,
	}, nil
}

type labelSetsStore struct {
	db     *bolt.DB
	labels Labels
}

func (s *labelSetsStore) Ensure(sets ...Set) ([]uint64, []SetKey, error) {
	var all Pairs
	for _, set := range sets {
		for k, v := range set {
			all = append(all, Pair{k, v})
		}
	}

	lids, err := s.labels.Ensure(all...)
	if err != nil {
		return nil, nil, err
	}

	ids := make([]uint64, 0, len(sets))
	sks := make([]SetKey, 0, len(sets))

	txbufs := txbuffs{
		buffers: &encpool,
	}
	defer txbufs.release()

	err = s.db.Update(func(tx *bolt.Tx) error {
		var (
			bids   = tx.Bucket(bktLabelSetIDs)
			blsets = tx.Bucket(bktLabelSets)
		)
		off := 0

		for _, set := range sets {
			sk := SetKey(lids[off : off+len(set)])

			sort.Sort(sk)
			skb := sk.bytes()
			txbufs.put(skb)

			var id uint64

			// The label set has never seen before, create it.
			if idb := blsets.Get(skb); idb == nil {
				id, err = bids.NextSequence()
				if err != nil {
					return err
				}
				idb := txbufs.uvarint(id)

				if err := blsets.Put(skb, idb); err != nil {
					return err
				}
				if err := bids.Put(idb, skb); err != nil {
					return err
				}
			} else {
				id, _ = binary.Uvarint(idb)
				// The set key must be nil for sets that are not new.
				sk = nil
			}

			ids = append(ids, id)
			sks = append(sks, sk)
			off += len(set)
		}
		return nil
	})
	return ids, sks, err
}

func (s *labelSetsStore) Get(ids ...uint64) ([]Set, error) {
	tx, err := s.db.Begin(false)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	bids := tx.Bucket(bktLabelSetIDs)
	if bids == nil {
		return nil, fmt.Errorf("labels bucket missing")
	}
	var (
		lsets = make([]Set, 0, len(ids))
		lens  = make([]int, 0, len(ids))
		lids  = make([]uint64, 0, len(ids)*5)
	)
	for _, id := range ids {
		idb := make([]byte, binary.MaxVarintLen64)
		idb = idb[:binary.PutUvarint(idb, id)]

		b := bids.Get(idb)
		if b == nil {
			return nil, fmt.Errorf("labelset for ID %q not found", id)
		}
		k, err := newSetKey(b)
		if err != nil {
			return nil, err
		}
		lids = append(lids, k...)
		lens = append(lens, len(k))
	}

	pairs, err := s.labels.Get(lids...)
	if err != nil {
		return nil, err
	}

	for _, l := range lens {
		lset := make(Set, l)
		lsets = append(lsets, lset)

		for i := 0; i < l; i++ {
			p := pairs[i]
			lset[p.Key] = p.Val
		}
		pairs = pairs[l:]
	}

	return lsets, nil
}

func (s *labelSetsStore) Close() error {
	return s.db.Close()
}

// SetKey is a list of label IDs that uniquely identify a Set if sorted.
type SetKey []uint64

func newSetKey(b []byte) ([]uint64, error) {
	var sk SetKey
	for len(b) > 0 {
		k, n := binary.Uvarint(b)
		sk = append(sk, k)
		b = b[n:]
	}
	return sk, nil
}

func (k SetKey) Len() int           { return len(k) }
func (k SetKey) Swap(i, j int)      { k[i], k[j] = k[j], k[i] }
func (k SetKey) Less(i, j int) bool { return k[i] < k[j] }

func (k SetKey) bytes() []byte {
	var (
		buf = encpool.get(len(k) * binary.MaxVarintLen64)
		// buf = make([]byte, len(k)*binary.MaxVarintLen64)
		n int
	)
	for _, x := range k {
		n += binary.PutUvarint(buf[n:], x)
	}
	return buf[:n]
}
