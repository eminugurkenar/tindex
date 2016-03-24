package tsindex

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"sort"

	"github.com/boltdb/bolt"
)

func init() {
	if _, ok := seriesStores["bolt"]; ok {
		panic("bolt series store initialized twice")
	}
	seriesStores["bolt"] = newBoltSeriesStore
}

var (
	bucketLabelToID  = []byte("label_to_id")
	bucketIDToLabel  = []byte("id_to_label")
	bucketSeriesToID = []byte("series_to_id")
	bucketIDToSeries = []byte("id_to_series")
)

// newBoltSeriesStore initializes a Bolt-based seriesStore under path.
func newBoltSeriesStore(path string) (seriesStore, error) {
	db, err := bolt.Open(filepath.Join(path, "series.db"), 0666, nil)
	if err != nil {
		return nil, err
	}
	s := &boltSeriesStore{
		db: db,
	}
	err = db.Update(func(tx *bolt.Tx) error {
		if _, err = tx.CreateBucketIfNotExists(bucketLabelToID); err != nil {
			return err
		}
		if _, err = tx.CreateBucketIfNotExists(bucketIDToLabel); err != nil {
			return err
		}
		if _, err = tx.CreateBucketIfNotExists(bucketSeriesToID); err != nil {
			return err
		}
		if _, err = tx.CreateBucketIfNotExists(bucketIDToSeries); err != nil {
			return err
		}
		return nil
	})
	return s, err
}

// boltSeriesStore implements a seriesStore based on Bolt.
type boltSeriesStore struct {
	db *bolt.DB
}

// Begin implements the seriesStore interface.
func (s *boltSeriesStore) Begin(writeable bool) (seriesTx, error) {
	tx, err := s.db.Begin(writeable)
	if err != nil {
		return nil, err
	}
	return &boltSeriesTx{
		Tx:         tx,
		IDToSeries: tx.Bucket(bucketIDToSeries),
		seriesToID: tx.Bucket(bucketSeriesToID),
		IDToLabel:  tx.Bucket(bucketIDToLabel),
		labelToID:  tx.Bucket(bucketLabelToID),
	}, nil
}

// boltSeriesTx implements a seriesTx.
type boltSeriesTx struct {
	*bolt.Tx

	seriesToID, IDToSeries *bolt.Bucket
	labelToID, IDToLabel   *bolt.Bucket
}

// series implements the seriesTx interface.
func (s *boltSeriesTx) series(sid uint64) (map[string]string, error) {
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(buf, sid)

	series := s.IDToSeries.Get(buf[:n])
	if series == nil {
		return nil, errNotFound
	}
	var ids []uint64
	r := bytes.NewReader(series)
	for r.Len() > 0 {
		id, _, err := readUvarint(r)
		if err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}

	m := map[string]string{}

	for _, id := range ids {
		k, v, err := s.label(id)
		if err != nil {
			return nil, err
		}
		m[k] = v
	}
	return m, nil
}

// ensureSeries implements the seriesTx interface.
func (s *boltSeriesTx) ensureSeries(desc map[string]string) (uint64, seriesKey, error) {
	// Ensure that all labels are persisted.
	var skey seriesKey
	for k, v := range desc {
		key, err := s.ensureLabel(k, v)
		if err != nil {
			return 0, nil, err
		}
		skey = append(skey, key)
	}
	sort.Sort(skey)

	// Check whether we have seen the series before.
	var sid uint64
	if sidb := s.IDToSeries.Get(skey.bytes()); sidb != nil {
		sid, _ := binary.Uvarint(sidb)
		// TODO(fabxc): validate.
		return sid, skey, nil
	}

	// We haven't seen this series before, create a new ID.
	sid, err := s.IDToSeries.NextSequence()
	if err != nil {
		return 0, nil, err
	}
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(buf, sid)

	if err := s.IDToSeries.Put(buf[:n], skey.bytes()); err != nil {
		return 0, nil, err
	}

	return sid, skey, nil
}

// label retrieves the key/value label associated with id.
func (s *boltSeriesTx) label(id uint64) (string, string, error) {
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(buf, id)
	label := s.IDToLabel.Get(buf[:n])
	if label == nil {
		return "", "", fmt.Errorf("label for ID %q not found", buf[:n])
	}
	p := bytes.Split(label, []byte{seperator})

	return string(p[0]), string(p[1]), nil
}

// ensureLabel returns a unique ID for the label. If the label was not seen
// before a new, monotonically increasing ID is returned.
func (s *boltSeriesTx) ensureLabel(key, val string) (uint64, error) {
	k := make([]byte, len(key)+len(val)+1)

	copy(k[:len(key)], []byte(val))
	k[len(key)] = seperator
	copy(k[len(key)+1:], []byte(val))

	var err error
	var id uint64
	if v := s.labelToID.Get(k); v != nil {
		id, _ = binary.Uvarint(v)
	} else {
		id, err = s.IDToLabel.NextSequence()
		if err != nil {
			return 0, err
		}
		buf := make([]byte, binary.MaxVarintLen64)
		n := binary.PutUvarint(buf, id)
		if err := s.labelToID.Put(k, buf[:n]); err != nil {
			return 0, err
		}
		if err := s.IDToLabel.Put(buf[:n], k); err != nil {
			return 0, err
		}
	}

	return id, nil
}

func (s *boltSeriesTx) labels(m Matcher) (ids []uint64, err error) {
	c := s.labelToID.Cursor()

	for lbl, id := c.Seek([]byte(m.Key())); bytes.HasPrefix(lbl, []byte(m.Key())); lbl, id = c.Next() {
		p := bytes.Split(lbl, []byte{seperator})
		if !m.Match(string(p[1])) {
			continue
		}
		ids = append(ids, binary.BigEndian.Uint64(id))
	}

	return ids, nil
}
