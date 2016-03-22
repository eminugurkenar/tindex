package tsindex

import (
	// "bytes"
	"encoding/binary"
	"errors"
	// "fmt"

	"github.com/boltdb/bolt"
)

var errNotFound = errors.New("not found")
var errOutOfOrder = errors.New("out of order")

type pkey uint64

func (k pkey) bytes() []byte { return encodeUint64(uint64(k)) }

type skiplist interface {
	next() (docid, pgid, error)
	seek(docid) (docid, pgid, error)
	append(docid, pgid) error
}

type skiplistStore interface {
	get(pkey) (skiplist, error)
}

type boltSkiplistStore struct {
	bkt *bolt.Bucket
}

func (s *boltSkiplistStore) get(k pkey) (skiplist, error) {
	b, err := s.bkt.CreateBucketIfNotExists(k.bytes())
	if err != nil {
		return nil, err
	}
	return newSkiplistBolt(b), nil
}

type skiplistBolt struct {
	bkt *bolt.Bucket
	c   *bolt.Cursor
}

func newSkiplistBolt(bkt *bolt.Bucket) *skiplistBolt {
	return &skiplistBolt{
		bkt: bkt,
		c:   bkt.Cursor(),
	}
}

func (s *skiplistBolt) next() (docid, pgid, error) {
	db, pb := s.c.Next()
	if db == nil {
		return 0, 0, errNotFound
	}
	return docid(decodeUint64(db)), pgid(decodeUint64(pb)), nil
}

func (s *skiplistBolt) seek(d docid) (docid, pgid, error) {
	db, pb := s.c.Seek(d.bytes())
	if db == nil {
		db, pb = s.c.Last()
		if db == nil {
			return 0, 0, errNotFound
		}
	}
	did, pid := docid(decodeUint64(db)), pgid(decodeUint64(pb))

	if did > d {
		db, pb = s.c.Prev()
		if db == nil {
			return 0, 0, errNotFound
		}
		did, pid = docid(decodeUint64(db)), pgid(decodeUint64(pb))
	}
	return did, pid, nil
}

func (s *skiplistBolt) append(d docid, p pgid) error {
	k, _ := s.c.Last()

	if k != nil && binary.BigEndian.Uint64(k) >= uint64(d) {
		return errOutOfOrder
	}

	buf := make([]byte, 16)
	binary.BigEndian.PutUint64(buf[:8], uint64(d))
	binary.BigEndian.PutUint64(buf[8:], uint64(p))

	return s.bkt.Put(buf[:8], buf[8:])
}
