package tindex

import (
	"encoding/binary"
	"io/ioutil"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/boltdb/bolt"
)

func newBoltTestDB(t *testing.T) *bolt.DB {
	dir, err := ioutil.TempDir("", "bolt_test")
	if err != nil {
		t.Fatal(err)
	}
	db, err := bolt.Open(filepath.Join(dir, "test.db"), 0666, nil)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func TestBoltTimelineIterator(t *testing.T) {
	type diff struct {
		val   uint64
		ts    uint64
		exist bool
	}

	var cases = []struct {
		diffs []diff
		res   []uint64
		ts    uint64
	}{
		{
			diffs: []diff{
				{100, 10, true},
				{100, 20, false},
			},
			res: []uint64{},
			ts:  1000,
		},
		{
			diffs: []diff{
				{100, 10, true},
				{100, 20, false},
				{100, 21, true},
				{100, 1001, false},
			},
			res: []uint64{100},
			ts:  1000,
		},
		{
			diffs: []diff{
				{100, 10, true},
				{100, 20, false},
				{100, 21, true},
			},
			res: []uint64{},
			ts:  20,
		},
		{
			diffs: []diff{
				{100, 10, true},
				{100, 50, false},
				{2, 50, true},
				{123, 5, true},
				{123, 20, false},
				{123, 49, true},
				{99, 1, false},
				{99, 2, true},
				{1, 51, true},
				{2, 51, false},
			},
			res: []uint64{2, 99, 123},
			ts:  50,
		},
	}

	for i, c := range cases {
		db := newBoltTestDB(t)
		defer db.Close()

		tx, err := db.Begin(true)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()

		b, err := tx.CreateBucket([]byte("test"))
		if err != nil {
			t.Fatal(err)
		}

		for _, d := range c.diffs {
			buf := make([]byte, 17)
			binary.BigEndian.PutUint64(buf[:8], d.val)
			binary.BigEndian.PutUint64(buf[8:], d.ts)
			if d.exist {
				buf[16] = 1
			}
			if err := b.Put(buf[:16], buf[16:]); err != nil {
				t.Fatal(err)
			}
		}
		ts := make([]byte, 8)
		binary.BigEndian.PutUint64(ts, c.ts)

		it := &boltTimelineIterator{
			c:  b.Cursor(),
			ts: ts,
		}
		res, err := expandIterator(it)
		if err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}

		if !reflect.DeepEqual(res, c.res) {
			t.Errorf("Result did not match, case %d:", i+1)
			t.Errorf("Expected: %v", c.res)
			t.Fatalf("Received: %v", res)
		}
	}
}
