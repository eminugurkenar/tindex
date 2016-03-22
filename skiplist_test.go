package tsindex

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	// "sync"
	"testing"

	"github.com/boltdb/bolt"
)

type testData struct {
	k      pkey
	v      docid
	offset pgid
}

func generateData(n int, maxKey uint32) []testData {
	data := make([]testData, n)

	for i := range data {
		data[i].k = pkey(rand.Intn(int(maxKey)))
		data[i].v = docid(1000000 + i)
		data[i].offset = pgid(rand.Int63n(1000))
	}

	return data
}

func TestSkiplistStore(t *testing.T) {
	dir, err := ioutil.TempDir("", "skiplist_test")
	if err != nil {
		t.Fatal(err)
	}

	db, err := bolt.Open(dir+"test.db", 0666, nil)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Commit()

	bkt, err := tx.CreateBucket([]byte("test"))
	if err != nil {
		t.Fatal(err)
	}

	sl := &boltSkiplistStore{bkt: bkt}

	data := generateData(10000, 1000)

	for _, d := range data {
		s, err := sl.get(d.k)
		if err != nil {
			t.Fatal(err)
		}
		if err := s.append(d.v, d.offset); err != nil {
			t.Fatal(err)
		}
	}

	for _, d := range data {
		s, err := sl.get(d.k)
		if err != nil {
			t.Fatal(err)
		}
		_, pid, err := s.seek(d.v)
		if err != nil {
			t.Fatal(err)
		}
		if pid != d.offset {
			t.Errorf("key: %v, val: %v, offset: %v.\tgot offset: %v", d.k, d.v, d.offset, pid)
		}
	}
}

func BenchmarkSkiplistSeek(b *testing.B) {

	dir, err := ioutil.TempDir("", "skiplist_test")
	if err != nil {
		b.Fatal(err)
	}

	db, err := bolt.Open(dir+"test.db", 0666, nil)
	if err != nil {
		b.Fatal(err)
	}
	tx, err := db.Begin(true)
	if err != nil {
		b.Fatal(err)
	}
	defer tx.Commit()

	bkt, err := tx.CreateBucket([]byte("test"))
	if err != nil {
		b.Fatal(err)
	}

	sl := &boltSkiplistStore{bkt: bkt}

	fmt.Println("run with N", b.N)

	data := generateData(int(b.N), uint32(b.N)/50+1)
	for _, d := range data {
		s, err := sl.get(d.k)
		if err != nil {
			b.Fatal(err)
		}
		if err := s.append(d.v, d.offset); err != nil {
			b.Fatal(err)
		}
	}

	fmt.Println("insert done")

	b.ResetTimer()

	for _, d := range data {
		s, err := sl.get(d.k)
		if err != nil {
			b.Fatal(err)
		}
		_, pid, err := s.seek(d.v)
		if err != nil {
			b.Fatal(err)
		}
		_ = pid
	}
}

func BenchmarkSkiplistAppend(b *testing.B) {

	dir, err := ioutil.TempDir("", "skiplist_test")
	if err != nil {
		b.Fatal(err)
	}

	db, err := bolt.Open(dir+"test.db", 0666, nil)
	if err != nil {
		b.Fatal(err)
	}
	tx, err := db.Begin(true)
	if err != nil {
		b.Fatal(err)
	}
	defer tx.Commit()

	bkt, err := tx.CreateBucket([]byte("test"))
	if err != nil {
		b.Fatal(err)
	}

	sl := &boltSkiplistStore{bkt: bkt}

	fmt.Println("run with N", b.N)

	data := generateData(int(b.N), uint32(b.N)/50+1)

	b.ResetTimer()

	for _, d := range data {
		s, err := sl.get(d.k)
		if err != nil {
			b.Fatal(err)
		}
		if err := s.append(d.v, d.offset); err != nil {
			b.Fatal(err)
		}
	}
}
