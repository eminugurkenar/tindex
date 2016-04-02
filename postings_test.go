package tindex

import (
	"io/ioutil"
	"math/rand"
	"reflect"
	"testing"
)

func genBatch(n int, lmin, lmax int64) PostingsBatches {
	batches := PostingsBatches{}
	for i := 0; i < n; i++ {
		var ids []uint64
		var last int64

		maxDelta := 1 + rand.Int63n(200000)

		m := lmin + rand.Int63n(lmax-lmin) + 1
		for j := int64(0); j < m; j++ {
			last = last + rand.Int63n(maxDelta) + 1
			ids = append(ids, uint64(last))
		}

		batches[uint64(i)] = ids
	}
	return batches
}

func TestPostings(t *testing.T) {
	dir, err := ioutil.TempDir("", "index")
	if err != nil {
		t.Fatal(err)
	}
	postings, err := NewPostings(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer postings.Close()

	batches := genBatch(50, 1, 10000)

	if err := postings.Append(batches); err != nil {
		t.Fatalf("Error appending batches: %s", err)
	}

	for k, ids := range batches {
		it, err := postings.Iter(k)
		if err != nil {
			t.Fatalf("Error getting iterator for %q: %s", k, err)
		}

		res, err := ExpandIterator(it)
		if err != nil {
			t.Fatalf("Error expanding iterator: %s", err)
		}

		if !reflect.DeepEqual(res, ids) {
			t.Errorf("Retrieved postings list doesn't match input")
			t.Errorf("Expected: %v", ids)
			t.Errorf("Received: %v", res)
		}
	}
}

func BenchmarkPostingsAppend(t *testing.B) {
	batches := genBatch(1000, 50, 10000)

	t.ResetTimer()

	for i := 0; i < t.N; i++ {
		t.StopTimer()

		dir, err := ioutil.TempDir("", "index")
		if err != nil {
			t.Fatal(err)
		}
		postings, err := NewPostings(dir)
		if err != nil {
			t.Fatal(err)
		}
		defer postings.Close()

		t.StartTimer()

		if err := postings.Append(batches); err != nil {
			t.Fatalf("Error appending batches: %s", err)
		}
	}
}

func BenchmarkPostingsRead(t *testing.B) {
	batches := genBatch(1000, 50, 10000)

	t.ResetTimer()

	for i := 0; i < t.N; i++ {
		t.StopTimer()

		dir, err := ioutil.TempDir("", "index")
		if err != nil {
			t.Fatal(err)
		}
		postings, err := NewPostings(dir)
		if err != nil {
			t.Fatal(err)
		}
		defer postings.Close()

		if err := postings.Append(batches); err != nil {
			t.Fatalf("Error appending batches: %s", err)
		}

		t.StartTimer()

		for k := range batches {
			it, err := postings.Iter(k)
			if err != nil {
				t.Fatalf("Error getting iterator for %q: %s", k, err)
			}

			_, err = ExpandIterator(it)
			if err != nil {
				t.Fatalf("Error expanding iterator: %s", err)
			}
		}
	}
}
