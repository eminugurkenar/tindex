package tindex

import (
	"io/ioutil"
	"reflect"
	"testing"
	"time"
)

func TestIndexEnsureLabels(t *testing.T) {
	dir, err := ioutil.TempDir("", "index")
	if err != nil {
		t.Fatal(err)
	}
	ix, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ix.Close()

	lsets := generateTestLabelSets(4)

	sids, err := ix.EnsureSets(lsets...)
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}

	// Ensuring sets twice must return the same IDs.
	sids2, err := ix.EnsureSets(lsets...)
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	if !reflect.DeepEqual(sids, sids2) {
		t.Errorf("Ensuring sets twice generated different IDs")
		t.Errorf("First:  %v", sids)
		t.Fatalf("Second: %v", sids2)
	}

	res, err := ix.Sets(sids...)
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	if !reflect.DeepEqual(sids, sids2) {
		t.Errorf("Retrived wrong sets for IDs")
		t.Errorf("Expected: %v", lsets)
		t.Fatalf("Received: %v", res)
	}
}

func TestIndexInstant(t *testing.T) {
	dir, err := ioutil.TempDir("", "index")
	if err != nil {
		t.Fatal(err)
	}
	ix, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ix.Close()

	sids, err := ix.EnsureSets(Set{
		"a": "b",
		"c": "d",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := ix.Active(time.Now(), sids...); err != nil {
		t.Fatal(err)
	}
	it, err := ix.Instant(time.Now(), NewEqualMatcher("a", "b"))
	if err != nil {
		t.Fatal(err)
	}
	res, err := ExpandIterator(it)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(res, sids) {
		t.Fatal("no match", res, sids)
	}
	it, err = ix.Instant(time.Now(), NewEqualMatcher("c", "d"))
	if err != nil {
		t.Fatal(err)
	}
	res, err = ExpandIterator(it)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(res, sids) {
		t.Fatal("no match", res)
	}

	if err := ix.Inactive(time.Now(), sids...); err != nil {
		t.Fatal(err)
	}
	it, err = ix.Instant(time.Now(), NewEqualMatcher("c", "d"))
	if err != nil {
		t.Fatal(err)
	}
	res, err = ExpandIterator(it)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(res, []uint64{}) {
		t.Fatal("no match", res)
	}
}

func TestIndexRange(t *testing.T) {
	dir, err := ioutil.TempDir("", "index")
	if err != nil {
		t.Fatal(err)
	}
	ix, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ix.Close()

	var (
		t0 = time.Now()
		t1 = t0.Add(time.Hour)
		t2 = t1.Add(time.Hour)
		t3 = t2.Add(time.Hour)
		// t4 = t3.Add(time.Hour)
	)

	sids, err := ix.EnsureSets(Set{
		"a": "b",
		"c": "d",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := ix.Active(t0, sids...); err != nil {
		t.Fatal(err)
	}
	it, err := ix.Range(t1, t2.Sub(t1), NewEqualMatcher("a", "b"))
	if err != nil {
		t.Fatal(err)
	}
	res, err := ExpandIterator(it)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(res, sids) {
		t.Fatal("no match", res, sids)
	}

	if err := ix.Inactive(t1, sids...); err != nil {
		t.Fatal(err)
	}
	it, err = ix.Range(t2, t3.Sub(t2), NewEqualMatcher("a", "b"))
	if err != nil {
		t.Fatal(err)
	}
	res, err = ExpandIterator(it)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(res, []uint64{}) {
		t.Fatal("no match", res, []uint64{})
	}

	if err := ix.Active(t0, sids...); err != nil {
		t.Fatal(err)
	}
	it, err = ix.Range(t0, t3.Sub(t0), NewEqualMatcher("a", "b"))
	if err != nil {
		t.Fatal(err)
	}
	res, err = ExpandIterator(it)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(res, sids) {
		t.Fatal("no match", res, sids)
	}
}

func TestIndexRange2(t *testing.T) {
	dir, err := ioutil.TempDir("", "index")
	if err != nil {
		t.Fatal(err)
	}
	ix, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ix.Close()

	data := []struct {
		m Set
		t []uint64
	}{
		{
			m: Set{
				"l1": "v1",
				"l2": "v2",
			},
			t: []uint64{1, 10, 20, 30, 40},
		},
		{
			m: Set{
				"l2": "v2",
				"l3": "v3",
			},
			t: []uint64{1, 10, 20, 30, 40},
		},
		{
			m: Set{
				"l2": "v2",
				"l4": "v4",
			},
			t: []uint64{1, 10, 20, 30, 40},
		},
		{
			m: Set{
				"l3": "v3",
				"l4": "v4",
			},
			t: []uint64{20, 30, 50},
		},
	}

	var t0 = time.Now()
	timeAt := func(i uint64) time.Time {
		return t0.Add(time.Duration(i) * time.Hour)
	}

	for _, d := range data {
		sids, err := ix.EnsureSets(d.m)
		if err != nil {
			t.Fatal(err)
		}

		for i, tt := range d.t {
			ts := timeAt(tt)

			if i%2 == 0 {
				if err := ix.Active(ts, sids...); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := ix.Inactive(ts, sids...); err != nil {
					t.Fatal(err)
				}
			}
		}
	}

	sids := []uint64{1, 2, 3}

	it, err := ix.Instant(timeAt(5), NewEqualMatcher("l2", "v2"))
	if err != nil {
		t.Fatal(err)
	}
	res, err := ExpandIterator(it)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(res, sids) {
		t.Fatal("no match", res, sids)
	}
}
