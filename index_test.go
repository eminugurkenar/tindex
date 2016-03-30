package tindex

import (
	"fmt"
	"io/ioutil"
	"time"
	// "math/rand"
	"reflect"
	"testing"
)

func generateTestLabelSets(n int) (lsets []labelSet) {
	for i := 0; i < n; i++ {
		lsets = append(lsets, labelSet{
			fmt.Sprintf("label_%d", i):   fmt.Sprintf("value_%d", i),
			fmt.Sprintf("label_%d", i+1): fmt.Sprintf("value_%d", i+1),
			fmt.Sprintf("label_%d", i+2): fmt.Sprintf("value_%d", i+2),
			fmt.Sprintf("label_%d", i+3): fmt.Sprintf("value_%d", i+3),
		})
	}
	return lsets
}

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
	inserted := map[uint64]labelSet{}

	for _, lset := range lsets {
		sid, err := ix.EnsureSeries(lset.m())
		if err != nil {
			t.Fatal(err)
		}
		// Insert again to check whether this doesn't mess things up.
		sid2, err := ix.EnsureSeries(lset.m())
		if err != nil {
			t.Fatal(err)
		}
		if sid2 != sid {
			t.Fatalf("Ensuring series twice created new ID")
		}
		inserted[sid] = lset
	}

	for sid, exp := range inserted {
		m, err := ix.Series(sid)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(labelSet(m), exp) {
			t.Errorf("No match %T %T", m, exp)
			t.Errorf("Expected: %s", exp)
			t.Fatalf("Received: %s", m)
		}
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

	sid, err := ix.EnsureSeries(map[string]string{
		"a": "b",
		"c": "d",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := ix.See(time.Now(), sid); err != nil {
		t.Fatal(err)
	}
	res, err := ix.Instant(time.Now(), NewEqualMatcher("a", "b"))
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(res, []uint64{sid}) {
		t.Fatal("no match", res, []uint64{sid})
	}
	res, err = ix.Instant(time.Now(), NewEqualMatcher("c", "d"))
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(res, []uint64{sid}) {
		t.Fatal("no match", res)
	}

	if err := ix.Unsee(time.Now(), sid); err != nil {
		t.Fatal(err)
	}
	res, err = ix.Instant(time.Now(), NewEqualMatcher("c", "d"))
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

	sid, err := ix.EnsureSeries(map[string]string{
		"a": "b",
		"c": "d",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := ix.See(t0, sid); err != nil {
		t.Fatal(err)
	}
	res, err := ix.Range(t1, t2, NewEqualMatcher("a", "b"))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(res, []uint64{sid}) {
		t.Fatal("no match", res, []uint64{sid})
	}

	if err := ix.Unsee(t1, sid); err != nil {
		t.Fatal(err)
	}
	res, err = ix.Range(t2, t3, NewEqualMatcher("a", "b"))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(res, []uint64{}) {
		t.Fatal("no match", res, []uint64{})
	}

	if err := ix.See(t0, sid); err != nil {
		t.Fatal(err)
	}
	res, err = ix.Range(t0, t3, NewEqualMatcher("a", "b"))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(res, []uint64{sid}) {
		t.Fatal("no match", res, []uint64{sid})
	}
}

func TestIndexRange2(t *testing.T) {
	dir, err := ioutil.TempDir("", "index")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(dir)
	ix, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ix.Close()

	data := []struct {
		m map[string]string
		t []uint64
	}{
		{
			m: map[string]string{
				"l1": "v1",
				"l2": "v2",
			},
			t: []uint64{1, 10, 20, 30, 40},
		},
		{
			m: map[string]string{
				"l2": "v2",
				"l3": "v3",
			},
			t: []uint64{1, 10, 20, 30, 40},
		},
		{
			m: map[string]string{
				"l2": "v2",
				"l4": "v4",
			},
			t: []uint64{1, 10, 20, 30, 40},
		},
		{
			m: map[string]string{
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
	var sids []uint64

	for _, d := range data {
		sid, err := ix.EnsureSeries(d.m)
		if err != nil {
			t.Fatal(err)
		}
		sids = append(sids, sid)
		fmt.Println("added", sid)

		for i, tt := range d.t {
			ts := timeAt(tt)

			if i%2 == 0 {
				if err := ix.See(ts, sid); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := ix.Unsee(ts, sid); err != nil {
					t.Fatal(err)
				}
			}
		}
	}

	res, err := ix.Instant(timeAt(5), NewEqualMatcher("l2", "v2"))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(res, []uint64{sids[0], sids[1]}) {
		t.Fatal("no match", res, []uint64{sids[0], sids[1]})
	}
}
