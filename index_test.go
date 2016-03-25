package tindex

// import (
// 	"fmt"
// 	"io"
// 	"io/ioutil"
// 	// "math/rand"
// 	"reflect"
// 	"sort"
// 	"testing"
// )

// func generateTestLabelSets(n int) (lsets []labelSet) {
// 	for i := 0; i < n; i++ {
// 		lsets = append(lsets, labelSet{
// 			fmt.Sprintf("label_%d", i):   fmt.Sprintf("value_%d", i),
// 			fmt.Sprintf("label_%d", i+1): fmt.Sprintf("value_%d", i+1),
// 			fmt.Sprintf("label_%d", i+2): fmt.Sprintf("value_%d", i+2),
// 			fmt.Sprintf("label_%d", i+3): fmt.Sprintf("value_%d", i+3),
// 		})
// 	}
// 	return lsets
// }

// func TestIndexEnsureLabels(t *testing.T) {
// 	dir, err := ioutil.TempDir("", "index")
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	ix, err := Open(dir, nil)
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	defer ix.Close()

// 	lsets := generateTestLabelSets(400)
// 	inserted := map[uint64]labelSet{}

// 	for _, lset := range lsets {
// 		sid, err := ix.EnsureSeries(lset.m())
// 		if err != nil {
// 			t.Fatal(err)
// 		}
// 		inserted[sid] = lset
// 	}

// 	for sid, exp := range inserted {
// 		m, err := ix.GetSeries(sid)
// 		if err != nil {
// 			t.Fatal(err)
// 		}
// 		if !reflect.DeepEqual(labelSet(m), exp) {
// 			t.Fatal("no match")
// 		}
// 	}
// }

// type testPostingsCursor struct {
// 	vals []int
// 	pos  int
// }

// func (c *testPostingsCursor) next() (docid, error) {
// 	if c.pos == len(c.vals) {
// 		return 0, io.EOF
// 	}
// 	c.pos++
// 	return docid(c.vals[c.pos-1]), nil
// }

// func (c *testPostingsCursor) seek(d docid) (docid, error) {
// 	c.pos = sort.SearchInts(c.vals, int(d))
// 	if c.pos == len(c.vals) {
// 		return 0, errNotFound
// 	}
// 	return docid(c.vals[c.pos]), nil
// }

// func (c *testPostingsCursor) append(d docid) error {
// 	return nil
// }

// func TestIntersect(t *testing.T) {
// 	a := &testPostingsCursor{
// 		vals: []int{1, 2, 3, 4, 7, 10},
// 	}
// 	b := &testPostingsCursor{
// 		vals: []int{3, 4, 5, 6, 7, 8},
// 	}
// 	fmt.Println(intersect(a, b))
// }

// func BenchmarkIntersect(t *testing.B) {
// 	a := &testPostingsCursor{}
// 	b := &testPostingsCursor{}
// 	c := &testPostingsCursor{}

// 	for i := 0; i < 10000000; i += 2 {
// 		a.vals = append(a.vals, i)
// 	}
// 	for i := 5000000; i < 5000100; i += 4 {
// 		b.vals = append(b.vals, i)
// 	}
// 	for i := 5090000; i < 5090100; i += 4 {
// 		b.vals = append(b.vals, i)
// 	}
// 	for i := 4990000; i < 5100000; i++ {
// 		c.vals = append(c.vals, i)
// 	}

// 	t.ResetTimer()

// 	for i := 0; i < t.N; i++ {
// 		intersect(a, c)
// 	}
// }
