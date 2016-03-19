package tsindex

import (
	"fmt"
	"io/ioutil"
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
		inserted[sid] = lset
	}

	for sid, exp := range inserted {
		m, err := ix.GetSeries(sid)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(labelSet(m), exp) {
			t.Fatal("no match")
		}
	}
}
