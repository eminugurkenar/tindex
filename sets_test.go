package tindex

import (
	"fmt"
	"io/ioutil"
	"reflect"
	"testing"
)

func generateTestLabelSets(n int) (lsets []Set) {
	for i := 0; i < n; i++ {
		lsets = append(lsets, Set{
			fmt.Sprintf("label_%d", i):   fmt.Sprintf("value_%d", i),
			fmt.Sprintf("label_%d", i+1): fmt.Sprintf("value_%d", i+1),
			fmt.Sprintf("label_%d", i+2): fmt.Sprintf("value_%d", i+2),
			fmt.Sprintf("label_%d", i+3): fmt.Sprintf("value_%d", i+3),
		})
	}
	return lsets
}

func TestLabelSetEnsure(t *testing.T) {
	dir, err := ioutil.TempDir("", "index")
	if err != nil {
		t.Fatal(err)
	}
	labels, err := NewLabels(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer labels.Close()

	labelsets, err := NewLabelSets(dir, labels)
	if err != nil {
		t.Fatal(err)
	}
	defer labelsets.Close()

	lsets := generateTestLabelSets(4)

	ids, _, err := labelsets.Ensure(lsets...)
	if err != nil {
		t.Fatalf("Error ensuring labels sets: %s", err)
	}

	res, err := labelsets.Get(ids...)
	if err != nil {
		t.Fatalf("Error getting label sets: %s", err)
	}

	if !reflect.DeepEqual(res, lsets) {
		t.Errorf("Label sets did not match:")
		t.Errorf("Expected: %s", lsets)
		t.Fatalf("Received: %s", res)
	}
}
