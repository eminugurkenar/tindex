package tsindex

import (
	"reflect"
	"testing"
)

func TestIntersectIterator(t *testing.T) {
	var cases = []struct {
		a, b []uint64
		res  []uint64
	}{
		{
			a:   []uint64{1, 2, 3, 4, 5},
			b:   []uint64{6, 7, 8, 9, 10},
			res: []uint64{},
		},
		{
			a:   []uint64{1, 2, 3, 4, 5},
			b:   []uint64{4, 5, 6, 7, 8},
			res: []uint64{4, 5},
		},
		{
			a:   []uint64{1, 2, 3, 4, 9, 10},
			b:   []uint64{1, 4, 5, 6, 7, 8, 10, 11},
			res: []uint64{1, 4, 10},
		},
	}

	for _, c := range cases {
		i1 := newListIterator(c.a)
		i2 := newListIterator(c.b)

		res, err := expandIterator(&intersectIterator{i1: i1, i2: i2})
		if err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		if !reflect.DeepEqual(res, c.res) {
			t.Fatalf("Expected %v but got %v", c.res, res)
		}
	}
}
