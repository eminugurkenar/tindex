package tindex

import (
	"reflect"
	"testing"
)

func TestMultiIntersect(t *testing.T) {
	var cases = []struct {
		a, b, c []uint64
		res     []uint64
	}{
		{
			a:   []uint64{1, 2, 3, 4, 5, 6, 1000, 1001},
			b:   []uint64{2, 4, 5, 6, 7, 8, 999, 1001},
			c:   []uint64{1, 2, 5, 6, 7, 8, 1001, 1200},
			res: []uint64{2, 5, 6, 1001},
		},
	}

	for _, c := range cases {
		i1 := newPlainListIterator(c.a)
		i2 := newPlainListIterator(c.b)
		i3 := newPlainListIterator(c.c)

		res, err := ExpandIterator(Intersect(i1, i2, i3))
		if err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		if !reflect.DeepEqual(res, c.res) {
			t.Fatalf("Expected %v but got %v", c.res, res)
		}
	}
}

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
		}, {
			a:   []uint64{1},
			b:   []uint64{0, 1},
			res: []uint64{1},
		},
	}

	for _, c := range cases {
		i1 := newPlainListIterator(c.a)
		i2 := newPlainListIterator(c.b)

		res, err := ExpandIterator(&intersectIterator{i1: i1, i2: i2})
		if err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		if !reflect.DeepEqual(res, c.res) {
			t.Fatalf("Expected %v but got %v", c.res, res)
		}
	}
}

func TestMergeIntersect(t *testing.T) {
	var cases = []struct {
		a, b, c []uint64
		res     []uint64
	}{
		{
			a:   []uint64{1, 2, 3, 4, 5, 6, 1000, 1001},
			b:   []uint64{2, 4, 5, 6, 7, 8, 999, 1001},
			c:   []uint64{1, 2, 5, 6, 7, 8, 1001, 1200},
			res: []uint64{1, 2, 3, 4, 5, 6, 7, 8, 999, 1000, 1001, 1200},
		},
	}

	for _, c := range cases {
		i1 := newPlainListIterator(c.a)
		i2 := newPlainListIterator(c.b)
		i3 := newPlainListIterator(c.c)

		res, err := ExpandIterator(Merge(i1, i2, i3))
		if err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		if !reflect.DeepEqual(res, c.res) {
			t.Fatalf("Expected %v but got %v", c.res, res)
		}
	}
}

func BenchmarkIntersect(t *testing.B) {
	var a, b, c, d []uint64

	for i := 0; i < 10000000; i += 2 {
		a = append(a, uint64(i))
	}
	for i := 5000000; i < 5000100; i += 4 {
		b = append(b, uint64(i))
	}
	for i := 5090000; i < 5090600; i += 4 {
		b = append(b, uint64(i))
	}
	for i := 4990000; i < 5100000; i++ {
		c = append(c, uint64(i))
	}
	for i := 4000000; i < 6000000; i++ {
		d = append(d, uint64(i))
	}

	i1 := newPlainListIterator(a)
	i2 := newPlainListIterator(b)
	i3 := newPlainListIterator(c)
	i4 := newPlainListIterator(d)

	t.ResetTimer()

	for i := 0; i < t.N; i++ {
		Intersect(i1, i2, i3, i4)
	}
}

func TestMergeIterator(t *testing.T) {
	var cases = []struct {
		a, b []uint64
		res  []uint64
	}{
		{
			a:   []uint64{1, 2, 3, 4, 5},
			b:   []uint64{6, 7, 8, 9, 10},
			res: []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		},
		{
			a:   []uint64{1, 2, 3, 4, 5},
			b:   []uint64{4, 5, 6, 7, 8},
			res: []uint64{1, 2, 3, 4, 5, 6, 7, 8},
		},
		{
			a:   []uint64{1, 2, 3, 4, 9, 10},
			b:   []uint64{1, 4, 5, 6, 7, 8, 10, 11},
			res: []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11},
		},
	}

	for _, c := range cases {
		i1 := newPlainListIterator(c.a)
		i2 := newPlainListIterator(c.b)

		res, err := ExpandIterator(&mergeIterator{i1: i1, i2: i2})
		if err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		if !reflect.DeepEqual(res, c.res) {
			t.Fatalf("Expected %v but got %v", c.res, res)
		}
	}
}

func TestSkippingIterator(t *testing.T) {
	var cases = []struct {
		skiplist skiplistIterator
		its      iteratorStore
		res      []uint64
	}{
		{
			skiplist: newPlainSkiplistIterator(map[uint64]uint64{
				5:   3,
				50:  2,
				500: 1,
			}),
			its: testIteratorStore{
				3: newPlainListIterator(list{5, 7, 8, 9}),
				2: newPlainListIterator(list{54, 60, 61}),
				1: newPlainListIterator(list{1200, 1300, 100000}),
			},
			res: []uint64{5, 7, 8, 9, 54, 60, 61, 1200, 1300, 100000},
		},
		{
			skiplist: newPlainSkiplistIterator(map[uint64]uint64{
				0:  3,
				50: 2,
			}),
			its: testIteratorStore{
				3: newPlainListIterator(list{5, 7, 8, 9}),
				2: newPlainListIterator(list{54, 60, 61}),
			},
			res: []uint64{5, 7, 8, 9, 54, 60, 61},
		},
	}

	for _, c := range cases {
		it := &skippingIterator{
			skiplist:  c.skiplist,
			iterators: c.its,
		}
		res, err := ExpandIterator(it)
		if err != nil {
			t.Fatalf("Unexpected error", err)
		}
		if !reflect.DeepEqual(res, c.res) {
			t.Fatalf("Expected %v but got %v", c.res, res)
		}
	}
}

type testIteratorStore map[uint64]Iterator

func (s testIteratorStore) get(id uint64) (Iterator, error) {
	it, ok := s[id]
	if !ok {
		return nil, errNotFound
	}
	return it, nil
}
