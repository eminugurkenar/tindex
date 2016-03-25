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
		i1 := newListIterator(c.a)
		i2 := newListIterator(c.b)
		i3 := newListIterator(c.c)

		res, err := expandIterator(intersect(i1, i2, i3))
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
		i1 := newListIterator(c.a)
		i2 := newListIterator(c.b)
		i3 := newListIterator(c.c)

		res, err := expandIterator(merge(i1, i2, i3))
		if err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		if !reflect.DeepEqual(res, c.res) {
			t.Fatalf("Expected %v but got %v", c.res, res)
		}
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
		i1 := newListIterator(c.a)
		i2 := newListIterator(c.b)

		res, err := expandIterator(&mergeIterator{i1: i1, i2: i2})
		if err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		if !reflect.DeepEqual(res, c.res) {
			t.Fatalf("Expected %v but got %v", c.res, res)
		}
	}
}
