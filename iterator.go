package tsindex

import (
	"io"
	"sort"
)

type iterator interface {
	// next retrieves the next document ID in the postings list.
	next() (uint64, error)
	// seek moves the cursor to ID or the closest following one, if it doesn't exist.
	// It returns the ID at the position.
	seek(id uint64) (uint64, error)
}

func expandIterator(it iterator) ([]uint64, error) {
	var (
		res = []uint64{}
		v   uint64
		err error
	)
	for v, err = it.seek(0); err == nil; v, err = it.next() {
		res = append(res, v)
	}
	if err == io.EOF {
		err = nil
	}
	return res, err
}

type intersectIterator struct {
	i1, i2 iterator
	v1, v2 uint64
	e1, e2 error
}

func intersect(its ...iterator) iterator {
	if len(its) == 0 {
		return nil
	}
	i1 := its[0]

	for _, i2 := range its {
		i1 = &intersectIterator{i1: i1, i2: i2}
	}
	return i1
}

func (it *intersectIterator) next() (uint64, error) {
	for {
		if it.e1 != nil {
			return 0, it.e1
		}
		if it.e2 != nil {
			return 0, it.e2
		}
		if it.v1 < it.v2 {
			it.v1, it.e1 = it.i1.seek(it.v2)
		} else if it.v2 < it.v1 {
			it.v2, it.e2 = it.i2.seek(it.v1)
		} else {
			v := it.v1
			it.v1, it.e1 = it.i1.next()
			it.v2, it.e2 = it.i2.next()
			return v, nil
		}
	}
}

func (it *intersectIterator) seek(id uint64) (uint64, error) {
	// We just have to advance the first iterator. The next common match is also
	// the next seeked ID of the intersection.
	it.v1, it.e1 = it.i1.seek(id)
	return it.next()
}

// listIterator implements the iterator interface on a sorted list of integers.
type listIterator struct {
	list list
	pos  int
}

func newListIterator(l []uint64) *listIterator {
	it := &listIterator{list: list(l)}
	sort.Sort(it.list)
	return it
}

func (it *listIterator) seek(id uint64) (uint64, error) {
	it.pos = sort.Search(it.list.Len(), func(i int) bool { return it.list[i] >= id })
	return it.next()

}

func (it *listIterator) next() (uint64, error) {
	if it.pos >= it.list.Len() {
		return 0, io.EOF
	}
	x := it.list[it.pos]
	it.pos++
	return x, nil
}

type list []uint64

func (l list) Len() int           { return len(l) }
func (l list) Less(i, j int) bool { return l[i] < l[j] }
func (l list) Swap(i, j int)      { l[i], l[j] = l[j], l[i] }
