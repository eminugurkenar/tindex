package tindex

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

type mergeIterator struct {
	i1, i2 iterator
	v1, v2 uint64
	e1, e2 error
}

func (it *mergeIterator) next() (uint64, error) {
	if it.e1 == io.EOF && it.e2 == io.EOF {
		return 0, io.EOF
	}
	if it.e1 != nil {
		if it.e1 != io.EOF {
			return 0, it.e1
		}
		x := it.v2
		it.v2, it.e2 = it.i2.next()
		return x, nil
	}
	if it.e2 != nil {
		if it.e2 != io.EOF {
			return 0, it.e2
		}
		x := it.v1
		it.v1, it.e1 = it.i1.next()
		return x, nil
	}
	if it.v1 < it.v2 {
		x := it.v1
		it.v1, it.e1 = it.i1.next()
		return x, nil
	} else if it.v2 < it.v1 {
		x := it.v2
		it.v2, it.e2 = it.i2.next()
		return x, nil
	} else {
		x := it.v1
		it.v1, it.e1 = it.i1.next()
		it.v2, it.e2 = it.i2.next()
		return x, nil
	}
}

func (it *mergeIterator) seek(id uint64) (uint64, error) {
	// We just have to advance the first iterator. The next common match is also
	// the next seeked ID of the intersection.
	it.v1, it.e1 = it.i1.seek(id)
	it.v2, it.e2 = it.i2.seek(id)
	return it.next()
}

func merge(its ...iterator) iterator {
	if len(its) == 0 {
		return nil
	}
	i1 := its[0]

	for _, i2 := range its[1:] {
		i1 = &mergeIterator{i1: i1, i2: i2}
	}
	return i1
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

	for _, i2 := range its[1:] {
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

// A skiplist iterator iterates through a list of value/pointer pairs.
type skiplistIterator interface {
	// seek returns the value and pointer at or before v.
	seek(v uint64) (val, ptr uint64, err error)
	// next returns the next value/pointer pair.
	next() (val, ptr uint64, err error)
}

// iteratorStore allows to retrieve an iterator based on a key.
type iteratorStore interface {
	get(uint64) (iterator, error)
}

// skipIterator implements the iterator interface based on skiplist, which
// allows to jump to the iterator closest to the seeked value.
//
// This iterator allows for speed up in seeks if the underlying data cannot
// be searched in O(log n).
// Ideally the skiplist is seekable in O(log n).
type skipIterator struct {
	skiplist  skiplistIterator
	iterators iteratorStore

	// The iterator holding the next value.
	cur iterator
}

// seek implements the iterator interface.
func (it *skipIterator) seek(id uint64) (uint64, error) {
	_, ptr, err := it.skiplist.seek(id)
	if err != nil {
		return 0, err
	}
	cur, err := it.iterators.get(ptr)
	if err != nil {
		return 0, err
	}
	it.cur = cur

	return it.cur.seek(id)
}

// next implements the iterator interface.
func (it *skipIterator) next() (uint64, error) {
	// If next was called initially.
	// TODO(fabxc): should this just panic and initial call to seek() be required?
	if it.cur == nil {
		return it.seek(0)
	}

	if id, err := it.cur.next(); err == nil {
		return id, nil
	} else if err != io.EOF {
		return 0, err
	}
	// We reached the end of the current iterator. Get the next iterator through
	// our skiplist.
	_, ptr, err := it.skiplist.next()
	if err != nil {
		// Here we return the actual io.EOF if we reached the end of the iterator
		// retrieved from the last skiplist entry.
		return 0, err
	}
	// Iterate over the next iterator.
	cur, err := it.iterators.get(ptr)
	if err != nil {
		return 0, err
	}
	it.cur = cur

	// Return the first value in the new iterator.
	return it.cur.seek(0)
}

// plainListIterator implements the iterator interface on a sorted list of integers.
type plainListIterator struct {
	list list
	pos  int
}

func newPlainListIterator(l []uint64) *plainListIterator {
	it := &plainListIterator{list: list(l)}
	sort.Sort(it.list)
	return it
}

func (it *plainListIterator) seek(id uint64) (uint64, error) {
	it.pos = sort.Search(it.list.Len(), func(i int) bool { return it.list[i] >= id })
	return it.next()

}

func (it *plainListIterator) next() (uint64, error) {
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

// plainSkiplistIterator implements the skiplistIterator interface on plain
// in-memory mapping.
type plainSkiplistIterator struct {
	m    map[uint64]uint64
	keys list
	pos  int
}

func newPlainSkiplistIterator(m map[uint64]uint64) *plainSkiplistIterator {
	var keys list
	for k := range m {
		keys = append(keys, k)
	}
	sort.Sort(keys)

	return &plainSkiplistIterator{
		m:    m,
		keys: keys,
	}
}

// seek implements the skiplistIterator interface.
func (it *plainSkiplistIterator) seek(id uint64) (uint64, uint64, error) {
	pos := sort.Search(len(it.keys), func(i int) bool { return it.keys[i] >= id })
	// The skiplist iterator points to the element at or before the last value.
	if pos > 0 && it.keys[pos] > id {
		it.pos = pos - 1
	} else {
		it.pos = pos
	}
	return it.next()

}

// next implements the skiplistIterator interface.
func (it *plainSkiplistIterator) next() (uint64, uint64, error) {
	if it.pos >= len(it.keys) {
		return 0, 0, io.EOF
	}
	k := it.keys[it.pos]
	it.pos++
	return k, it.m[k], nil
}
