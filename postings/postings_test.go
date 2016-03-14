package postings

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"reflect"
	// "encoding/hex"
	"testing"
)

func TestPostings(t *testing.T) {
	dir, err := ioutil.TempDir("", "postings_test")
	if err != nil {
		t.Fatal(err)
	}
	defer fmt.Println(dir)

	p, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	data := generateData(&generateOpts{
		numKeys:   1,
		numValMin: 390,
		numValMax: 400,
	})

	for _, d := range data {
		for _, set := range d.Set {
			if err := p.Set(d.Key, set.Value, set.Ptr); err != nil {
				t.Fatal(err)
			}
		}
	}

	fmt.Println("SET COMPLETE\n\n")

	for _, d := range data {
		set, err := p.Get(d.Key, d.Set[0].Value, d.Set[len(d.Set)-1].Value)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(set, d.Set) {
			t.Fatalf("Expected for key %d set |%d|\n %v\n\n but got |%d| %v", d.Key, len(d.Set), d.Set, len(set), set)
		}
	}
}

type generateOpts struct {
	// Total umber of postings lists.
	numKeys int

	numValMin int
	numValMax int
}

type testData struct {
	Key Key
	Set Set
}

func generateData(opts *generateOpts) []testData {
	data := []testData{}
	for i := range make([]struct{}, opts.numKeys) {
		var d testData
		d.Key = Key(i)

		var last int
		n := opts.numValMin + rand.Intn(opts.numValMax-opts.numValMin)
		for n > 0 {
			last = last + 1 + rand.Intn(5)
			d.Set = append(d.Set, Pair{Value: Value(last), Ptr: uint64(n)})
			n--
		}
		data = append(data, d)
	}
	return data
}

// func TestIntersection(t *testing.T) {
// 	postings := New()

// 	var sets []Set

// 	for i := range make([]struct{}, 10) {
// 		list, err := postings.List(Key(i))
// 		if err != nil {
// 			t.Fatal(err)
// 		}
// 		for j := range make([]struct{}, 100) {
// 			if err := list.Append(Value(i*5 + j)); err != nil {
// 				t.Fatal(err)
// 			}
// 		}
// 		set, err := list.Get(0, 110)
// 		if err != nil {
// 			t.Fatal(err)
// 		}
// 		sets = append(sets, set)
// 	}

// 	res := Intersect(sets...)

// 	var exp Set
// 	for i := range make([]struct{}, 55) {
// 		exp = append(exp, Value(45+i))
// 	}

// 	if !reflect.DeepEqual(exp, res) {
// 		t.Errorf("Expected intersection did not match result")
// 		t.Errorf("Expected: %v", exp)
// 		t.Errorf("Got:      %v", res)
// 	}
// }

// func benchmarkIntersection(b *testing.B, numIntersects int) {
// 	postings := New()
// 	err := generateData(postings, &generateOpts{
// 		numLists:  numIntersects,
// 		numValMin: 1000000,
// 		numValMax: 1200000,
// 	})
// 	if err != nil {
// 		b.Fatal(err)
// 	}
// 	d
// 	var sets []Set
// 	for i := range make([]struct{}, numIntersects) {
// 		list, err := postings.List(Key(i))
// 		if err != nil {
// 			b.Fatal(err)
// 		}
// 		set, err := list.Get(0, 100000000)
// 		if err != nil {
// 			b.Fatal(err)
// 		}

// 		sets = append(sets, set)
// 	}

// 	b.StartTimer()

// 	res := Intersect(sets...)
// 	b.Logf("Intersection size: %d", len(res))
// }

// func BenchmarkIntersection5(b *testing.B) {
// 	benchmarkIntersection(b, 5)
// }
