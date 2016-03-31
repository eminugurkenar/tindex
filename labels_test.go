package tindex

import (
	"io/ioutil"
	"reflect"
	"testing"
)

func TestLabelsEnsure(t *testing.T) {
	dir, err := ioutil.TempDir("", "index")
	if err != nil {
		t.Fatal(err)
	}
	s, err := NewLabels(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	pairs := []Pair{
		{"label1", "value1"},
		{"label1", "value2"},
		{"label2", "value3"},
		{"label2", "value1"},
		{"label3", "value2"},
		{"label3", "value3"},
	}
	var ids []uint64

	ids, err = s.Ensure(pairs...)
	if err != nil {
		t.Fatalf("Error ensuring label pairs", err)
	}

	// We want the correct pairs back for the IDs.
	res, err := s.Get(ids...)
	if err != nil {
		t.Fatalf("Error retrieving pairs: %s", err)
	}

	if !reflect.DeepEqual(res, pairs) {
		t.Errorf("Retrieved pairs did not match")
		t.Errorf("Expected: %v", pairs)
		t.Fatalf("Received: %v", res)
	}
}

func TestLabelsSearch(t *testing.T) {
	dir, err := ioutil.TempDir("", "index")
	if err != nil {
		t.Fatal(err)
	}
	s, err := NewLabels(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	pairs := []Pair{
		{"label1", "value1"},
		{"label1", "value2"},
		{"label2", "value3"},
		{"label2", "value1"},
		{"label3", "value2"},
		{"label3", "value3"},
	}
	var ids []uint64

	ids, err = s.Ensure(pairs...)
	if err != nil {
		t.Fatalf("Error ensuring label pairs", err)
	}

	mustRegexpMatcher := func(k, exp string) Matcher {
		m, err := NewRegexpMatcher(k, exp)
		if err != nil {
			t.Fatal(err)
		}
		return m
	}

	cases := []struct {
		m   Matcher
		res []uint64
	}{
		{
			// Simple existing match.
			m:   NewEqualMatcher("label2", "value3"),
			res: []uint64{ids[2]},
		},
		{
			// A value that doesn't exist for the label.
			m:   NewEqualMatcher("label2", "x"),
			res: nil,
		},
		{
			// A label that doesn't exist.
			m:   NewEqualMatcher("x", "value1"),
			res: nil,
		},
		{
			// A simple matching regexp.
			m:   mustRegexpMatcher("label1", "val.*"),
			res: []uint64{ids[0], ids[1]},
		},
		{
			// A non-matching regexp.
			m:   mustRegexpMatcher("label1", "val.*FOO"),
			res: nil,
		},
	}

	for _, c := range cases {
		res, err := s.Search(c.m)
		if err != nil {
			t.Fatalf("Search failed: %s", err)
		}
		if !reflect.DeepEqual(res, c.res) {
			t.Errorf("Retrieved label IDs did not match")
			t.Errorf("Expected: %v", c.res)
			t.Fatalf("Received: %v", res)
		}
	}
}
