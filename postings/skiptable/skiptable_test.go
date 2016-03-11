package skiptable

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func TestNewSkipTableInit(t *testing.T) {
	dir, err := ioutil.TempDir("", "skiptable_test")
	if err != nil {
		t.Fatal(err)
	}

	table := &skipTable{
		dir: dir,
	}
	if err := table.init(); err != nil {
		t.Fatal(err)
	}

	stat, err := os.Stat(filepath.Join(dir, "skip-0-0"))
	if err != nil {
		t.Fatal(err)
	}

	// Allocated file must be a multiple of page size.
	expSize := skipTableRows * skipLineLength
	ps := syscall.Getpagesize()
	numPages := expSize / ps
	if ps*numPages < expSize {
		ps++
	}

	if stat.Size() != int64(ps*numPages) {
		t.Fatalf("Expected new block size %d but got %d", skipLineLength*skipTableRows, stat.Size())
	}
}
