package skiptable

import (
	"fmt"
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

	table := &SkipTable{
		opts: Opts{
			BlockRows:       256,
			BlockLineLength: 256,
		},
		dir: dir,
	}
	if err := table.init(); err != nil {
		t.Fatal(err)
	}
	defer table.Close()

	stat, err := os.Stat(filepath.Join(dir, fmt.Sprintf(filenamePat, 0, 0)))
	if err != nil {
		t.Fatal(err)
	}

	// Allocated file must be a multiple of page size.
	expSize := table.opts.BlockRows * table.opts.BlockLineLength
	ps := syscall.Getpagesize()
	numPages := expSize / ps
	if ps*numPages < expSize {
		ps++
	}

	if stat.Size() != int64(ps*numPages) {
		t.Fatalf("Expected new block size %d but got %d", ps*numPages, stat.Size())
	}
}

func TestSkipTableStore(t *testing.T) {
	dir, err := ioutil.TempDir("", "skiptable_test")
	if err != nil {
		t.Fatal(err)
	}

	table, err := New(dir, Opts{
		BlockRows:       256,
		BlockLineLength: 256,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer table.Close()

	if err := table.Store(123, 1024, 3); err != nil {
		t.Fatal(err)
	}

	offset, err := table.Offset(123, 4)
	if err != nil {
		t.Fatal(err)
	}
	if offset != 1024 {
		t.Errorf("Expected offset 1024 but got %d", offset)
	}
}
