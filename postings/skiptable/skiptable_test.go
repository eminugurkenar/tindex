package skiptable

import (
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func TestSkipTableAllocate(t *testing.T) {
	dir, err := ioutil.TempDir("", "skiptable_test")
	if err != nil {
		t.Fatal(err)
	}

	table, err := New(dir, Opts{
		BlockRows:       16,
		BlockLineLength: 16,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer table.Close()

	// Allocated file must be a multiple of page size.
	expSize := table.opts.BlockRows * table.opts.BlockLineLength
	ps := syscall.Getpagesize()
	numPages := expSize / ps
	if ps*numPages < expSize {
		numPages++
	}

	// Allocate a few blocks.
	for i := 0; i < 5; i++ {
		for j := 0; j < 4; j++ {
			err := table.allocateBlock(i, j)
			if err != nil {
				t.Fatalf("Allocating block %d/%d failed: %s", i, j, err)
			}

			stat, err := os.Stat(filepath.Join(dir, fmt.Sprintf(filenamePat, 0, 0)))
			if err != nil {
				t.Fatalf("Stat for block %d%d failed: %s", i, j, err)
			}

			if stat.Size() != int64(ps*numPages) {
				t.Fatalf("Expected new block size %d but got %d", ps*numPages, stat.Size())
			}
		}
	}

	if err := table.allocateBlock(1, 1); err == nil {
		t.Fatal("Expected error but got none")
	}
	if err := table.allocateBlock(7, 1); err == nil {
		t.Fatal("Expected error but got none")
	}
	if err := table.allocateBlock(5, 1); err == nil {
		t.Fatal("Expected error but got none")
	}
}

func TestSkipTableLoad(t *testing.T) {
	dir, err := ioutil.TempDir("", "skiptable_test")
	if err != nil {
		t.Fatal(err)
	}

	table, err := New(dir, Opts{
		BlockRows:       16,
		BlockLineLength: 16,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Allocated file must be a multiple of page size.
	expSize := table.opts.BlockRows * table.opts.BlockLineLength
	ps := syscall.Getpagesize()
	numPages := expSize / ps
	if ps*numPages < expSize {
		numPages++
	}

	// Allocate a few blocks.
	for i := 0; i < 5; i++ {
		for j := 0; j < 4; j++ {
			err := table.allocateBlock(i, j)
			if err != nil {
				t.Fatalf("Allocating block %d/%d failed: %s", i, j, err)
			}

			stat, err := os.Stat(filepath.Join(dir, fmt.Sprintf(filenamePat, 0, 0)))
			if err != nil {
				t.Fatalf("Stat for block %d%d failed: %s", i, j, err)
			}

			if stat.Size() != int64(ps*numPages) {
				t.Fatalf("Expected new block size %d but got %d", ps*numPages, stat.Size())
			}
			b := table.blocks[i][j]

			// Put a marker so we can verify the loaded order is correct.
			binary.BigEndian.PutUint32(b[:4], uint32(i))
			binary.BigEndian.PutUint32(b[4:8], uint32(j))
		}
	}

	table.Close()

	table, err = New(dir, Opts{
		BlockRows:       16,
		BlockLineLength: 16,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Validate loaded blocks.
	for i := 0; i < 5; i++ {
		for j := 0; j < 4; j++ {
			b := table.blocks[i][j]

			// Put a marker so we can verify the loaded order is correct.
			if uint32(i) != binary.BigEndian.Uint32(b[:4]) {
				t.Fatal("wrong block position loaded")
			}
			if uint32(j) != binary.BigEndian.Uint32(b[4:8]) {
				t.Fatal("wrong block position loaded")
			}
		}
	}

}

func TestSkipTableStore(t *testing.T) {
	dir, err := ioutil.TempDir("", "skiptable_test")
	if err != nil {
		t.Fatal(err)
	}

	table, err := New(dir, Opts{
		BlockRows:       16,
		BlockLineLength: 16,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer table.Close()

	// if err := table.Store(123, 1024, 3); err != nil {
	// 	t.Fatal(err)
	// }

	// offset, err := table.Offset(123, 4)
	// if err != nil {
	// 	t.Fatal(err)
	// }
	// if offset != 1024 {
	// 	t.Errorf("Expected offset 1024 but got %d", offset)
	// }
}