package skiptable

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"math/rand"
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

type testData struct {
	k      Key
	v      Value
	offset uint32
}

func generateData(n int, maxKey uint32) []testData {
	data := make([]testData, n)

	for i := range data {
		data[i].k = Key(rand.Intn(int(maxKey)))
		data[i].v = Value(i*10 + rand.Intn(5))
		data[i].offset = uint32(rand.Int31n(1000))
	}

	return data
}

func hexdump(t *testing.T, table *SkipTable) {
	filenames, err := filepath.Glob(filepath.Join(table.dir, filenameGlob))
	if err != nil {
		t.Fatal(err)
	}

	for _, fn := range filenames {
		f, err := os.Open(fn)
		if err != nil {
			t.Fatal(err)
		}
		b, err := ioutil.ReadAll(f)
		if err != nil {
			t.Fatal(err)
		}
		// Truncate page padding.
		t.Logf("%s:\n%s\n", fn, hex.Dump(b[:table.opts.BlockRows*table.opts.BlockLineLength]))
	}
}

func TestSkipTableStore(t *testing.T) {
	dir, err := ioutil.TempDir("", "skiptable_test")
	if err != nil {
		t.Fatal(err)
	}

	table, err := New(dir, Opts{
		BlockRows:       10,
		BlockLineLength: 128,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer table.Close()

	data := generateData(10, 1)

	for _, d := range data {
		if err := table.Store(d.k, d.v, d.offset); err != nil {
			t.Fatal(err)
		}
	}

	for _, d := range data {
		offset, err := table.Offset(d.k, d.v) //+Value(rand.Intn(5)))
		if err != nil {
			t.Fatal(err)
		}
		if offset != d.offset {
			t.Errorf("key: %v, val: %v, offset: %v.\tgot offset: %v", d.k, d.v, d.offset, offset)
		}
	}

	hexdump(t, table)
}

func BenchmarkSkipTableStore(b *testing.B) {
	dir, err := ioutil.TempDir("", "skiptable_test")
	if err != nil {
		b.Fatal(err)
	}

	table, err := New(dir, Opts{
		BlockRows:       4096,
		BlockLineLength: 512,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer table.Close()

	data := generateData(int(b.N), uint32(b.N)/50+1)

	b.ResetTimer()

	for _, d := range data {
		if err := table.Store(d.k, d.v, d.offset); err != nil {
			b.Fatal(err)
		}
	}
}
func BenchmarkSkipTableOffset(b *testing.B) {
	dir, err := ioutil.TempDir("", "skiptable_test")
	if err != nil {
		b.Fatal(err)
	}

	table, err := New(dir, Opts{
		BlockRows:       4096,
		BlockLineLength: 512,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer table.Close()

	data := generateData(int(b.N), uint32(b.N)/50+1)

	for _, d := range data {
		if err := table.Store(d.k, d.v, d.offset); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()

	for _, d := range data {
		offset, err := table.Offset(d.k, d.v) //+Value(rand.Intn(5)))
		if err != nil {
			b.Fatal(err)
		}
		_ = offset
	}
}
