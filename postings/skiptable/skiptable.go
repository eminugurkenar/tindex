package skiptable

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"

	"github.com/edsrzf/mmap-go"
)

type Key uint32

type Value uint32

type Set []Value

type SkipTable struct {
	files  []*os.File
	blocks [][]mmap.MMap

	dir  string
	opts Opts
}

const (
	filenameGlob = "st-*-*"
	filenamePat  = "st-%d-%d"
)

type Opts struct {
	BlockRows       int
	BlockLineLength int
}

// DefaultOpts are the default options for skip tables.
// They result in block files of 8MB each.
var DefaultOpts = Opts{
	BlockRows:       1 << 15,
	BlockLineLength: 1 << 8,
}

func New(dir string, opts Opts) (*SkipTable, error) {
	st := &SkipTable{
		dir:  dir,
		opts: opts,
	}

	filenames, err := filepath.Glob(filepath.Join(dir, filenameGlob))
	if err != nil {
		return nil, err
	}
	sort.Strings(filenames)

	for _, fn := range filenames {
		parts := strings.Split(fn, "-")
		if len(parts) != 3 {
			return nil, fmt.Errorf("unexpected file %s", fn)
		}
		row, err := strconv.Atoi(parts[1])
		if err != nil {
			return nil, fmt.Errorf("unexpected file %s", fn)
		}
		col, err := strconv.Atoi(parts[2])
		if err != nil {
			return nil, fmt.Errorf("unexpected file %s", fn)
		}

		if err := st.loadBlock(row, col); err != nil {
			return nil, fmt.Errorf("error loading block %s: %s", fn, err)
		}
	}
	return st, nil
}

func (st *SkipTable) filename(row, col int) string {
	return fmt.Sprintf(filepath.Join(st.dir, filenamePat), row, col)
}

func (st *SkipTable) blockFileSize() int64 {
	size := st.opts.BlockRows * st.opts.BlockLineLength
	ps := syscall.Getpagesize()
	numPages := size / ps

	if numPages*ps < size {
		numPages++
	}
	return int64(ps * numPages)
}

func (st *SkipTable) allocateBlock(row, col int) error {
	// For the first column the row must be new.
	if col == 0 && len(st.blocks) != row {
		return fmt.Errorf("inconsistent allocation row")
	}
	if col > 0 && len(st.blocks) != row+1 {
		return fmt.Errorf("inconsistent allocation column")
	}

	fn := st.filename(row, col)

	if _, err := os.Stat(fn); !os.IsNotExist(err) {
		return fmt.Errorf("file %s already exists", fn)
	}

	f, err := os.Create(fn)
	if err != nil {
		return fmt.Errorf("creating %s failed: %s", fn, err)
	}
	st.files = append(st.files, f)

	if err := f.Truncate(st.blockFileSize()); err != nil {
		return err
	}

	b, err := mmap.Map(f, mmap.RDWR, 0)
	if err != nil {
		return fmt.Errorf("mmapping %s failed: %s", f.Name(), err)
	}

	if col == 0 {
		st.blocks = append(st.blocks, []mmap.MMap{})
	}
	st.blocks[row] = append(st.blocks[row], b)

	return nil
}

func (st *SkipTable) loadBlock(row, col int) error {
	// For the first column the row must be new.
	if col == 0 && len(st.blocks) != row {
		return fmt.Errorf("inconsistent allocation row")
	}
	if col > 0 && len(st.blocks) != row+1 {
		return fmt.Errorf("inconsistent allocation column")
	}

	stat, err := os.Stat(st.filename(row, col))
	if err != nil {
		return err
	}
	if stat.Size() != st.blockFileSize() {
		return fmt.Errorf("unexpected file size %d", stat.Size())
	}

	f, err := os.OpenFile(st.filename(row, col), os.O_RDWR, 0666)
	if err != nil {
		return err
	}
	st.files = append(st.files, f)

	b, err := mmap.Map(f, mmap.RDWR, 0)
	if err != nil {
		return err
	}

	if col == 0 {
		st.blocks = append(st.blocks, []mmap.MMap{})
	}
	st.blocks[row] = append(st.blocks[row], b)

	return nil

}

func (st *SkipTable) row(k Key) (int, int) {
	br := int(k) / st.opts.BlockRows
	bo := (int(k) - br*st.opts.BlockRows) * st.opts.BlockLineLength
	return br, bo
}

func (st *SkipTable) Offset(k Key, v Value) (uint32, error) {
	br, bo := st.row(k)

	for _, block := range st.blocks[br] {
		b := block[bo : bo+st.opts.BlockLineLength]

		var i int
		var prevOffset uint32
		for {
			val := Value(binary.BigEndian.Uint32(b[i+4 : i+8]))
			if val > v {
				return prevOffset, nil
			}

			prevOffset = binary.BigEndian.Uint32(b[i : i+4])

			i++
		}
	}
	return 0, fmt.Errorf("Offset for key %v not found", k)
}

func (st *SkipTable) Store(k Key, offset uint32, start Value) error {
	br, bo := st.row(k)

	for _, block := range st.blocks[br] {
		b := block[bo : bo+st.opts.BlockLineLength]

		// TODO(fabxc): delta-compress this.
		var i int
		for {
			if binary.BigEndian.Uint64(b[i:i+8]) != 0 {
				i++
				continue
			}
			binary.BigEndian.PutUint32(b[i:i+4], offset)
			binary.BigEndian.PutUint32(b[i+4:i+8], uint32(start))
			return nil
		}
	}

	return fmt.Errorf("error")
}

func (st *SkipTable) Sync() error {
	for _, br := range st.blocks {
		for _, b := range br {
			if err := b.Flush(); err != nil {
				return err
			}
		}
	}
	for _, f := range st.files {
		if err := f.Sync(); err != nil {
			return err
		}
	}
	return nil

}

func (st *SkipTable) Close() error {
	if err := st.Sync(); err != nil {
		return err
	}
	for _, br := range st.blocks {
		for _, b := range br {
			if err := b.Unmap(); err != nil {
				return err
			}
		}
	}
	for _, f := range st.files {
		if err := f.Close(); err != nil {
			return err
		}
	}
	return nil
}
