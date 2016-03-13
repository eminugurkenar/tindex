package skiptable

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
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

const (
	lineUnused    = '\x00'
	lineEncUint32 = '\x01'
)

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
	if row > len(st.blocks) {
		return fmt.Errorf("inconsistent allocation row")
	} else if row == len(st.blocks) {
		if col != 0 {
			return fmt.Errorf("inconsistent allocation row")
		}
	} else if len(st.blocks[row]) > col {
		return fmt.Errorf("inconsistent allocation col")
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
	if col > 0 && len(st.blocks[row]) != col {
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

var ErrNotFound = errors.New("not found")

func findUint32(l *line, v Value) (uint32, error) {
	b := make([]byte, 8)

	if _, err := l.Read(b); err != nil {
		return 0, err
	}
	// The initial val/offset pair is always set.
	lastOffset := binary.BigEndian.Uint32(b[4:])

	// The first entry might already be higher than what we are looking for.
	// In that case the first relevant values are at this offset.
	if Value(binary.BigEndian.Uint32(b[:4])) >= v {
		return lastOffset, nil
	}

	for {
		_, err := l.Read(b)
		// If we've reached the end of the skip list the last offset is
		// our result.
		if err == io.EOF {
			break
		}
		if err != nil {
			return 0, err
		}
		// If the value is larger than what we are searching for, the last
		// offset is the closest result.
		value := Value(binary.BigEndian.Uint32(b[:4]))
		if value > v {
			break
		}
		lastOffset = binary.BigEndian.Uint32(b[4:])
		if value == v {
			break
		}
	}

	return lastOffset, nil
}

func initUint32(l *line, v Value, offset uint32) error {
	b := make([]byte, 8)
	binary.BigEndian.PutUint32(b[:4], uint32(v))
	binary.BigEndian.PutUint32(b[4:], offset)

	_, err := l.Write(b)
	return err
}

func storeUint32(l *line, v Value, offset uint32) error {
	b := make([]byte, 8)

	n, err := l.Read(b)
	if err != nil {
		return err
	}
	// The initial val/offset pair is always set but we don't need it.

	for {
		n, err = l.Read(b)
		// If we've reached the end of the skip list the last offset is
		// our result.
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		// If the value is larger than what we are searching for, the last
		// offset is the closest result.
		if Value(binary.BigEndian.Uint32(b[:4])) == 0 {
			break
		}
	}
	for n > 0 {
		n--
		if err = l.UnreadByte(); err != nil {
			return err
		}
	}

	binary.BigEndian.PutUint32(b[:4], uint32(v))
	binary.BigEndian.PutUint32(b[4:], offset)

	_, err = l.Write(b)
	return err
}

func (st *SkipTable) Offset(k Key, v Value) (uint32, error) {
	br, bo := st.row(k)

	if br >= len(st.blocks) {
		return 0, ErrNotFound
	}

	l := &line{
		blocks:     st.blocks[br],
		offset:     bo,
		lineLength: st.opts.BlockLineLength,
	}

	// Read the byte specifying the line encoding.
	c, err := l.ReadByte()
	if err != nil {
		return 0, err
	}

	switch c {
	case lineUnused:
		return 0, ErrNotFound
	case lineEncUint32:
		return findUint32(l, v)
	}

	return 0, fmt.Errorf("unknown line encoding %q", c)
}

func (st *SkipTable) Store(k Key, start Value, offset uint32) error {
	br, bo := st.row(k)

	for len(st.blocks) <= br {
		if err := st.allocateBlock(len(st.blocks), 0); err != nil {
			return err
		}
	}

	l := &line{
		blocks:     st.blocks[br],
		offset:     bo,
		lineLength: st.opts.BlockLineLength,
		allocate: func(col int) (mmap.MMap, error) {
			err := st.allocateBlock(br, col)
			if err != nil {
				return nil, err
			}
			return st.blocks[br][col], nil
		},
	}

	// Read the byte specifying the line encoding.
	c, err := l.ReadByte()
	if err != nil {
		return err
	}

	switch c {
	case lineUnused:
		if err := l.UnreadByte(); err != nil {
			return err
		}
		if err := l.WriteByte(lineEncUint32); err != nil {
			return err
		}
		return initUint32(l, start, offset)
	case lineEncUint32:
		return storeUint32(l, start, offset)
	}

	return fmt.Errorf("unknown line encoding %q", c)
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

type line struct {
	allocate   func(col int) (mmap.MMap, error)
	blocks     []mmap.MMap
	col        int
	offset     int
	pos        int
	lineLength int
}

func (l *line) UnreadByte() error {
	if l.pos > 0 {
		l.pos--
		return nil
	}
	if l.col == 0 {
		return fmt.Errorf("cannot unread from zero position")
	}
	l.col--
	l.pos = l.lineLength - 1
	return nil
}

func (l *line) Read(b []byte) (n int, err error) {
	for i := range b {
		if b[i], err = l.ReadByte(); err != nil {
			return n, err
		}
		n++
	}
	return n, nil
}

func (l *line) Write(b []byte) (n int, err error) {
	for _, c := range b {
		if err = l.WriteByte(c); err != nil {
			return n, err
		}
		n++
	}
	return n, err
}

func (l *line) ReadByte() (byte, error) {
	if l.col == len(l.blocks) {
		return 0, io.EOF
	}
	b := l.blocks[l.col]
	if l.pos == l.lineLength {
		l.col++
		l.pos = 0
		return l.ReadByte()
	}
	c := b[l.offset+l.pos]
	l.pos++
	return c, nil
}

func (l *line) WriteByte(c byte) error {
	var b mmap.MMap
	var err error
	if l.pos == l.lineLength {
		l.pos = 0
		l.col++
		if l.col == len(l.blocks) {
			b, err = l.allocate(l.col)
			if err != nil {
				return err
			}
			l.blocks = append(l.blocks, b)
		} else {
			b = l.blocks[l.col]
		}
	} else {
		b = l.blocks[l.col]
	}

	b[l.offset+l.pos] = c
	l.pos++

	return nil
}
