package skiptable

import (
	"bytes"
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
	BlockLineLength: 1 << 7,
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

func (st *SkipTable) getLine(k Key, create bool) ([]byte, error) {
	br, bo := st.row(k)

	if create {
		for br >= len(st.blocks) {
			if err := st.allocateBlock(len(st.blocks), 0); err != nil {
				return nil, err
			}
		}
	} else if br >= len(st.blocks) {
		return nil, ErrNotFound
	}

	line := make([]byte, 0, st.opts.BlockLineLength*len(st.blocks[br]))
	for _, block := range st.blocks[br] {
		line = append(line, []byte(block[bo:bo+st.opts.BlockLineLength])...)
	}

	return line, nil
}

var ErrNotFound = errors.New("not found")

func (st *SkipTable) RangeOffsets(k Key, min, max Value) ([]uint32, error) {
	line, err := st.getLine(k, false)
	if err != nil {
		return nil, err
	}
	r, err := newLineReader(line)
	if err != nil {
		return nil, err
	}

	return rw.findRange(min, max), nil
}

func (st *SkipTable) Offset(k Key, v Value) (uint32, error) {
	line, err := st.getLine(k, false)
	if err != nil {
		return 0, err
	}
	r, err := newLineReader(line)
	if err != nil {
		return 0, err
	}

	return rw.find(v), nil
}

func (st *SkipTable) Store(k Key, start Value, offset uint32) error {
	line, err := st.getLine(k, true)
	if err != nil {
		return err
	}
	a, err := newLineAppender(line)
	if err != nil {
		return err
	}
	b, i, err := a.append(start, offset)
	if err != nil {
		return err
	}

	// Insert the new bytes b at position i.
	// TODO(fabxc): this is a bit hacky for now. We've to figure out whether
	// to pass down the original blocks into the readerw.
	br, bo := st.row(k)

	fmt.Println(b, i)
	_, n := binary.Varint([]byte(st.blocks[br][0][bo:]))
	i += n

	block := st.blocks[br][i/st.opts.BlockLineLength]
	copy([]byte(block[bo+i%st.opts.BlockLineLength:bo+i%st.opts.BlockLineLength+len(b)]), b)

	return nil
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

type lineReader interface {
	find(v Value) (offset uint32)
	findRange(min, max Value) (offsets []uint32)
}

type lineEncoding int

const (
	lineEncodingUnused = lineEncoding(iota)
	lineEncodingUint
	lineEncodingVarint
	lineEncodingDelta
)

// newLineReader reads a line of skipentries.
//
// TODO(fabxc): investigate whether not constructing the entire line but pass down
// the blocks, offset, and line length instead has relevant performance implications.
func newLineReader(line []byte) (lineReader, error) {
	e, n := binary.Uvarint(line)
	if n == 0 {
		return nil, fmt.Errorf("unable to read encoding varint")
	}
	enc := lineEncoding(e)

	switch enc {
	case lineEncodingUnused:
		return nil, ErrNotFound
	case lineEncodingUint:
		return newLineReaderUint(line[n:]), nil
	}

	return nil, fmt.Errorf("unknown line encoding %q", enc)
}

type lineReaderUint struct {
	line []byte
}

func newLineReaderUint(line []byte) *lineReaderUint {
	return &lineReaderUint{
		line: line,
	}
}

// find implements the lineReader interface.
func (r *lineReaderUint) find(value Value) uint32 {
	off, _ := rw.findWithPos(value)
	return off
}

// findWithPos returns the offset for the value and the position of the
// next byte to read.
func (r *lineReaderUint) findWithPos(value Value) (uint32, int) {
	// The first value is guaranteed to exist and may also be zero.
	lastValue := Value(binary.BigEndian.Uint32(rw.line[:4]))

	i := 8
	for ; i+8 < len(rw.line); i += 8 {
		if value == lastValue {
			break
		}
		val := Value(binary.BigEndian.Uint32(rw.line[:4]))
		// Values in the skiplist must be monotonically increasing.
		// This signals the end.
		if val > value || val < lastValue {
			break
		}

		lastValue = val
	}

	return binary.BigEndian.Uint32(rw.line[i-4 : i]), i
}

// findRange implements the lineReader itnerface.
func (r *lineReaderUint) findRange(min, max Value) (offsets []uint32) {
	first, i := rw.findWithPos(min)
	offsets = append(offsets, first)

	lastVal := Value(binary.BigEndian.Uint32(rw.line[i-8 : i-4]))

	for ; i+8 < len(rw.line); i += 8 {
		val := Value(binary.BigEndian.Uint32(rw.line[:4]))
		if val < lastVal || val > max {
			break
		}
		offsets = append(offsets, binary.BigEndian.Uint32(rw.line[i+4:i+8]))
	}

	return offsets
}

var (
	errLineFull   = errors.New("line full")
	errOutOfOrder = errors.New("out of order")
)

// lineAppender adds an offset entry to a skiplist line.
type lineAppender interface {
	append(value Value, offset uint32) ([]byte, int, error)
}

func newLineAppender(line []byte) (lineAppender, error) {
	e, n := binary.Uvarint(line)
	if n == 0 {
		return nil, fmt.Errorf("unable to read encoding varint")
	}
	enc := lineEncoding(e)

	var fresh = false
	// If the line is still fresh, set an encoding to use.
	if enc == lineEncodingUnused {
		enc = lineEncodingUint
		fresh = true
		n = binary.PutUvarint(line, uint64(enc))
	}

	switch lineEncoding(enc) {
	case lineEncodingUint:
		return newLineAppenderUint(line[n:], fresh), nil
	}

	return nil, fmt.Errorf("unknown line encoding %q", enc)
}

// lineAppenderUint implements a lineAppender where values are unicoded
// as a sequence of uint32 big-endian value/offset pairs.
type lineAppenderUint struct {
	line  []byte
	fresh bool
}

func newLineAppenderUint(line []byte, fresh bool) *lineAppenderUint {
	return &lineAppenderUint{
		line:  line,
		fresh: fresh,
	}
}

// append implements the lineAppender interface.
func (a *lineAppenderUint) append(value Value, offset uint32) ([]byte, int, error) {
	if a.fresh {
		b := make([]byte, 8)
		binary.BigEndian.PutUint32(b[:4], uint32(value))
		binary.BigEndian.PutUint32(b[4:8], offset)

		a.fresh = false

		return b, 0, nil
	}

	var lastValue Value
	for i := 0; i+8 < len(a.line); i += 8 {
		val := Value(binary.BigEndian.Uint32(a.line[:4]))
		// If there's an entry to something larger than our current value,
		// the order is violated.
		if val >= value {
			return nil, 0, errOutOfOrder
		}
		// Values in the skiplist must be monotonically increasing.
		// This signals the end.
		if i > 0 && val < lastValue {
			b := make([]byte, 8)
			binary.BigEndian.PutUint32(b[:4], uint32(value))
			binary.BigEndian.PutUint32(b[4:8], offset)
			return b, i, nil
		}

		lastValue = val
	}

	return nil, 0, errLineFull
}

type blockLineRW struct {
	bl     [][]byte // Original data blocks.
	offset int64    // Offset to line segment start in each block.
	length int64    // Line length per block.
	exp    []byte   // Copy of the line across blocks.
	i      int64    // Current position.
}

func newBlockLineRW(bl [][]byte, off, length int64) *blockLineRW {
	var (
		n   = 0
		exp = make([]byte, len(bl)*length)
	)
	for i, b := range bl {
		n += copy(exp[n:], b[off:off+length])
	}
	return &blockLineRW{
		Reader: bytes.NewReader(exp),
		bl:     bl,
		offset: off,
		length: length,
	}
}

// Len returns the number of bytes of the unread portion of the
// slice.
func (r *Reader) Len() int {
	if rw.i >= int64(len(rw.exp)) {
		return 0
	}
	return int(int64(len(rw.exp)) - rw.i)
}

// Size returns the original length of the underlying byte slice.
// Size is the number of bytes available for reading via ReadAt.
// The returned value is always the same and is not affected by calls
// to any other method.
func (rw *blockLineRW) Size() int64 { return int64(len(rw.exp)) }

func (rw *blockLineRW) Read(b []byte) (n int, err error) {
	if len(b) == 0 {
		return 0, nil
	}
	if rw.i >= int64(len(rw.exp)) {
		return 0, io.EOF
	}
	n = copy(b, rw.exp[rw.i:])
	rw.i += int64(n)
	return
}

func (rw *blockLineRW) ReadAt(b []byte, off int64) (n int, err error) {
	// Cannot modify state - see io.ReaderAt.
	if off < 0 {
		return 0, errors.New("negative offset")
	}
	if off >= int64(len(rw.exp)) {
		return 0, io.EOF
	}
	n = copy(b, rw.exp[off:])
	if n < len(b) {
		err = io.EOF
	}
	return
}

func (rw *blockLineRW) ReadByte() (byte, error) {
	if rw.i >= int64(len(rw.exp)) {
		return 0, io.EOF
	}
	b := rw.exp[rw.i]
	rw.i++
	return b, nil
}

func (rw *blockLineRW) UnreadByte() error {
	if rw.i <= 0 {
		return errors.New("at beginning of slice")
	}
	rw.i--
	return nil
}

// Seek implements the io.Seeker interface.
func (rw *blockLineRW) Seek(offset int64, whence int) (int64, error) {
	var abs int64
	switch whence {
	case 0:
		abs = offset
	case 1:
		abs = int64(rw.i) + offset
	case 2:
		abs = int64(len(rw.exp)) + offset
	default:
		return 0, errors.New("invalid whence")
	}
	if abs < 0 {
		return 0, errors.New("negative position")
	}
	rw.i = abs
	return abs, nil
}

func (rw *blockLineRW) WriteByte(c byte) error {
	if rw.i >= int64(rw.exp) {
		return io.EOF
	}
	// Write to the expanded copy and the original data.
	rw.bl[rw.i/rw.length][rw.offset+(rw.i%rw.length)] = c
	rw.exp[rw.i] = c
	rw.i++
	return nil
}

func (rw *blockLineRW) Write(b []byte) (n int, err error) {
	// Writes are rare. We just reuse WriteByte for simplicity.
	for _, c := range b {
		if err = rw.WriteByte(c); err != nil {
			return
		}
		n++
	}
	return
}

func (rw *blockLineRW) WriteAt(b []byte, offset int64) (n int, err error) {
	pos := rw.i
	// Write are rare. We just reuse Seek and Write for simplicity.
	if err = rw.Seek(offset, 0); err != nil {
		return
	}
	if n, err = rw.Write(b); err != nil {
		return
	}
	// Jump back to original position.
	err = rw.Seek(rw.i, 0)
	return
}
