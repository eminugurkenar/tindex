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

func (st *SkipTable) rawLine(k Key, create bool) (*rawLine, error) {
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

	var blocks [][]byte
	for _, block := range st.blocks[br] {
		blocks = append(blocks, []byte(block))
	}

	return newBlockLineRW(blocks, int64(bo), int64(st.opts.BlockLineLength)), nil
}

var ErrNotFound = errors.New("not found")

func (st *SkipTable) RangeOffsets(k Key, min, max Value) ([]uint64, error) {
	line, err := st.rawLine(k, false)
	if err != nil {
		return nil, err
	}
	r, err := newLineReader(line)
	if err != nil {
		return nil, err
	}

	return r.findRange(min, max)
}

func (st *SkipTable) Offset(k Key, v Value) (uint64, error) {
	line, err := st.rawLine(k, false)
	if err != nil {
		return 0, err
	}
	r, err := newLineReader(line)
	if err != nil {
		return 0, err
	}

	return r.find(v)
}

func (st *SkipTable) Store(k Key, start Value, offset uint64) error {
	line, err := st.rawLine(k, true)
	if err != nil {
		return err
	}
	a, err := newLineAppender(line)
	if err != nil {
		return err
	}
	err = a.append(start, offset)
	if err != errLineFull {
		return err
	}

	// The line was full. Allocate a new block for the line and try again.
	br, _ := st.row(k)
	if err := st.allocateBlock(br, len(st.blocks[br])); err != nil {
		return err
	}
	line, err = st.rawLine(k, true)
	if err != nil {
		return err
	}
	a, err = newLineAppender(line)
	if err != nil {
		return err
	}
	return a.append(start, offset)
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
	find(v Value) (offset uint64, err error)
	findRange(min, max Value) (offsets []uint64, err error)
}

type lineEncoding int

const (
	lineEncodingUnused = lineEncoding(iota)
	lineEncodingVarint
	lineEncodingDelta
)

// newLineReader reads a line of skipentries.
//
// TODO(fabxc): investigate whether not constructing the entire line but pass down
// the blocks, offset, and line length instead has relevant performance implications.
func newLineReader(line *rawLine) (lineReader, error) {
	e, err := binary.ReadUvarint(line)
	if err != nil {
		return nil, fmt.Errorf("unable to read encoding varint: %s", err)
	}
	enc := lineEncoding(e)

	switch enc {
	case lineEncodingUnused:
		return nil, ErrNotFound
	case lineEncodingVarint:
		return newLineReaderVarint(line), nil
	}

	return nil, fmt.Errorf("unknown line encoding %q", enc)
}

type lineReaderVarint struct {
	line *rawLine
}

func newLineReaderVarint(line *rawLine) *lineReaderVarint {
	return &lineReaderVarint{line: line}
}

func (r *lineReaderVarint) next() (v uint64, o uint64, err error) {
	if v, err = binary.ReadUvarint(r.line); err != nil {
		return 0, 0, err
	}
	if o, err = binary.ReadUvarint(r.line); err != nil {
		return 0, 0, err
	}
	return v, o, err
}

// find implements the lineReader interface.
func (r *lineReaderVarint) find(value Value) (uint64, error) {
	val, off, err := r.next()
	if err != nil {
		return 0, err
	}
	var (
		lastVal = val
		lastOff = off
	)

	for {
		if Value(lastVal) == value {
			break
		}
		if val, off, err = r.next(); err != nil {
			if err == io.EOF {
				break
			}
			return 0, err
		}
		// We reached the end or stepped beyond the searched value.
		// The last offset is what we are looking for.
		if Value(val) > value || val == 0 {
			break
		}
		lastVal = val
		lastOff = off
	}

	return lastOff, nil
}

// findRange implements the lineReader itnerface.
func (r *lineReaderVarint) findRange(min, max Value) (offsets []uint64, err error) {
	val, off, err := r.next()
	if err != nil {
		return nil, err
	}
	var (
		lastVal = val
		lastOff = off
	)

	for {
		// The range might not be covered at all.
		if Value(lastVal) > max {
			return nil, nil
		}
		if Value(lastVal) == min {
			break
		}
		if val, off, err = r.next(); err != nil {
			if err == io.EOF {
				return append(offsets, lastOff), nil
			}
			return nil, err
		}
		// Values in the skiplist must be strictly monotonically increasing.
		// This signals the end.
		if val == 0 {
			return append(offsets, lastOff), nil
		}
		// If the value is larger than the minimum, the the previous pair
		// contained the correct starting offset.
		if Value(val) > min {
			offsets = append(offsets, lastOff)
			lastOff = off
			break
		}
		lastVal = val
		lastOff = off
	}

	offsets = append(offsets, lastOff)

	// We consumed the first one or two offsets with min/max range.
	// Scan everything further until we reached the max.
	for {
		if Value(lastVal) == max {
			break
		}
		if val, off, err = r.next(); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		// We reached the end or stepped beyond max. The previous offset
		// is the last relevant one.
		if Value(val) > max || val == 0 {
			break
		}
		lastVal = val
		lastOff = off
	}

	return offsets, nil
}

var (
	errLineFull   = errors.New("line full")
	errOutOfOrder = errors.New("out of order")
)

// lineAppender adds an offset entry to a skiplist line.
type lineAppender interface {
	append(value Value, offset uint64) error
}

func newLineAppender(line *rawLine) (lineAppender, error) {
	e, _, err := readUvarint(line)
	if err != nil {
		return nil, fmt.Errorf("unable to read encoding varint")
	}
	enc := lineEncoding(e)

	var fresh = false
	// If the line is still fresh, set an encoding to use.
	if enc == lineEncodingUnused {
		enc = lineEncodingVarint
		fresh = true
		if _, err := line.Seek(0, 0); err != nil {
			return nil, err
		}
		if _, err := writeUvarint(line, uint64(enc)); err != nil {
			return nil, err
		}
	}

	switch enc {
	case lineEncodingVarint:
		return newLineAppenderVarint(line, fresh), nil
	}

	return nil, fmt.Errorf("unknown line encoding %q", enc)
}

type lineAppenderVarint struct {
	line  *rawLine
	fresh bool
}

func newLineAppenderVarint(line *rawLine, fresh bool) *lineAppenderVarint {
	return &lineAppenderVarint{
		line:  line,
		fresh: fresh,
	}
}

// append implements the lineAppender interface.
func (a *lineAppenderVarint) append(value Value, offset uint64) error {
	if a.fresh {
		if _, err := writeUvarint(a.line, uint64(value)); err != nil {
			return err
		}
		if _, err := writeUvarint(a.line, offset); err != nil {
			return err
		}
		a.fresh = false
		return nil
	}

	lastVal, _, err := readUvarint(a.line)
	if err != nil {
		return err
	}
	// Consume and discard offset
	if _, _, err := readUvarint(a.line); err != nil {
		return err
	}

	for {
		if Value(lastVal) >= value {
			return errOutOfOrder
		}
		val, n, err := readUvarint(a.line)
		if err != nil {
			if err == io.EOF {
				return errLineFull
			}
			return err
		}

		if val == 0 {
			// Ensure sufficient space so we don't have to unwrite and
			// a new line segment is allocated.
			if a.line.Len() < 2*binary.MaxVarintLen64 {
				return errLineFull
			}
			// Jump back to where the last varint started
			if _, err := a.line.Seek(int64(-n), 1); err != nil {
				return err
			}
			if _, err := writeUvarint(a.line, uint64(value)); err != nil {
				return err
			}
			if _, err := writeUvarint(a.line, offset); err != nil {
				return err
			}
			return nil
		}
		// Consume and discard offset.
		if _, _, err = readUvarint(a.line); err != nil {
			return err
		}

		lastVal = val
	}
}

type rawLine struct {
	bl     [][]byte // Original data blocks.
	offset int64    // Offset to line segment start in each block.
	length int64    // Line length per block.

	exp []byte // Copy of the expanded line across blocks.
	i   int64  // Current position.
}

func newBlockLineRW(bl [][]byte, off, length int64) *rawLine {
	var (
		n   = 0
		exp = make([]byte, len(bl)*int(length))
	)
	for _, b := range bl {
		n += copy(exp[n:], b[off:off+length])
	}
	return &rawLine{
		exp:    exp,
		bl:     bl,
		offset: off,
		length: length,
	}
}

// Len returns the number of bytes of the unread portion of the
// slice.
func (rw *rawLine) Len() int {
	if rw.i >= int64(len(rw.exp)) {
		return 0
	}
	return int(int64(len(rw.exp)) - rw.i)
}

// Size returns the original length of the underlying byte slice.
// Size is the number of bytes available for reading via ReadAt.
// The returned value is always the same and is not affected by calls
// to any other method.
func (rw *rawLine) Size() int64 { return int64(len(rw.exp)) }

func (rw *rawLine) Read(b []byte) (n int, err error) {
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

func (rw *rawLine) ReadAt(b []byte, off int64) (n int, err error) {
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

func (rw *rawLine) ReadByte() (byte, error) {
	if rw.i >= int64(len(rw.exp)) {
		return 0, io.EOF
	}
	b := rw.exp[rw.i]
	rw.i++
	return b, nil
}

func (rw *rawLine) UnreadByte() error {
	if rw.i <= 0 {
		return errors.New("at beginning of slice")
	}
	rw.i--
	return nil
}

// Seek implements the io.Seeker interface.
func (rw *rawLine) Seek(offset int64, whence int) (int64, error) {
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

func (rw *rawLine) WriteByte(c byte) error {
	if rw.i >= int64(len(rw.exp)) {
		return io.EOF
	}
	// Write to the expanded copy and the original data.
	rw.bl[rw.i/rw.length][rw.offset+(rw.i%rw.length)] = c
	rw.exp[rw.i] = c
	rw.i++
	return nil
}

func (rw *rawLine) Write(b []byte) (n int, err error) {
	// Writes are rare. We just reuse WriteByte for simplicity.
	for _, c := range b {
		if err = rw.WriteByte(c); err != nil {
			return
		}
		n++
	}
	return
}

func (rw *rawLine) WriteAt(b []byte, offset int64) (n int, err error) {
	pos := rw.i
	// Write are rare. We just reuse Seek and Write for simplicity.
	if _, err = rw.Seek(offset, 0); err != nil {
		return
	}
	if n, err = rw.Write(b); err != nil {
		return
	}
	// Jump back to original position.
	_, err = rw.Seek(pos, 0)
	return
}

func writeUvarint(w io.ByteWriter, x uint64) (i int, err error) {
	for x >= 0x80 {
		if err = w.WriteByte(byte(x) | 0x80); err != nil {
			return i, err
		}
		x >>= 7
		i++
	}
	if err = w.WriteByte(byte(x)); err != nil {
		return i, err
	}
	return i + 1, err
}

func readUvarint(r io.ByteReader) (uint64, int, error) {
	var (
		x uint64
		s uint
	)
	for i := 0; ; i++ {
		b, err := r.ReadByte()
		if err != nil {
			return x, i, err
		}
		if b < 0x80 {
			if i > 9 || i == 9 && b > 1 {
				return x, i + 1, errors.New("varint overflows a 64-bit integer")
			}
			return x | uint64(b)<<s, i + 1, nil
		}
		x |= uint64(b&0x7f) << s
		s += 7
	}
}
