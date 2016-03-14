package postings

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/edsrzf/mmap-go"

	"github.com/fabxc/tsindex/postings/skiptable"
)

type Key uint32

type Value uint32

type Pair struct {
	Value Value
	Ptr   uint64
}

type Set []Pair

type Postings interface {
	Get(k Key, from, to Value) (Set, error)
	Set(k Key, v Value, ptr uint64) error
	Sync() error
	Close() error
}

const indexBlockSize = 4096

type postings struct {
	skipTable *skiptable.SkipTable
	nextPage  int
	data      mmap.MMap
	file      *os.File
}

const indexFilename = "index"
const initialIndexFilesize = 1 << 15

func New(dir string) (Postings, error) {
	if err := os.Mkdir(filepath.Join(dir, "skiptable"), 0777); err != nil {
		return nil, err
	}
	st, err := skiptable.New(filepath.Join(dir, "skiptable"), skiptable.DefaultOpts)
	if err != nil {
		return nil, err
	}
	var f *os.File
	fn := filepath.Join(dir, indexFilename)

	_, err = os.Stat(fn)
	if os.IsNotExist(err) {
		f, err = os.Create(fn)
		if err != nil {
			return nil, err
		}
		if err = f.Truncate(initialIndexFilesize); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	} else {
		f, err = os.OpenFile(fn, os.O_RDWR, 0666)
		if err != nil {
			return nil, fmt.Errorf("error opening file %q: %s", fn, err)
		}
	}

	// TODO(fabxc): verify file is padded to page size.

	data, err := mmap.Map(f, mmap.RDWR, 0)
	if err != nil {
		return nil, fmt.Errorf("mmap error: %s", err)
	}

	return &postings{
		skipTable: st,
		data:      data,
		file:      f,
	}, nil
}

func (p *postings) allocatePage(k Key, v Value) (uint32, error) {
	fmt.Println("alloc")

	stat, err := p.file.Stat()
	if err != nil {
		return 0, err
	}
	// Grow file.
	if int64(p.nextPage*indexBlockSize) > stat.Size() {
		fmt.Println("grow file")
		if err := p.data.Flush(); err != nil {
			return 0, err
		}
		if err := p.data.Unmap(); err != nil {
			return 0, err
		}
		if err := p.file.Truncate(stat.Size() * 2); err != nil {
			return 0, err
		}
		data, err := mmap.Map(p.file, mmap.RDWR, 0)
		if err != nil {
			return 0, err
		}
		p.data = data
	}

	offset := uint32(p.nextPage)
	if err := p.skipTable.Store(skiptable.Key(k), skiptable.Value(v), offset); err != nil {
		return 0, fmt.Errorf("skip table store: %s", err)
	}
	p.nextPage++

	return offset, nil
}

func (p *postings) Set(k Key, v Value, pointer uint64) error {
	fmt.Println("set", k, v, pointer)
	offset, err := p.skipTable.Offset(skiptable.Key(k), skiptable.Value(v))
	if err != nil {
		if err != skiptable.ErrNotFound {
			return fmt.Errorf("error querying skip table: %s", err)
		}
		offset, err = p.allocatePage(k, v)
		if err != nil {
			return err
		}
	}

	irw := &indexRW{
		allocate: func() ([]byte, uint32, error) {
			fmt.Println("allocate from write")
			off, err := p.allocatePage(k, v)
			if err != nil {
				return nil, 0, fmt.Errorf("page allocation failed: %s", err)
			}
			return p.data, off, nil
		},
		offsets: []uint32{offset},
		data:    p.data,
		pos:     1,
	}

	b := make([]byte, 12)
	for {
		n, err := irw.Read(b)
		if err == io.EOF {
			fmt.Println("write by eof")
			for i := 0; i < n; i++ {
				irw.UnreadByte()
			}
			break
		}
		if err != nil {
			return err
		}

		val := Value(binary.BigEndian.Uint32(b[:4]))
		ptr := binary.BigEndian.Uint64(b[4:])

		if val == 0 {
			fmt.Println("write by 0")
			// We ran beyond the last pair but there's still room in the current
			// page. Undo the read.
			for i := 0; i < 12; i++ {
				irw.UnreadByte()
			}
			break
		}
		if val < v {
			continue
		}
		if val == v {
			if ptr == pointer {
				return nil
			}
			return fmt.Errorf("pointer for value %d already set to %d", v, ptr)
		}
		if val > v {
			return fmt.Errorf("cannot insert smaller value")
		}
	}
	binary.BigEndian.PutUint32(b[:4], uint32(v))
	binary.BigEndian.PutUint64(b[4:], pointer)

	_, err = irw.Write(b)
	return err
}

func (p *postings) Get(k Key, from, to Value) (Set, error) {
	fmt.Println("get", k, from, to)
	offs, err := p.skipTable.RangeOffsets(skiptable.Key(k), skiptable.Value(from), skiptable.Value(to))
	if err != nil {
		return nil, fmt.Errorf("skip table: %s", err)
	}

	var res Set

	irw := &indexRW{
		data:    p.data,
		offsets: offs,
		pos:     1,
	}

	b := make([]byte, 12)
	for {
		_, err := irw.Read(b)
		if err == io.EOF {
			fmt.Println("eof")
			break
		}
		if err != nil {
			return nil, err
		}

		val := Value(binary.BigEndian.Uint32(b[:4]))
		ptr := binary.BigEndian.Uint64(b[4:])

		if val < from {
			continue
		}
		if val > to {
			fmt.Printf("reached val > to (%d) at %d %d\n", val, irw.block, irw.pos)
			break
		}

		res = append(res, Pair{
			Value: Value(val),
			Ptr:   ptr,
		})
	}

	return res, nil
}

func (p *postings) Sync() error {
	if err := p.skipTable.Sync(); err != nil {
		return err
	}
	if err := p.data.Flush(); err != nil {
		return err
	}
	if err := p.file.Sync(); err != nil {
		return err
	}

	return nil
}

func (p *postings) Close() error {
	if err := p.Sync(); err != nil {
		return err
	}
	if err := p.skipTable.Close(); err != nil {
		return err
	}
	if err := p.data.Unmap(); err != nil {
		return err
	}
	if err := p.file.Close(); err != nil {
		return err
	}

	return nil
}

func Intersect(sets ...Set) Set {
	if len(sets) == 0 {
		return nil
	}

	s1 := sets[0]
	for _, s2 := range sets[1:] {
		s1 = intersect(s1, s2)
	}
	return s1
}

func intersect(s1, s2 Set) Set {
	var result Set
	var i, j int
	for {
		if i >= len(s1) || j >= len(s2) {
			break
		}
		if s1[i].Value < s2[j].Value {
			i++
		} else if s2[j].Value < s1[i].Value {
			j++
		} else {
			result = append(result, s2[j])
			j++
			i++
		}
	}
	return result
}

type indexRW struct {
	allocate func() ([]byte, uint32, error)
	data     []byte
	offsets  []uint32
	block    int
	pos      int
}

func (irw *indexRW) Read(b []byte) (n int, err error) {
	// fmt.Println("read", 12)
	for i := range b {
		if b[i], err = irw.ReadByte(); err != nil {
			return n, err
		}
		n++
	}
	return n, nil
}

func (irw *indexRW) UnreadByte() error {
	if irw.pos > 0 {
		irw.pos--
		return nil
	}
	if irw.block == 0 {
		return fmt.Errorf("cannot unread from zero position")
	}
	irw.block--
	irw.pos = indexBlockSize - 1
	return nil
}

func (irw *indexRW) ReadByte() (byte, error) {
	if irw.pos == indexBlockSize {
		if irw.block == len(irw.offsets)-1 {
			return 0, io.EOF
		}
		// First byte is reserved.
		irw.pos = 1
		irw.block++
	}
	// fmt.Println("access offset", irw.offsets, irw.block)
	offset := irw.offsets[irw.block]
	c := irw.data[offset*indexBlockSize+uint32(irw.pos)]
	irw.pos++

	return c, nil
}

func (irw *indexRW) Write(b []byte) (n int, err error) {
	fmt.Println("write", b, irw.offsets[irw.block], irw.pos)
	for _, c := range b {
		if err = irw.WriteByte(c); err != nil {
			return n, err
		}
		n++
	}
	return n, nil
}

func (irw *indexRW) WriteByte(c byte) error {
	// fmt.Println("writeb", c, irw.pos, irw.offsets[irw.block], indexBlockSize)
	if irw.pos == indexBlockSize {
		if irw.block == len(irw.offsets)-1 {
			d, off, err := irw.allocate()
			if err != nil {
				return err
			}
			irw.data = d
			irw.offsets = append(irw.offsets, off)
		}
		irw.block++
		irw.pos = 1
		fmt.Println(irw.block, irw.offsets)
	}

	offset := irw.offsets[irw.block]
	irw.data[offset*indexBlockSize+uint32(irw.pos)] = c

	irw.pos++

	return nil
}
