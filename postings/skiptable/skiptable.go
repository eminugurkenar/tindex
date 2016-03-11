package skiptable

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	"github.com/edsrzf/mmap-go"
)

type Key uint32

type Value uint32

type Set []Value

type skipTable struct {
	files  []*os.File
	blocks []mmap.MMap

	dir string
}

const (
	skipTableFileSize = 4096 * 8
	skipLineLength    = 1024    // TODO(fabxc): reduce after delta compression.
	skipTableRows     = 1 << 10 // TODO(fabxc): increase later.
)

func New(dir string) (*skipTable, error) {
	st := &skipTable{}
	return st, st.init()
}

func (st *skipTable) init() error {
	filenames, err := filepath.Glob(filepath.Join(st.dir, "skip-0-*"))
	if err != nil {
		return fmt.Errorf("finding table files failed: %s", err)
	}

	for _, fn := range filenames {
		f, err := os.Open(fn)
		if err != nil {
			return fmt.Errorf("opening %q failed: %s", fn, err)
		}

		b, err := mmap.Map(f, mmap.RDWR, 0)
		if err != nil {
			return fmt.Errorf("mmapping %s failed: %s", fn, err)
		}

		st.files = append(st.files, f)
		st.blocks = append(st.blocks, b)
	}

	if len(st.files) == 0 {
		f, err := os.Create(filepath.Join(st.dir, "skip-0-0"))
		if err != nil {
			return fmt.Errorf("creating %s failed: %s", "skip-0-0", err)
		}
		ps := int64(syscall.Getpagesize())
		numPages := (skipLineLength * skipTableRows) / ps
		if numPages*ps < skipLineLength*skipTableRows {
			numPages++
		}
		if err := f.Truncate(ps * numPages); err != nil {
			return err
		}

		b, err := mmap.Map(f, mmap.RDWR, 0)
		if err != nil {
			return fmt.Errorf("mmapping %s failed: %s", f.Name(), err)
		}

		st.files = append(st.files, f)
		st.blocks = append(st.blocks, b)
	}

	return nil
}

func (st *skipTable) offset(k Key, v Value) (uint32, error) {
	for _, block := range st.blocks {
		blockOffset := int32(k) * skipLineLength
		b := block[blockOffset : blockOffset+skipLineLength]

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

func (st *skipTable) store(k Key, offset uint32, start Value) error {
	for _, block := range st.blocks {
		blockOffset := int32(k) * skipLineLength
		b := block[blockOffset : blockOffset+skipLineLength]

		// TODO(fabxc): delta-compress this.
		var i int
		for {
			if binary.BigEndian.Uint64(b[i:i+8]) != 0 {
				continue
			}
			binary.BigEndian.PutUint32(b[i:i+4], offset)
			binary.BigEndian.PutUint32(b[i+4:i+8], uint32(start))

			i++
		}
	}

	return nil
}

func (st *skipTable) sync() error {
	for _, b := range st.blocks {
		if err := b.Flush(); err != nil {
			return err
		}
	}
	for _, f := range st.files {
		if err := f.Sync(); err != nil {
			return err
		}
	}
	return nil

}

func (st *skipTable) close() error {
	if err := st.sync(); err != nil {
		return err
	}
	for _, b := range st.blocks {
		if err := b.Unmap(); err != nil {
			return err
		}
	}
	for _, f := range st.files {
		if err := f.Close(); err != nil {
			return err
		}
	}
	return nil
}
