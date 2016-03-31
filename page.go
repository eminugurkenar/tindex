package tindex

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const pageSize = 2048

var errPageFull = errors.New("page full")

type pageCursor interface {
	Iterator
	append(v uint64) error
}

type page interface {
	cursor() pageCursor
	init(v uint64) error
	data() []byte
}

type pageDelta struct {
	b []byte
}

func newPageDelta(data []byte) *pageDelta {
	return &pageDelta{b: data}
}

func (p *pageDelta) init(v uint64) error {
	// Write first value.
	binary.PutUvarint(p.b, v)
	return nil
}

func (p *pageDelta) cursor() pageCursor {
	return &pageDeltaCursor{data: p.b}
}

func (p *pageDelta) data() []byte {
	return p.b
}

type pageDeltaCursor struct {
	data []byte
	pos  int
	cur  uint64
}

func (p *pageDeltaCursor) append(id uint64) error {
	// Run to the end.
	_, err := p.Next()
	for ; err == nil; _, err = p.Next() {
		// Consume.
	}
	if err != io.EOF {
		return err
	}
	if len(p.data)-p.pos < binary.MaxVarintLen64 {
		return errPageFull
	}
	if p.cur >= id {
		fmt.Println("outoforder", p.cur, id)
		return errOutOfOrder
	}
	p.pos += binary.PutUvarint(p.data[p.pos:], id-p.cur)
	p.cur = id

	return nil
}

func (p *pageDeltaCursor) Close() error {
	return nil
}

func (p *pageDeltaCursor) Seek(min uint64) (v uint64, err error) {
	if min < p.cur {
		p.pos = 0
	}
	for v, err = p.Next(); err == nil && v < min; v, err = p.Next() {
		// Consume.
	}
	return p.cur, err
}

func (p *pageDeltaCursor) Next() (uint64, error) {
	var n int
	if p.pos == 0 {
		p.cur, n = binary.Uvarint(p.data)
	} else {
		var dv uint64
		dv, n = binary.Uvarint(p.data[p.pos:])
		if n <= 0 || dv == 0 {
			return 0, io.EOF
		}
		p.cur += dv
	}
	p.pos += n

	return p.cur, nil
}
