package tindex

import (
	"encoding/binary"
	"errors"
)

var errPageFull = errors.New("page full")

type pageDeltaCursor struct {
	data []byte

	pos, off int
	cur      docid
	fresh    bool
}

func newPageDeltaCursor(data []byte, offset int) *pageDeltaCursor {
	return &pageDeltaCursor{
		data:  data,
		off:   offset,
		pos:   offset,
		fresh: true,
	}
}

var errPageFull = errors.New("page full")

func (p *pageDeltaCursor) append(id docid) error {
	_, err := p.seek(0)
	for ; err == nil; _, err = p.next() {
		// Consume.
	}
	if err != io.EOF {
		return err
	}
	if p.size()-p.pos < binary.MaxVarintLen64 {
		return errPageFull
	}
	n := binary.PutUvarint(p.data[p.pos:], uint64(id))
	p.pos += n

	return nil
}

func (p *pageDeltaCursor) seek(min docid) (docid, error) {
	p.pos = p.off
	v, err := p.next()
	for ; err == nil && v < min; v, err = p.next() {
		// Consume.
	}
	return p.cur, err
}

func (p *pageDeltaCursor) next() (docid, error) {
	var n int
	if p.fresh {
		id, n := binary.Uvarint(p.data[p.pos:])
		if n <= 0 {
			return 0, io.EOF
		}
		p.cur = docid(id)
		p.fresh = false

	} else {
		var dv int64
		dv, n = binary.Varint(p.data[p.pos:])
		if n <= 0 || dv == 0 {
			return 0, io.EOF
		}
		p.cur = docid(int64(p.cur) + dv)
	}
	p.pos += n

	return p.cur, nil
}
