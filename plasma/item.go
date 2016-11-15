package plasma

import (
	"bytes"
	"github.com/t3rm1n4l/nitro/skiplist"
	"reflect"
	"unsafe"
)

// TODO: Cleanup the current ugly-hackup implementation

// Layout for the item is as follows:
// [    32 bit header       ][opt 32 bit keylen][key][64 bit sn][opt val]
// [insert bit][val bit][len]

const (
	itmInsertMask = 0x80000000
	itmHasValMask = 0x40000000
	itmLenMask    = 0x3fffffff
	itmHdrLen     = 4
	itmSnSize     = 8
	itmKlenSize   = 4
)

// A placeholder type for holding item data
type item uint32

func (itm *item) Size() int {
	return itm.l() + itmHdrLen
}

func (itm *item) IsInsert() bool {
	return itmInsertMask&*itm > 0

}

func (itm *item) HasValue() bool {
	return itmHasValMask&*itm > 0
}

func (itm *item) Sn() uint64 {
	kptr, klen := itm.k()
	return *(*uint64)(unsafe.Pointer(kptr + uintptr(klen)))
}

func (itm *item) l() int {
	return int(itmLenMask & *itm)
}

func (itm *item) k() (uintptr, int) {
	basePtr := uintptr(unsafe.Pointer(itm)) + itmHdrLen
	var klen int
	var kptr uintptr

	if itm.HasValue() {
		klen = int(*(*uint32)(unsafe.Pointer(basePtr)))
		kptr = basePtr + itmKlenSize
	} else {
		klen = itm.l()
		kptr = basePtr
	}

	return kptr, klen
}

func (itm *item) Key() (bs []byte) {
	kptr, klen := itm.k()
	sh := (*reflect.SliceHeader)(unsafe.Pointer(&bs))
	sh.Data = kptr
	sh.Len = klen
	sh.Cap = klen
	return
}

func (itm *item) Value() (bs []byte) {
	kptr, klen := itm.k()
	l := itm.l()

	sh := (*reflect.SliceHeader)(unsafe.Pointer(&bs))
	sh.Data = kptr + uintptr(klen) + itmSnSize
	sh.Len = l - klen
	sh.Cap = sh.Len
	return
}

func (s *Plasma) newItem(k, v []byte, sn uint64, del bool) *item {
	kl := len(k)
	vl := len(v)

	sz := uintptr(itmHdrLen + itmSnSize + kl + vl)
	if vl > 0 {
		sz += itmKlenSize
	}

	ptr := s.alloc(sz)
	hdr := (*uint32)(ptr)
	if !del {
		*hdr |= itmInsertMask
	}

	if vl > 0 {
		*hdr |= itmHasValMask | uint32(vl+kl+itmKlenSize)
		klen := (*uint32)(unsafe.Pointer(uintptr(ptr) + itmHdrLen))
		*klen = uint32(kl)

		snp := (*uint64)(unsafe.Pointer(uintptr(ptr) + uintptr(itmHdrLen+itmKlenSize+kl)))
		*snp = sn
		memcopy(unsafe.Pointer(uintptr(ptr)+itmHdrLen+itmKlenSize), unsafe.Pointer(&k[0]), kl)
		memcopy(unsafe.Pointer(uintptr(ptr)+itmHdrLen+itmKlenSize+itmSnSize+uintptr(kl)), unsafe.Pointer(&v[0]), vl)
	} else {
		snp := (*uint64)(unsafe.Pointer(uintptr(ptr) + uintptr(itmHdrLen+kl)))
		*snp = sn
		*hdr |= uint32(kl)
		memcopy(unsafe.Pointer(uintptr(ptr)+itmHdrLen), unsafe.Pointer(&k[0]), kl)
	}

	return (*item)(ptr)
}

func cmpItem(a, b unsafe.Pointer) int {
	if a == skiplist.MinItem || b == skiplist.MaxItem {
		return -1
	}

	if a == skiplist.MaxItem || b == skiplist.MinItem {
		return 1
	}

	itma := (*item)(a)
	itmb := (*item)(b)

	cv := bytes.Compare(itma.Key(), itmb.Key())
	if cv == 0 {
		return int(itmb.Sn()) - int(itma.Sn())
	}

	return cv
}
