package plasma

import (
	"bytes"
	"fmt"
	"github.com/couchbase/nitro/skiplist"
	"reflect"
	"unsafe"
)

// TODO: Cleanup the current ugly-hackup implementation

// Layout for the item is as follows:
// [    32 bit header       ][opt 32 bit keylen][key][opt 64 bit sn][opt val]
// [insert bit][val bit][sn bit][len]

const (
	itmInsertFlag = 0x80000000
	itmHasValFlag = 0x40000000
	itmSnFlag     = 0x20000000
	itmLenMask    = 0x1fffffff
	itmHdrLen     = 4
	itmSnSize     = 8
	itmKlenSize   = 4
)

// A placeholder type for holding item data
type item uint32

func (itm *item) Size() int {
	var snSize int
	if *itm&itmSnFlag > 0 {
		snSize = itmSnSize
	}
	return itm.l() + itmHdrLen + snSize
}

func (itm *item) IsInsert() bool {
	return itmInsertFlag&*itm > 0

}

func (itm *item) Item() unsafe.Pointer {
	return unsafe.Pointer(itm)
}

func (itm *item) Len() int {
	return 1
}

func (itm *item) At(int) PageItem {
	return itm
}

func (itm *item) HasValue() bool {
	return itmHasValFlag&*itm > 0
}

func (itm *item) Sn() uint64 {
	if *itm&itmSnFlag > 0 {
		kptr, klen := itm.k()
		return *(*uint64)(unsafe.Pointer(kptr + uintptr(klen)))
	}
	return 0
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

	var snSize int
	if *itm&itmSnFlag > 0 {
		snSize = itmSnSize
	}

	sh := (*reflect.SliceHeader)(unsafe.Pointer(&bs))
	sh.Data = kptr + uintptr(klen+snSize)
	sh.Len = l - klen - itmKlenSize
	sh.Cap = sh.Len
	return
}

func (s *Plasma) newItem(k, v []byte, sn uint64, del bool, buf *Buffer) (
	*item, error) {
	if len(k) > itmLenMask {
		return nil, ErrKeyTooLarge
	}

	kl := len(k)
	vl := len(v)

	var snSize int
	if sn > 1 {
		snSize = itmSnSize
	}

	sz := uintptr(itmHdrLen + snSize + kl + vl)
	if vl > 0 {
		sz += itmKlenSize
	}

	var ptr unsafe.Pointer
	if buf == nil {
		ptr = s.alloc(sz)
	} else {
		buf.Grow(0, int(sz))
		ptr = buf.Ptr(0)
	}

	hdr := (*uint32)(ptr)
	*hdr = 0
	if !del {
		*hdr |= itmInsertFlag
	}

	if sn > 1 {
		*hdr |= itmSnFlag
	}

	if vl > 0 {
		*hdr |= itmHasValFlag | uint32(vl+kl+itmKlenSize)
		klen := (*uint32)(unsafe.Pointer(uintptr(ptr) + itmHdrLen))
		*klen = uint32(kl)

		if sn > 1 {
			snp := (*uint64)(unsafe.Pointer(uintptr(ptr) + uintptr(itmHdrLen+itmKlenSize+kl)))
			*snp = sn
		}
		memcopy(unsafe.Pointer(uintptr(ptr)+itmHdrLen+itmKlenSize), unsafe.Pointer(&k[0]), kl)
		memcopy(unsafe.Pointer(uintptr(ptr)+itmHdrLen+itmKlenSize+uintptr(snSize)+uintptr(kl)), unsafe.Pointer(&v[0]), vl)
	} else {
		if sn > 1 {
			snp := (*uint64)(unsafe.Pointer(uintptr(ptr) + uintptr(itmHdrLen+kl)))
			*snp = sn
		}
		*hdr |= uint32(kl)
		memcopy(unsafe.Pointer(uintptr(ptr)+itmHdrLen), unsafe.Pointer(&k[0]), kl)
	}

	return (*item)(ptr), nil
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

	return bytes.Compare(itma.Key(), itmb.Key())
}

func itemStringer(itm unsafe.Pointer) string {
	if itm == skiplist.MinItem {
		return "minItem"
	} else if itm == skiplist.MaxItem {
		return "maxItem"
	}

	x := (*item)(itm)
	v := "(nil)"
	if x.HasValue() {
		v = string(x.Value())
	}
	return fmt.Sprintf("item key:%s val:%s sn:%d insert: %v", string(x.Key()), v, x.Sn(), x.IsInsert())
}
