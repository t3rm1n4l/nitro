package plasma

import (
	"github.com/couchbase/nitro/mm"
	"reflect"
	"unsafe"
)

var (
	metaDeltaSize       = unsafe.Sizeof(*new(metaPageDelta))
	recDeltaSize        = unsafe.Sizeof(*new(recordDelta))
	basePageSize        = unsafe.Sizeof(*new(basePage))
	splitPageDeltaSize  = unsafe.Sizeof(*new(splitPageDelta))
	mergePageDeltaSize  = unsafe.Sizeof(*new(mergePageDelta))
	flushPageDeltaSize  = unsafe.Sizeof(*new(flushPageDelta))
	removePageDeltaSize = unsafe.Sizeof(*new(removePageDelta))
	rollbackDeltaSize   = unsafe.Sizeof(*new(rollbackDelta))
	swapoutDeltaSize    = unsafe.Sizeof(*new(swapoutDelta))
	swapinDeltaSize     = unsafe.Sizeof(*new(swapinDelta))
)

type pgFreeObj struct {
	h       *pageDelta
	evicted bool
}

type allocCtx struct {
	allocDeltaList []*pageDelta
	freePageList   []pgFreeObj
	memUsed        int
	nrecAllocs     int
	nrecSwapin     int
}

func (aCtx *allocCtx) GetAllocOps() ([]*pageDelta, []pgFreeObj, int, int, int) {
	a := aCtx.allocDeltaList
	f := aCtx.freePageList
	m := aCtx.memUsed
	nra := aCtx.nrecAllocs
	nrs := aCtx.nrecSwapin

	aCtx.memUsed = 0
	aCtx.nrecAllocs = 0
	aCtx.nrecSwapin = 0
	aCtx.allocDeltaList = aCtx.allocDeltaList[:0]
	aCtx.freePageList = aCtx.freePageList[:0]
	return a, f, nra, nrs, m
}

func (ctx *allocCtx) addDeltaAlloc(ptr unsafe.Pointer) {
	ctx.allocDeltaList = append(ctx.allocDeltaList, (*pageDelta)(ptr))
}

func (pg *page) free(evicted bool) {
	if pg.head != nil {
		pg.freePageList = append(pg.freePageList, pgFreeObj{h: pg.head, evicted: evicted})
	}
}

func (s *storeCtx) destroyPg(ptr *pageDelta) {
	if s.useMemMgmt {
		for pd := ptr; pd != nil; {
			next := pd.next
			if pd.op == opBasePage || pd.op == opSwapoutDelta {
				next = nil
			} else if pd.op == opPageMergeDelta {
				pdm := (*mergePageDelta)(unsafe.Pointer(pd))
				s.destroyPg(pdm.mergeSibling)
			} else if pd.op == opSwapinDelta {
				sid := (*swapinDelta)(unsafe.Pointer(pd))
				s.destroyPg(sid.ptr)
			}

			s.freeMM(unsafe.Pointer(pd))
			pd = next
		}
	}
}

func (pg *page) allocMetaDelta(hiItm unsafe.Pointer) *metaPageDelta {
	l := pg.itemSize(hiItm)
	size := metaDeltaSize + l

	if pg.useMemMgmt {
		ptr := pg.allocMM(size)
		pg.memUsed += mm.SizeAt(ptr)
		d := (*metaPageDelta)(ptr)
		if l == 0 {
			d.hiItm = hiItm
		} else {
			d.hiItm = unsafe.Pointer(uintptr(ptr) + metaDeltaSize)
			memcopy(d.hiItm, hiItm, int(l))
		}
		pg.addDeltaAlloc(ptr)
		return d
	}

	pg.memUsed += int(size)
	return &metaPageDelta{hiItm: pg.dup(hiItm)}
}

func (pg *page) allocRecordDelta(itm unsafe.Pointer) *recordDelta {
	l := pg.itemSize(itm)
	size := recDeltaSize + l
	pg.nrecAllocs++

	if pg.useMemMgmt {
		ptr := pg.allocMM(size)
		pg.memUsed += mm.SizeAt(ptr)
		d := (*recordDelta)(ptr)
		if l == 0 {
			d.itm = itm
		} else {
			d.itm = unsafe.Pointer(uintptr(ptr) + recDeltaSize)
			memcopy(d.itm, itm, int(l))
		}
		pg.addDeltaAlloc(ptr)
		return d
	}

	pg.memUsed += int(size)
	d := new(recordDelta)
	d.itm = pg.dup(itm)
	return d
}

func (pg *page) allocBasePage(n int, dataSz uintptr, hiItm unsafe.Pointer) *basePage {
	l := pg.itemSize(hiItm)
	size := basePageSize + dataSz + uintptr(n)*8 + l
	pg.nrecAllocs += n

	if pg.useMemMgmt {
		ptr := pg.allocMM(size)
		pg.memUsed += mm.SizeAt(ptr)
		bp := (*basePage)(ptr)
		sh := (*reflect.SliceHeader)(unsafe.Pointer(&bp.items))
		sh.Data = uintptr(ptr) + basePageSize
		sh.Len = n
		sh.Cap = n
		bp.data = unsafe.Pointer(uintptr(ptr) + basePageSize + uintptr(n)*8)
		if l == 0 {
			bp.hiItm = hiItm
		} else {
			bp.hiItm = unsafe.Pointer(uintptr(ptr) + basePageSize + uintptr(n)*8 + dataSz)
			memcopy(bp.hiItm, hiItm, int(l))
		}
		pg.addDeltaAlloc(ptr)
		return bp
	}

	pg.memUsed += int(size)
	bp := new(basePage)
	bp.items = make([]unsafe.Pointer, n)
	bp.data = pg.alloc(dataSz)
	bp.hiItm = pg.dup(hiItm)
	return bp
}

func (pg *page) allocSplitPageDelta(hiItm unsafe.Pointer) *splitPageDelta {
	l := pg.itemSize(hiItm)
	size := splitPageDeltaSize + l

	if pg.useMemMgmt {
		ptr := pg.allocMM(size)
		pg.memUsed += mm.SizeAt(ptr)
		d := (*splitPageDelta)(ptr)
		if l == 0 {
			d.hiItm = hiItm
		} else {
			d.hiItm = unsafe.Pointer(uintptr(ptr) + splitPageDeltaSize)
			memcopy(d.hiItm, hiItm, int(l))
		}
		pg.addDeltaAlloc(ptr)
		return d
	}

	pg.memUsed += int(size)
	d := new(splitPageDelta)
	d.hiItm = pg.dup(hiItm)
	return d
}

func (pg *page) allocMergePageDelta(hiItm unsafe.Pointer) *mergePageDelta {
	l := pg.itemSize(hiItm)
	size := mergePageDeltaSize + l

	if pg.useMemMgmt {
		ptr := pg.allocMM(size)
		pg.memUsed += mm.SizeAt(ptr)
		d := (*mergePageDelta)(ptr)
		if l == 0 {
			d.hiItm = hiItm
		} else {
			d.hiItm = unsafe.Pointer(uintptr(ptr) + mergePageDeltaSize)
			memcopy(d.hiItm, hiItm, int(l))
		}
		pg.addDeltaAlloc(ptr)
		return d
	}

	pg.memUsed += int(size)
	d := new(mergePageDelta)
	d.hiItm = pg.dup(hiItm)
	return d
}

func (pg *page) allocFlushPageDelta() *flushPageDelta {
	if pg.useMemMgmt {
		ptr := pg.allocMM(flushPageDeltaSize)
		pg.memUsed += mm.SizeAt(ptr)
		pg.addDeltaAlloc(ptr)
		return (*flushPageDelta)(ptr)
	}

	pg.memUsed += int(flushPageDeltaSize)
	return new(flushPageDelta)
}

func (pg *page) allocRemovePageDelta() *removePageDelta {
	if pg.useMemMgmt {
		ptr := pg.allocMM(removePageDeltaSize)
		pg.memUsed += mm.SizeAt(ptr)
		pg.addDeltaAlloc(ptr)
		return (*removePageDelta)(ptr)
	}

	pg.memUsed += int(removePageDeltaSize)
	return new(removePageDelta)
}

func (pg *page) allocRollbackPageDelta() *rollbackDelta {
	if pg.useMemMgmt {
		ptr := pg.allocMM(rollbackDeltaSize)
		pg.memUsed += mm.SizeAt(ptr)
		pg.addDeltaAlloc(ptr)
		return (*rollbackDelta)(ptr)
	}

	pg.memUsed += int(rollbackDeltaSize)
	return new(rollbackDelta)
}

func (pg *page) allocSwapoutDelta(hiItm unsafe.Pointer) *swapoutDelta {
	l := pg.itemSize(hiItm)
	size := swapoutDeltaSize + l
	if pg.useMemMgmt {
		ptr := pg.allocMM(size)
		pg.memUsed += mm.SizeAt(ptr)
		d := (*swapoutDelta)(ptr)
		if l == 0 {
			d.hiItm = hiItm
		} else {
			d.hiItm = unsafe.Pointer(uintptr(ptr) + mergePageDeltaSize)
			memcopy(d.hiItm, hiItm, int(l))
		}
		pg.addDeltaAlloc(ptr)
		return (*swapoutDelta)(ptr)
	}

	pg.memUsed += int(size)
	d := new(swapoutDelta)
	d.hiItm = pg.dup(hiItm)
	return d
}

func (pg *page) allocSwapinDelta() *swapinDelta {
	size := swapoutDeltaSize

	if pg.useMemMgmt {
		ptr := pg.allocMM(size)
		pg.memUsed += mm.SizeAt(ptr)
		pg.addDeltaAlloc(ptr)
		return (*swapinDelta)(ptr)
	}

	pg.memUsed += int(size)
	d := new(swapinDelta)
	return d
}
