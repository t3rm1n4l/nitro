package plasma

import (
	"encoding/binary"
	"fmt"
	"github.com/couchbase/nitro/mm"
	"github.com/couchbase/nitro/skiplist"
	"sort"
	"unsafe"
)

type pageOp uint16

const (
	opBasePage pageOp = iota + 1

	opMetaDelta

	opInsertDelta
	opDeleteDelta

	opPageSplitDelta
	opPageRemoveDelta
	opPageMergeDelta

	opFlushPageDelta
	opRelocPageDelta

	opRollbackDelta

	opSwapoutDelta
	opSwapinDelta
)

const (
	FullMarshal   = 0
	NoFullMarshal = ^0
)

const (
	minKeyEncoded byte = iota + 1
	maxKeyEncoded
	itemKeyEncoded
)

var pageHeaderSize = int(unsafe.Sizeof(*new(pageDelta)))

type PageId interface{}

// TODO: Identify corner cases
func NextPid(pid PageId) PageId {
	return PageId(pid.(*skiplist.Node).GetNext())
}

type Page interface {
	Insert(itm unsafe.Pointer)
	Delete(itm unsafe.Pointer)
	Lookup(itm unsafe.Pointer) unsafe.Pointer
	NewIterator() ItemIterator

	InRange(itm unsafe.Pointer) bool

	NeedCompaction(int) bool
	NeedMerge(int) bool
	NeedSplit(int) bool
	NeedRemoval() bool

	Close()
	Split(PageId) Page
	Merge(Page)
	Compact() (fdSize int)
	Rollback(s, end uint64)

	Append(Page)
	Marshal(b *Buffer, maxSegments int) (bs []byte, fdSz int, staleFdSz int, numSegments int)

	GetVersion() uint16
	IsFlushed() bool
	NeedsFlush() bool
	IsEvictable() bool
	InCache() bool
	MaxItem() unsafe.Pointer
	MinItem() unsafe.Pointer
	SetNext(PageId)
	Next() PageId

	Evict(offset LSSOffset, numSegments int)
	SwapIn(ptr *pageDelta)

	GetAllocOps() (a []*pageDelta, f []pgFreeObj, nra int, nrs int, sz int)
	GetFlushDataSize() int
	ComputeMemUsed() int
	AddFlushRecord(off LSSOffset, dataSz int, numSegments int)

	// TODO: Clean up later
	IsEmpty() bool
	GetFlushInfo() (LSSOffset, int, int)
	SetNumSegments(int)
}

type ItemIterator interface {
	SeekFirst() error
	Seek(unsafe.Pointer) error
	Get() unsafe.Pointer
	Valid() bool
	Next() error
}

type PageItemsList interface {
	Len() int
	At(i int) PageItem
}

var nilPageItemsList = (*pageItemsList)(&[]PageItem{})

type PageItem interface {
	IsInsert() bool
	Item() unsafe.Pointer

	PageItemsList
}

type pageItem struct {
	itm unsafe.Pointer
}

func (pi *pageItem) IsInsert() bool {
	return true
}

func (pi *pageItem) Item() unsafe.Pointer {
	return pi.itm
}

func (pi *pageItem) Len() int {
	return 1
}

func (pi *pageItem) At(i int) PageItem {
	return pi
}

type pageItemsList []PageItem

func (pil *pageItemsList) Len() int {
	return len(*pil)
}

func (pil *pageItemsList) At(i int) PageItem {
	return (*pil)[i]
}

// | 14 bit version | 1 bit evicted | 1 bit flushed |
type pageState uint16

func (ps *pageState) GetVersion() uint16 {
	return uint16(*ps & 0x3fff)
}

func (ps *pageState) IsFlushed() bool {
	return *ps&0x8000 > 0
}

func (ps *pageState) SetFlushed() {
	*ps |= 0x8000
}

func (ps *pageState) IsEvicted() bool {
	return *ps&0x4000 > 0
}

func (ps *pageState) SetEvicted(v bool) {
	if v {
		*ps |= 0x4000
	} else {
		*ps &= 0xbfff
	}
}

func (ps *pageState) IncrVersion() {
	v := uint16(*ps & 0x3fff)
	*ps = pageState((v + 1) & 0x3fff)
}

type metaPageDelta pageDelta

type pageDelta struct {
	op       pageOp
	chainLen uint16
	numItems uint16
	state    pageState

	next *pageDelta

	hiItm        unsafe.Pointer
	rightSibling PageId
}

func (pd *pageDelta) IsInsert() bool {
	return pd.op == opInsertDelta
}

func (pd *pageDelta) Item() unsafe.Pointer {
	return (*recordDelta)(unsafe.Pointer(pd)).itm
}

func (pd *pageDelta) Len() int {
	return 1
}

func (pd *pageDelta) At(int) PageItem {
	return pd
}

type basePage struct {
	op       pageOp
	chainLen uint16
	numItems uint16
	state    pageState

	data unsafe.Pointer

	hiItm        unsafe.Pointer
	rightSibling PageId
	items        []unsafe.Pointer
}

type recordDelta struct {
	pageDelta
	itm unsafe.Pointer
}

func (rd *recordDelta) IsInsert() bool {
	return rd.op == opInsertDelta
}

func (rd *recordDelta) Item() unsafe.Pointer {
	return rd.itm
}

type splitPageDelta struct {
	pageDelta
	itm unsafe.Pointer
}

type mergePageDelta struct {
	pageDelta
	itm          unsafe.Pointer
	mergeSibling *pageDelta
}

type flushPageDelta struct {
	pageDelta
	offset      LSSOffset
	flushDataSz int32
	numSegments int32
}

type removePageDelta pageDelta

type rollbackDelta struct {
	pageDelta
	rb rollbackSn
}

func (rpd *rollbackDelta) Filter() interface{} {
	return &rpd.rb
}

type swapoutDelta struct {
	pageDelta

	offset      LSSOffset
	numSegments int32
}

type swapinDelta struct {
	pageDelta
	ptr *pageDelta
}

type ItemSizeFn func(unsafe.Pointer) uintptr
type FilterGetter func() ItemFilter

type page struct {
	ctx *wCtx
	*storeCtx
	*allocCtx

	nextPid     PageId
	low         unsafe.Pointer
	state       pageState
	prevHeadPtr unsafe.Pointer
	head        *pageDelta
	tail        *pageDelta
}

func (pg *page) SetNext(pid PageId) {
	if pg.head == nil {
		pg.nextPid = pid
	} else {
		pg.head.rightSibling = pid
	}
}

func (pg *page) InCache() bool {
	return uintptr(unsafe.Pointer(pg.prevHeadPtr))&uintptr(evictMask) == 0
}

func (pg *page) Reset() {
	pg.nextPid = nil
	pg.low = nil
	pg.head = nil
	pg.tail = nil
	pg.prevHeadPtr = nil
}

func (pg *page) newFlushPageDelta(offset LSSOffset, dataSz int, numSegments int) *flushPageDelta {
	pd := pg.allocFlushPageDelta()

	*(*pageDelta)(unsafe.Pointer(pd)) = *pg.head
	pd.next = pg.head
	reloc := numSegments == 0
	if reloc {
		pd.op = opRelocPageDelta
		pd.state.IncrVersion()
		pd.numSegments = 1
	} else {
		pd.op = opFlushPageDelta
		pd.numSegments = int32(numSegments)
	}

	pd.offset = offset
	pd.state.SetFlushed()
	pd.flushDataSz = int32(dataSz)
	return pd
}

func (pg *page) newRecordDelta(op pageOp, itm unsafe.Pointer) *pageDelta {
	pd := pg.allocRecordDelta(itm)
	*(*pageDelta)(unsafe.Pointer(pd)) = *pg.head
	pd.next = pg.head

	pd.op = op
	pd.chainLen++
	return (*pageDelta)(unsafe.Pointer(pd))
}

func (pg *page) newSplitPageDelta(itm unsafe.Pointer, pid PageId) *pageDelta {
	pd := pg.allocSplitPageDelta(itm)
	itm = pd.hiItm
	*(*pageDelta)(unsafe.Pointer(pd)) = *pg.head
	pd.next = pg.head

	pd.op = opPageSplitDelta
	pd.itm = itm
	pd.hiItm = itm
	pd.chainLen++
	pd.rightSibling = pid
	return (*pageDelta)(unsafe.Pointer(pd))
}

func (pg *page) newMergePageDelta(itm unsafe.Pointer, sibl *pageDelta) *pageDelta {
	pd := pg.allocMergePageDelta(sibl.hiItm)
	hiItm := pd.hiItm
	*(*pageDelta)(unsafe.Pointer(pd)) = *pg.head
	pd.next = pg.head

	pd.op = opPageMergeDelta
	pd.mergeSibling = sibl
	pd.itm = itm
	pd.hiItm = hiItm
	pd.chainLen += sibl.chainLen + 1
	pd.numItems += sibl.numItems
	pd.rightSibling = sibl.rightSibling
	return (*pageDelta)(unsafe.Pointer(pd))
}

func (pg *page) newRemovePageDelta() *pageDelta {
	pd := pg.allocRemovePageDelta()
	*(*pageDelta)(unsafe.Pointer(pd)) = *pg.head
	pd.next = pg.head

	pd.op = opPageRemoveDelta
	return (*pageDelta)(unsafe.Pointer(pd))
}

func (pg *page) newBasePage(itms []unsafe.Pointer) *pageDelta {
	var sz uintptr
	var hiItm unsafe.Pointer

	if pg.head != nil {
		hiItm = pg.head.hiItm
	}

	n := len(itms)
	for _, itm := range itms {
		sz += pg.itemSize(itm)
	}

	bp := pg.allocBasePage(n, sz, hiItm)
	bp.op = opBasePage
	bp.numItems = uint16(n)
	bp.state = 0

	var offset uintptr
	for i, itm := range itms {
		itmsz := pg.itemSize(itm)
		dstItm := unsafe.Pointer(uintptr(bp.data) + offset)
		memcopy(dstItm, itm, int(itmsz))
		bp.items[i] = dstItm
		offset += itmsz
	}

	bp.numItems = uint16(n)
	if pg.head != nil {
		bp.rightSibling = pg.head.rightSibling
	}

	return (*pageDelta)(unsafe.Pointer(bp))
}

// TODO: Fix the low bound check ?
func (pg *page) InRange(itm unsafe.Pointer) bool {
	if pg.cmp(itm, pg.head.hiItm) >= 0 {
		return false
	}

	return true
}

func (pg *page) Insert(itm unsafe.Pointer) {
	pg.head = pg.newRecordDelta(opInsertDelta, itm)
}

func (pg *page) Delete(itm unsafe.Pointer) {
	pg.head = pg.newRecordDelta(opDeleteDelta, itm)
}

func (pg *page) equal(itm0, itm1, hi unsafe.Pointer) bool {
	return pg.cmp(itm0, itm1) == 0 && pg.cmp(itm0, hi) < 0
}

func (pg *page) Lookup(itm unsafe.Pointer) unsafe.Pointer {
	hiItm := pg.MaxItem()
	filter := pg.getLookupFilter()
	head := pg.head
	itmBuf := pg.ctx.GetBuffer(bufTempItem)

loop:
	pw := newPgDeltaWalker(head, pg.ctx)
	defer pw.Close()

	for ; !pw.End(); pw.Next() {
		op := pw.Op()
		switch op {
		case opInsertDelta:
			ritm := pw.Item()
			pgItm := pw.PageItem()
			if filter.Process(pgItm).Len() > 0 && pg.equal(ritm, itm, hiItm) {
				l := int(pg.itemSize(ritm))
				itmBuf.Grow(0, l)
				resultPtr := itmBuf.Ptr(0)
				memcopy(resultPtr, ritm, l)
				return resultPtr
			}
		case opDeleteDelta:
			ritm := pw.Item()
			pgItm := pw.PageItem()
			if filter.Process(pgItm).Len() > 0 && pg.equal(ritm, itm, hiItm) {
				return nil
			}
		case opBasePage:
			items := pw.BaseItems()
			n := len(items)
			index := sort.Search(n, func(i int) bool {
				return pg.cmp(items[i], itm) >= 0
			})

			for ; index < n && pg.equal(items[index], itm, hiItm); index++ {
				bpItm := (*basePageItem)(items[index])
				if filter.Process(bpItm).Len() > 0 {
					ritm := items[index]
					l := int(pg.itemSize(ritm))
					itmBuf.Grow(0, l)
					resultPtr := itmBuf.Ptr(0)
					memcopy(resultPtr, ritm, l)
					return resultPtr
				}
			}

			return nil
		case opPageSplitDelta:
			sitm := pw.Item()
			if pg.cmp(sitm, hiItm) < 0 {
				hiItm = sitm
			}
		case opPageMergeDelta:
			if pg.cmp(itm, pw.Item()) >= 0 {
				head = pw.MergeSibling()
				goto loop
			}

		case opRollbackDelta:
			filter.AddFilter(pw.RollbackFilter())

		case opFlushPageDelta:
		case opRelocPageDelta:
		case opPageRemoveDelta:
		case opSwapinDelta:
		case opMetaDelta:
		case opSwapoutDelta:
		default:
			panic(fmt.Sprint("should not happen op:", op))
		}
	}

	return nil
}

func (pg *page) NeedCompaction(threshold int) bool {
	return int(pg.head.chainLen) > threshold
}

func (pg *page) NeedSplit(threshold int) bool {
	return int(pg.head.numItems) > threshold
}

func (pg *page) NeedMerge(threshold int) bool {
	return int(pg.head.numItems) < threshold
}

func (pg *page) NeedRemoval() bool {
	return pg.head.op == opPageRemoveDelta
}

func (pg *page) Close() {
	pg.head = pg.newRemovePageDelta()
}

func (pg *page) Split(pid PageId) Page {
	var items []unsafe.Pointer
	pw := newPgDeltaWalker(pg.head, pg.ctx)
	defer pw.Close()
	for ; !pw.End(); pw.Next() {
		if pw.Op() == opBasePage {
			items = pw.BaseItems()
			break
		}
	}

	var mid int
	if len(items) > 0 {
		mid = len(items) / 2
		for mid > 0 {
			// Make sure that split is performed by different key boundary
			if pg.cmp(items[mid], pg.head.hiItm) < 0 {
				if mid-1 >= 0 && pg.cmp(items[mid], items[mid-1]) > 0 {
					break
				}
			}
			mid--
		}
	}

	if mid > 0 {
		numItems := len(items[:mid])
		if pgi := pg.doSplit(items[mid], pid, numItems); pgi != nil {
			return pgi
		}
	}

	return nil
}

func (pg *page) doSplit(itm unsafe.Pointer, pid PageId, numItems int) *page {
	splitPage := new(page)
	*splitPage = *pg
	splitPage.prevHeadPtr = nil
	it, itms, _, _ := pg.collectItems(pg.head, itm, pg.head.hiItm)
	defer it.Close()
	if len(itms) == 0 {
		return nil
	}
	bp := pg.newBasePage(itms)
	splitPage.head = bp

	itm = (*basePage)(unsafe.Pointer(bp)).items[0]
	splitPage.low = itm
	pg.head = pg.newSplitPageDelta(itm, pid)

	if numItems >= 0 {
		pg.head.numItems = uint16(numItems)
	} else {
		// During recovery
		pg.head.numItems /= 2
	}
	return splitPage
}

func (pg *page) Compact() int {
	state := pg.head.state

	it, itms, fdataSz, numLSSRecs := pg.collectItems(pg.head, nil, pg.head.hiItm)
	pg.free(false)
	pg.nrecSwapin += numLSSRecs
	pg.head = pg.newBasePage(itms)
	it.Close()
	state.IncrVersion()
	pg.head.state = state
	return fdataSz
}

func (pg *page) Merge(sp Page) {
	siblPage := (sp.(*page)).head
	pdm := pg.newMergePageDelta(pg.head.hiItm, siblPage)
	pdm.next = pg.head
	pg.head = pdm
}

func (pg *page) Append(p Page) {
	aPg := p.(*page)
	if pg.tail == nil {
		*pg = *aPg
	} else {
		pg.tail.next = aPg.head
		pg.tail = aPg.tail
	}
}

func (pg *page) inRange(lo, hi unsafe.Pointer, itm unsafe.Pointer) bool {
	return pg.cmp(itm, hi) < 0 && pg.cmp(itm, lo) >= 0
}

func prettyPrint(head *pageDelta, stringify func(unsafe.Pointer) string) {
	pw := newPgDeltaWalker(head, nil)
	defer pw.Close()
loop:
	for ; !pw.End(); pw.Next() {
		op := pw.Op()
		switch op {
		case opInsertDelta, opDeleteDelta:
			fmt.Printf("Delta op:%d, itm:%s\n", op, stringify(pw.Item()))
		case opBasePage:
			for _, itm := range pw.BaseItems() {
				fmt.Printf("Basepage itm:%s\n", stringify(itm))
			}
			break loop
		case opFlushPageDelta:
			offset, _, _ := pw.FlushInfo()
			fmt.Printf("-------flush------ max:%s, offset:%d\n", stringify(pw.HighItem()), offset)
		case opPageSplitDelta:
			fmt.Println("-------split------ ", stringify(pw.Item()))
		case opPageMergeDelta:
			fmt.Println("-------merge-siblings------- ", stringify(pw.Item()))
			prettyPrint(pw.MergeSibling(), stringify)
			fmt.Println("-----------")
		case opPageRemoveDelta:
			fmt.Println("---remove-delta---")
		case opRollbackDelta:
			start, end := pw.RollbackInfo()
			fmt.Println("-----rollback----", start, end)
		}
	}
}

func (pg *page) collectItems(head *pageDelta,
	loItm, hiItm unsafe.Pointer) (itr pgOpIterator, itms []unsafe.Pointer, dataSz int, numLSSRecs int) {

	var sts pgOpIteratorStats
	it := newPgOpIterator(pg.head, pg.cmp, loItm, hiItm, pg.getCompactFilter(), pg.ctx, &sts)
	for it.Init(); it.Valid(); it.Next() {
		itm := it.Get()
		itms = append(itms, itm.Item())
	}

	return it, itms, sts.fdSz, sts.numLSSRecords
}

type pageIterator struct {
	it   pgOpIterator
	pg   *page
	itms []unsafe.Pointer
	i    int
}

func (pi *pageIterator) Get() unsafe.Pointer {
	return pi.itms[pi.i]
}

func (pi *pageIterator) Valid() bool {
	return pi.i < len(pi.itms)
}

func (pi *pageIterator) Next() error {
	pi.i++
	return nil
}

func (pi *pageIterator) SeekFirst() error {
	pi.it, pi.itms, _, _ = pi.pg.collectItems(pi.pg.head, nil, pi.pg.head.hiItm)
	return nil
}

func (pi *pageIterator) Seek(itm unsafe.Pointer) error {
	pi.it, pi.itms, _, _ = pi.pg.collectItems(pi.pg.head, itm, pi.pg.head.hiItm)
	return nil

}

func (pi *pageIterator) Close() {
	pi.it.Close()
}

// This method is only used by page_tests
// TODO: Cleanup implementation by using pageOpIterator
func (pg *page) NewIterator() ItemIterator {
	return &pageIterator{
		pg: pg,
	}
}

func (pg *page) Marshal(buf *Buffer, maxSegments int) (bs []byte, dataSz, staleFdSz int, numSegments int) {
	hiItm := pg.MaxItem()
	offset, staleFdSz, numSegments := pg.marshal(buf, 0, pg.head, hiItm, false, maxSegments)
	return buf.Get(0, offset), offset, staleFdSz, numSegments
}

func (pg *page) unmarshalIndexKey(data []byte, roffset int) (unsafe.Pointer, int) {
	flag := data[roffset]
	roffset += 1
	switch flag {
	case itemKeyEncoded:
		itm := unsafe.Pointer(&data[roffset])
		roffset += int(pg.itemSize(itm))
		return itm, roffset
	case minKeyEncoded:
		return skiplist.MinItem, roffset
	case maxKeyEncoded:
		return skiplist.MaxItem, roffset
	}

	panic(fmt.Sprintf("invalid flag %d", flag))
}

func (pg *page) unmarshalItem(data []byte, roffset int) (unsafe.Pointer, int) {
	itm := unsafe.Pointer(&data[roffset])
	l := pg.itemSize(itm)
	roffset += int(l)
	return itm, roffset

}

func (pg *page) marshalIndexKey(key unsafe.Pointer, woffset int, buf *Buffer) int {
	flag := buf.Get(woffset, 1)
	woffset += 1
	if key == skiplist.MinItem {
		flag[0] = minKeyEncoded

	} else if key == skiplist.MaxItem {
		flag[0] = maxKeyEncoded
	} else {
		flag[0] = itemKeyEncoded
		return pg.marshalItem(key, woffset, buf)
	}

	return woffset
}

func (pg *page) marshalItem(itm unsafe.Pointer, woffset int, b *Buffer) int {
	l := int(pg.itemSize(itm))
	b.Grow(0, woffset+l)
	memcopy(b.Ptr(woffset), itm, l)
	woffset += l

	return woffset
}

func (pg *page) marshal(buf *Buffer, woffset int, head *pageDelta,
	hiItm unsafe.Pointer, child bool, maxSegments int) (offset int, staleFdSz int, numSegments int) {

	if head == nil {
		return
	}

	var isFullMarshal bool = maxSegments == 0
	stateBufOffset := woffset
	hasReloc := false

	if !child {
		woffset += 2

		// pageLow
		woffset = pg.marshalIndexKey(pg.MinItem(), woffset, buf)

		// chainlen
		binary.BigEndian.PutUint16(buf.Get(woffset, 2), uint16(head.chainLen))
		woffset += 2

		// numItems
		binary.BigEndian.PutUint16(buf.Get(woffset, 2), uint16(head.numItems))
		woffset += 2

		// pageHigh
		woffset = pg.marshalIndexKey(pg.MaxItem(), woffset, buf)
	}

	pw := newPgDeltaWalker(head, pg.ctx)
	defer pw.Close()
loop:
	for ; !pw.End(); pw.Next() {
		op := pw.Op()
		switch op {
		case opInsertDelta, opDeleteDelta:
			itm := pw.Item()
			if pg.cmp(itm, hiItm) < 0 {
				binary.BigEndian.PutUint16(buf.Get(woffset, 2), uint16(op))
				woffset += 2
				woffset = pg.marshalItem(itm, woffset, buf)
			}
		case opPageSplitDelta:
			itm := pw.Item()
			if pg.cmp(itm, hiItm) < 0 {
				hiItm = itm
			}
			binary.BigEndian.PutUint16(buf.Get(woffset, 2), uint16(op))
			woffset += 2
		case opPageMergeDelta:
			mergeSibling := pw.MergeSibling()
			var fdSz int
			woffset, fdSz, _ = pg.marshal(buf, woffset, mergeSibling, hiItm, true, 0)
			if !hasReloc {
				staleFdSz += fdSz
			}
		case opBasePage:
			if child {
				// Encode items as insertDelta
				for _, itm := range pw.BaseItems() {
					if pg.cmp(itm, hiItm) < 0 {
						binary.BigEndian.PutUint16(buf.Get(woffset, 2), uint16(opInsertDelta))
						woffset += 2

						woffset = pg.marshalItem(itm, woffset, buf)
					}
				}
			} else {
				binary.BigEndian.PutUint16(buf.Get(woffset, 2), uint16(op))
				woffset += 2
				bufNitmOffset := woffset
				nItms := 0
				woffset += 2
				for _, itm := range pw.BaseItems() {
					if pg.cmp(itm, hiItm) < 0 {
						woffset = pg.marshalItem(itm, woffset, buf)
						nItms++
					}
				}
				binary.BigEndian.PutUint16(buf.Get(bufNitmOffset, 2), uint16(nItms))
			}
			break loop
		case opFlushPageDelta, opRelocPageDelta, opSwapoutDelta:
			offset, dataSz, numSegs := pw.FlushInfo()
			if int(numSegs) > maxSegments {
				isFullMarshal = true
			} else if !isFullMarshal {
				binary.BigEndian.PutUint16(buf.Get(woffset, 2), uint16(opFlushPageDelta))
				woffset += 2
				binary.BigEndian.PutUint64(buf.Get(woffset, 8), uint64(offset))
				woffset += 8
				numSegments = int(numSegs)
				break loop
			}

			if !hasReloc {
				staleFdSz += int(dataSz)
			}

			if op == opRelocPageDelta {
				hasReloc = true
			}
		case opRollbackDelta:
			start, end := pw.RollbackInfo()
			binary.BigEndian.PutUint16(buf.Get(woffset, 2), uint16(op))
			woffset += 2
			binary.BigEndian.PutUint64(buf.Get(woffset, 8), uint64(start))
			woffset += 8
			binary.BigEndian.PutUint64(buf.Get(woffset, 8), uint64(end))
			woffset += 8
		case opPageRemoveDelta, opMetaDelta, opSwapinDelta:
		default:
			panic(fmt.Sprintf("unknown delta %d", op))
		}
	}

	if !child {
		// pageVersion
		state := head.state
		if numSegments == 0 {
			state.IncrVersion()
		} else {
			numSegments++
		}
		binary.BigEndian.PutUint16(buf.Get(stateBufOffset, 2), uint16(state))
	}

	pw.SwapIn(pg)
	return woffset, staleFdSz, numSegments
}

func getLSSPageMeta(data []byte) (itm unsafe.Pointer, pv uint16) {
	roffset := 0
	pv = binary.BigEndian.Uint16(data[roffset : roffset+2])
	roffset += 2

	l := int(binary.BigEndian.Uint16(data[roffset : roffset+2]))
	roffset += 2
	if l == 0 {
		itm = skiplist.MaxItem
	} else {
		itm = unsafe.Pointer(&data[roffset])
	}

	return
}

func (pg *page) Unmarshal(data []byte, ctx *wCtx) {
	pg.unmarshalDelta(data, ctx)
}

func (pg *page) unmarshalDelta(data []byte, ctx *wCtx) (offset LSSOffset, hasChain bool) {
	roffset := 0
	state := pageState(binary.BigEndian.Uint16(data[roffset : roffset+2]))
	state.SetFlushed()

	roffset += 2
	pg.state = state

	pg.low, roffset = pg.unmarshalIndexKey(data, roffset)

	chainLen := binary.BigEndian.Uint16(data[roffset : roffset+2])
	roffset += 2

	numItems := binary.BigEndian.Uint16(data[roffset : roffset+2])
	roffset += 2

	var itm, hiItm unsafe.Pointer
	hiItm, roffset = pg.unmarshalIndexKey(data, roffset)

	lastPd := (*pageDelta)(unsafe.Pointer(pg.allocMetaDelta(hiItm)))
	lastPd.op = opMetaDelta
	lastPd.state = state
	lastPd.numItems = numItems
	lastPd.chainLen = chainLen
	lastPd.next = nil
	lastPd.rightSibling = nil
	pg.head = lastPd

	var pd *pageDelta
loop:
	for roffset < len(data) {
		op := pageOp(binary.BigEndian.Uint16(data[roffset : roffset+2]))
		roffset += 2

		switch op {
		case opInsertDelta, opDeleteDelta:
			itm, roffset = pg.unmarshalItem(data, roffset)
			rpd := pg.allocRecordDelta(itm)
			*(*pageDelta)(unsafe.Pointer(rpd)) = *pg.head
			rpd.next = nil
			rpd.op = op
			pd = (*pageDelta)(unsafe.Pointer(rpd))
			pd.next = nil
		case opPageSplitDelta:
			spd := pg.allocSplitPageDelta(hiItm)
			*(*pageDelta)(unsafe.Pointer(spd)) = *pg.head
			spd.next = nil
			spd.op = op
			spd.itm = pg.head.hiItm
			pd = (*pageDelta)(unsafe.Pointer(spd))
		case opBasePage:
			nItms := int(binary.BigEndian.Uint16(data[roffset : roffset+2]))
			roffset += 2
			size := 0
			var itms []unsafe.Pointer
			for i := 0; i < nItms; i++ {
				itm, roffset = pg.unmarshalItem(data, roffset)
				itms = append(itms, itm)
				size += int(pg.itemSize(itm))
			}

			bp := pg.newBasePage(itms)
			bp.state = state
			pd = (*pageDelta)(unsafe.Pointer(bp))
		case opFlushPageDelta, opRelocPageDelta:
			offset = LSSOffset(binary.BigEndian.Uint64(data[roffset : roffset+8]))
			hasChain = true
			break loop
		case opRollbackDelta:
			rpd := pg.allocRollbackPageDelta()
			*(*pageDelta)(unsafe.Pointer(rpd)) = *pg.head
			rpd.next = nil
			rpd.rb = rollbackSn{
				start: binary.BigEndian.Uint64(data[roffset : roffset+8]),
				end:   binary.BigEndian.Uint64(data[roffset+8 : roffset+16]),
			}

			rpd.op = op
			pd = (*pageDelta)(unsafe.Pointer(rpd))
			roffset += 16
			pd.next = nil
		}

		lastPd.next = pd
		lastPd = pd
	}

	pg.tail = lastPd
	return
}

func (pg *page) AddFlushRecord(offset LSSOffset, dataSz int, numSegments int) {
	fd := pg.newFlushPageDelta(offset, dataSz, numSegments)
	pg.head = (*pageDelta)(unsafe.Pointer(fd))
}

func (pg *page) Rollback(startSn, endSn uint64) {
	pd := pg.allocRollbackPageDelta()
	*(*pageDelta)(unsafe.Pointer(pd)) = *pg.head
	pd.next = pg.head

	pd.op = opRollbackDelta
	pd.chainLen++
	pd.rb.start = startSn
	pd.rb.end = endSn
	pg.head = (*pageDelta)(unsafe.Pointer(pd))
}

func marshalPageSMO(pg Page, b *Buffer) []byte {
	woffset := 0

	target := pg.(*page)
	l := int(target.itemSize(target.low))
	buf := b.Get(0, l+2)
	binary.BigEndian.PutUint16(buf[woffset:woffset+2], uint16(l))
	woffset += 2
	memcopy(unsafe.Pointer(&buf[woffset]), target.low, l)
	woffset += l

	return buf[:woffset]
}

func getRmPageLow(data []byte) unsafe.Pointer {
	roffset := 0
	l := int(binary.BigEndian.Uint16(data[roffset : roffset+2]))
	if l == 0 {
		return nil
	}

	roffset += 2
	return unsafe.Pointer(&data[roffset])
}

func (pg *page) GetFlushDataSize() int {
	hasReloc := false
	flushDataSz := 0
	pw := newPgDeltaWalker(pg.head, pg.ctx)
	defer pw.Close()
loop:
	for ; !pw.End(); pw.Next() {
		op := pw.Op()
		switch op {
		case opBasePage:
			break loop
		case opFlushPageDelta, opRelocPageDelta:
			if !hasReloc {
				_, d, _ := pw.FlushInfo()
				flushDataSz += int(d)
			}

			if op == opRelocPageDelta {
				hasReloc = true
			}
		}
	}

	return flushDataSz
}

func decodePageState(data []byte) (state pageState, key unsafe.Pointer) {
	roffset := 0
	state = pageState(binary.BigEndian.Uint16(data[roffset : roffset+2]))
	roffset += 2

	flag := data[roffset]
	roffset += 1
	if flag == minKeyEncoded {
		key = skiplist.MinItem
	} else {
		key = unsafe.Pointer(&data[roffset])
	}

	return
}

func (pg *page) GetVersion() uint16 {
	if pg.head == nil {
		return 0
	}

	return pg.head.state.GetVersion()
}

func (pg *page) IsFlushed() bool {
	if pg.head == nil {
		return false
	}

	return pg.head.state.IsFlushed()
}

func (pg *page) MaxItem() unsafe.Pointer {
	if pg.head == nil {
		return skiplist.MaxItem
	}

	return pg.head.hiItm
}

func (pg *page) MinItem() unsafe.Pointer {
	return pg.low
}

func (pg *page) Next() PageId {
	if pg.head == nil {
		return pg.nextPid
	}

	return pg.head.rightSibling
}

func newPage(ctx *wCtx, low unsafe.Pointer, ptr unsafe.Pointer) Page {
	return newPage2(low, ptr, ctx, ctx.storeCtx, ctx.pgAllocCtx)
}

func newPage2(low unsafe.Pointer, ptr unsafe.Pointer,
	ctx *wCtx, sCtx *storeCtx, aCtx *allocCtx) Page {
	pg := &page{
		ctx:         ctx,
		storeCtx:    sCtx,
		allocCtx:    aCtx,
		head:        (*pageDelta)(ptr),
		low:         low,
		prevHeadPtr: ptr,
	}
	return pg
}

func (s *Plasma) newSeedPage(ctx *wCtx) Page {
	pg := newPage(ctx, skiplist.MinItem, nil).(*page)
	d := pg.allocMetaDelta(skiplist.MaxItem)
	d.op = opMetaDelta
	d.rightSibling = s.EndPageId()
	d.next = nil

	pg.head = (*pageDelta)(unsafe.Pointer(d))
	return pg
}

// TODO: Depreciate
func (pg *page) IsEmpty() bool {
	return pg.head == nil
}

func (pg *page) IsEvictable() bool {
	if pg.head == nil {
		return false
	}

	switch pg.head.op {
	case opFlushPageDelta, opRelocPageDelta:
		return true
	}

	return false
}

func (pg *page) NeedsFlush() bool {
	if pg.head == nil {
		return false
	}

	switch pg.head.op {
	case opFlushPageDelta, opRelocPageDelta, opPageRemoveDelta, opSwapoutDelta:
		return false
	}

	return true
}

func (pg *page) GetFlushInfo() (LSSOffset, int, int) {
	if pg.head.op == opFlushPageDelta || pg.head.op == opRelocPageDelta {
		fpd := (*flushPageDelta)(unsafe.Pointer(pg.head))
		return fpd.offset, int(fpd.numSegments), int(fpd.flushDataSz)
	} else if pg.head.op == opSwapoutDelta {
		sod := (*swapoutDelta)(unsafe.Pointer(pg.head))
		return sod.offset, int(sod.numSegments), 0
	}

	panic(fmt.Sprintf("invalid delta op:%d", pg.head.op))
}

func (pg *page) Evict(offset LSSOffset, numSegments int) {
	pg.free(true)
	sod := pg.allocSwapoutDelta(pg.head.hiItm)
	hiItm := sod.hiItm
	*(*pageDelta)(unsafe.Pointer(sod)) = *pg.head
	sod.hiItm = hiItm
	sod.state.SetEvicted(true)
	sod.op = opSwapoutDelta
	sod.offset = offset

	if numSegments == 0 {
		sod.state.IncrVersion()
		sod.numSegments = 1
	} else {
		sod.numSegments = int32(numSegments)
	}
	sod.next = nil
	pg.head = (*pageDelta)(unsafe.Pointer(sod))
}

func (pg *page) SwapIn(ptr *pageDelta) {
	sid := pg.allocSwapinDelta()
	sid.ptr = ptr
	*(*pageDelta)(unsafe.Pointer(sid)) = *pg.head
	sid.state.SetEvicted(false)
	sid.op = opSwapinDelta
	sid.next = pg.head
	pg.head = (*pageDelta)(unsafe.Pointer(sid))
}

func (pg *page) SetNumSegments(n int) {
	if pg.head.op == opFlushPageDelta {
		fpd := (*flushPageDelta)(unsafe.Pointer(pg.head))
		fpd.numSegments = int32(n)
		return
	}

	panic(fmt.Sprintf("invalid delta op:%d", pg.head.op))
}

func (pg *page) ComputeMemUsed() int {
	_, size := computeMemUsed(pg.head, pg.itemSize)
	return size
}

func computeMemUsed(pd *pageDelta, itemSize ItemSizeFn) (n int, size int) {
loop:
	for ; pd != nil; pd = pd.next {
		switch pd.op {
		case opBasePage:
			bp := (*basePage)(unsafe.Pointer(pd))
			for _, _ = range bp.items {
				n++
			}

			size += mm.SizeAt(unsafe.Pointer(pd))
			break loop
		case opInsertDelta, opDeleteDelta:
			n++
			size += mm.SizeAt(unsafe.Pointer(pd))
		case opPageRemoveDelta:
			size += mm.SizeAt(unsafe.Pointer(pd))
		case opPageSplitDelta:
			size += mm.SizeAt(unsafe.Pointer(pd))
		case opPageMergeDelta:
			pdm := (*mergePageDelta)(unsafe.Pointer(pd))
			nx, sx := computeMemUsed(pdm.mergeSibling, itemSize)
			size += int(mergePageDeltaSize + itemSize(pdm.hiItm))
			size += sx
			n += nx
		case opFlushPageDelta, opRelocPageDelta:
			size += mm.SizeAt(unsafe.Pointer(pd))
		case opRollbackDelta:
			size += mm.SizeAt(unsafe.Pointer(pd))
		case opMetaDelta:
			size += mm.SizeAt(unsafe.Pointer(pd))
		case opSwapoutDelta:
			size += mm.SizeAt(unsafe.Pointer(pd))
			break loop
		case opSwapinDelta:
			sid := (*swapinDelta)(unsafe.Pointer(pd))
			nx, sx := computeMemUsed(sid.ptr, itemSize)
			size += int(swapinDeltaSize)
			size += sx
			n += nx
		default:
			panic(fmt.Sprintf("unsupported delta %d", pd.op))
		}
	}

	return n, size
}
