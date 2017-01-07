package plasma

import (
	"fmt"
	"github.com/t3rm1n4l/nitro/skiplist"
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"
)

type PageReader func(offset lssOffset) (Page, error)

const maxCtxBuffers = 3

var (
	memQuota    int64
	dbInstances *skiplist.Skiplist
)

func init() {
	dbInstances = skiplist.New()
}

type Plasma struct {
	Config
	*skiplist.Skiplist
	*pageTable
	wlist                  []*Writer
	lss                    *lsStore
	lssCleanerWriter       *Writer
	persistWriters         []*Writer
	evictWriters           []*Writer
	stoplssgc, stopswapper chan struct{}
	sync.RWMutex

	// MVCC data structures
	mvcc         sync.RWMutex
	currSn       uint64
	numSnCreated int
	gcSn         uint64
	currSnapshot *Snapshot

	lastMaxSn uint64

	rpSns          unsafe.Pointer
	rpVersion      uint16
	recoveryPoints []*RecoveryPoint
}

type Stats struct {
	Compacts int64
	Splits   int64
	Merges   int64
	Inserts  int64
	Deletes  int64

	CompactConflicts int64
	SplitConflicts   int64
	MergeConflicts   int64
	InsertConflicts  int64
	DeleteConflicts  int64
	SwapInConflicts  int64

	BytesIncoming int64
	BytesWritten  int64

	FlushDataSz int64
	MemSz       int64

	NumCachedPages  int64
	NumPages        int64
	NumPagesSwapOut int64
	NumPagesSwapIn  int64
}

func (s *Stats) Merge(o *Stats) {
	s.Compacts += o.Compacts
	s.Splits += o.Splits
	s.Merges += o.Merges
	s.Inserts += o.Inserts
	s.Deletes += o.Deletes

	s.CompactConflicts += o.CompactConflicts
	s.SplitConflicts += o.SplitConflicts
	s.MergeConflicts += o.MergeConflicts
	s.InsertConflicts += o.InsertConflicts
	s.DeleteConflicts += o.DeleteConflicts
	s.SwapInConflicts += o.SwapInConflicts

	s.FlushDataSz += o.FlushDataSz
	s.MemSz += o.MemSz

	s.NumPagesSwapOut += o.NumPagesSwapOut
	s.NumPagesSwapIn += o.NumPagesSwapIn

	s.BytesIncoming += o.BytesIncoming
}

func (s Stats) String() string {
	return fmt.Sprintf("===== Stats =====\n"+
		"count             = %d\n"+
		"compacts          = %d\n"+
		"splits            = %d\n"+
		"merges            = %d\n"+
		"inserts           = %d\n"+
		"deletes           = %d\n"+
		"compact_conflicts = %d\n"+
		"split_conflicts   = %d\n"+
		"merge_conflicts   = %d\n"+
		"insert_conflicts  = %d\n"+
		"delete_conflicts  = %d\n"+
		"swapin_conflicts  = %d\n"+
		"flushdata_size    = %d\n"+
		"memory_size       = %d\n"+
		"num_cached_pages  = %d\n"+
		"num_pages         = %d\n"+
		"num_pages_swapout = %d\n"+
		"num_pages_swapin  = %d\n"+
		"bytes_incoming    = %d\n"+
		"bytes_written     = %d\n"+
		"write_amp         = %.2f\n",
		s.Inserts-s.Deletes,
		s.Compacts, s.Splits, s.Merges,
		s.Inserts, s.Deletes, s.CompactConflicts,
		s.SplitConflicts, s.MergeConflicts,
		s.InsertConflicts, s.DeleteConflicts,
		s.SwapInConflicts, s.FlushDataSz,
		s.MemSz, s.NumCachedPages,
		s.NumPages, s.NumPagesSwapOut,
		s.NumPagesSwapIn, s.BytesIncoming,
		s.BytesWritten, float64(s.BytesWritten)/float64(s.BytesIncoming))
}

func New(cfg Config) (*Plasma, error) {
	var err error

	cfg = applyConfigDefaults(cfg)
	sl := skiplist.New()
	s := &Plasma{
		Config:   cfg,
		Skiplist: sl,
	}

	ptWr := s.NewWriter()

	var cfGetter, lfGetter FilterGetter
	if cfg.EnableShapshots {
		cfGetter = func() ItemFilter {
			gcSn := atomic.LoadUint64(&s.gcSn) + 1
			rpSns := (*[]uint64)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&s.rpSns))))

			var gcPos int
			for _, sn := range *rpSns {
				if sn < gcSn {
					gcPos++
				} else {
					break
				}
			}

			var snIntervals []uint64
			if gcPos == 0 {
				snIntervals = []uint64{0, gcSn}
			} else {
				snIntervals = make([]uint64, gcPos+2)
				copy(snIntervals[1:], (*rpSns)[:gcPos])
				snIntervals[gcPos+1] = gcSn
			}

			return &gcFilter{snIntervals: snIntervals}
		}

		lfGetter = func() ItemFilter {
			return &rollbackFilter{}
		}
	} else {
		cfGetter = func() ItemFilter {
			return new(defaultFilter)
		}

		lfGetter = func() ItemFilter {
			return &nilFilter
		}
	}

	s.pageTable = newPageTable(sl, cfg.ItemSize, cfg.Compare, cfGetter, lfGetter, ptWr.wCtx.sts)

	pid := s.StartPageId()
	pg := s.newSeedPage()
	s.CreateMapping(pid, pg)

	if s.shouldPersist {
		s.lss, err = newLSStore(cfg.File, cfg.LSSLogSegmentSize, cfg.FlushBufferSize, 2)
		if err != nil {
			return nil, err
		}

		err = s.doRecovery()
	}

	s.doInit()

	if s.shouldPersist {
		s.persistWriters = make([]*Writer, runtime.NumCPU())
		s.evictWriters = make([]*Writer, runtime.NumCPU())
		for i, _ := range s.persistWriters {
			s.persistWriters[i] = s.NewWriter()
			s.evictWriters[i] = s.NewWriter()
		}
		s.lssCleanerWriter = s.NewWriter()

		s.stoplssgc = make(chan struct{})
		s.stopswapper = make(chan struct{})

		if cfg.AutoLSSCleaning {
			go s.lssCleanerDaemon()
		}

		if cfg.AutoSwapper {
			go s.swapperDaemon()
		}
	}

	sbuf := dbInstances.MakeBuf()
	defer dbInstances.FreeBuf(sbuf)
	dbInstances.Insert(unsafe.Pointer(s), ComparePlasma, sbuf, &dbInstances.Stats)
	return s, err
}

func (s *Plasma) doInit() {
	if s.EnableShapshots {
		if s.currSn == 0 {
			s.currSn = 1
		}

		s.currSnapshot = &Snapshot{
			sn:       s.currSn,
			refCount: 1,
			db:       s,
		}

		s.updateMaxSn(s.currSn, true)
		s.updateRecoveryPoints(s.recoveryPoints)
		s.updateRPSns(s.recoveryPoints)
	}
}

func (s *Plasma) doRecovery() error {
	pg := &page{
		storeCtx: s.storeCtx,
	}

	w := s.NewWriter()

	buf := w.wCtx.GetBuffer(0)

	fn := func(offset lssOffset, bs []byte) (bool, error) {
		typ := getLSSBlockType(bs)
		bs = bs[lssBlockTypeSize:]
		switch typ {
		case lssDiscard:
		case lssRecoveryPoints:
			s.rpVersion, s.recoveryPoints = unmarshalRPs(bs)
		case lssMaxSn:
			s.currSn = decodeMaxSn(bs)
		case lssPageRemove:
			rmPglow := unmarshalPageSMO(pg, bs)
			pid := s.getPageId(rmPglow, w.wCtx)
			if pid != nil {
				currPg, err := s.ReadPage(pid, w.wCtx.pgRdrFn, true)
				if err != nil {
					return false, err
				}

				w.sts.FlushDataSz -= int64(currPg.GetFlushDataSize())
				s.unindexPage(pid, w.wCtx)
			}
		case lssPageData, lssPageReloc, lssPageUpdate:
			pg.Unmarshal(bs, w.wCtx)

			newPageData := (typ == lssPageData || typ == lssPageReloc)
			pid := s.getPageId(pg.low, w.wCtx)

			if pid == nil {
				if newPageData {
					pid = s.AllocPageId()
					s.CreateMapping(pid, pg)
					s.indexPage(pid, w.wCtx)
				} else {
					break
				}
			}

			flushDataSz := len(bs)
			w.sts.FlushDataSz += int64(flushDataSz)

			currPg, err := s.ReadPage(pid, w.wCtx.pgRdrFn, true)
			if err != nil {
				return false, err
			}

			if newPageData {
				w.sts.FlushDataSz -= int64(currPg.GetFlushDataSize())
			} else {
				pg.Append(currPg)
			}

			pg.AddFlushRecord(offset, flushDataSz, false)
			s.CreateMapping(pid, pg)
		}

		pg.Reset()
		return true, nil
	}

	err := s.lss.Visitor(fn, buf)
	if err != nil {
		return err
	}

	// Initialize rightSiblings for all pages
	var lastPg Page
	callb := func(pid PageId, partn RangePartition) error {
		pg, err := s.ReadPage(pid, w.pgRdrFn, true)
		if lastPg != nil {
			if err == nil && s.cmp(lastPg.MaxItem(), pg.MinItem()) != 0 {
				panic("found missing page")
			}

			lastPg.SetNext(pid)
		}

		lastPg = pg
		w.sts.MemSz += int64(pg.ComputeMemUsed())
		return err
	}

	s.PageVisitor(callb, 1)
	s.gcSn = s.currSn

	if lastPg != nil {
		lastPg.SetNext(s.EndPageId())
		if lastPg.MaxItem() != skiplist.MaxItem {
			panic("invalid last page")
		}
	}

	return err
}

func (s *Plasma) Close() {
	if s.Config.AutoLSSCleaning {
		s.stoplssgc <- struct{}{}
		<-s.stoplssgc
	}

	if s.Config.AutoSwapper {
		s.stopswapper <- struct{}{}
		<-s.stopswapper
	}

	if s.Config.shouldPersist {
		s.lss.Close()
	}

	sbuf := dbInstances.MakeBuf()
	defer dbInstances.FreeBuf(sbuf)
	dbInstances.Delete(unsafe.Pointer(s), ComparePlasma, sbuf, &dbInstances.Stats)
}

func ComparePlasma(a, b unsafe.Pointer) int {
	return int(uintptr(a)) - int(uintptr(b))
}

type Writer struct {
	*Plasma
	*wCtx
}

type wCtx struct {
	buf       *skiplist.ActionBuffer
	pgBuffers [][]byte
	slSts     *skiplist.Stats
	sts       *Stats

	pgRdrFn PageReader
}

func (s *Plasma) newWCtx() *wCtx {
	ctx := &wCtx{
		buf:       s.Skiplist.MakeBuf(),
		slSts:     &s.Skiplist.Stats,
		sts:       new(Stats),
		pgBuffers: make([][]byte, maxCtxBuffers),
	}

	ctx.pgRdrFn = func(offset lssOffset) (Page, error) {
		return s.fetchPageFromLSS(offset, ctx)
	}

	return ctx
}

func (ctx *wCtx) GetBuffer(id int) []byte {
	if ctx.pgBuffers[id] == nil {
		ctx.pgBuffers[id] = make([]byte, maxPageEncodedSize)
	}

	return ctx.pgBuffers[id]
}

func (s *Plasma) NewWriter() *Writer {

	w := &Writer{
		Plasma: s,
		wCtx:   s.newWCtx(),
	}

	s.Lock()
	defer s.Unlock()

	s.wlist = append(s.wlist, w)
	return w
}

func (s *Plasma) MemoryInUse() int64 {
	s.RLock()
	defer s.RUnlock()

	var memSz int64
	for _, w := range s.wlist {
		memSz += w.sts.MemSz
	}

	return memSz
}

func (s *Plasma) GetStats() Stats {
	var sts Stats

	s.RLock()
	defer s.RUnlock()

	sts.NumPages = int64(s.Skiplist.GetStats().NodeCount + 1)
	for _, w := range s.wlist {
		sts.Merge(w.sts)
	}
	sts.NumCachedPages = sts.NumPages - sts.NumPagesSwapOut + sts.NumPagesSwapIn
	sts.BytesWritten = s.lss.BytesWritten()
	return sts
}

func (s *Plasma) LSSDataSize() int64 {
	var sz int64

	s.RLock()
	defer s.RUnlock()

	for _, w := range s.wlist {
		sz += w.sts.FlushDataSz
	}

	return sz
}

func (s *Plasma) indexPage(pid PageId, ctx *wCtx) {
	n := pid.(*skiplist.Node)
	if !s.Skiplist.Insert4(n, s.cmp, s.cmp, ctx.buf, n.Level(), false, ctx.slSts) {
		panic("duplicate index node")
	}
}

func (s *Plasma) unindexPage(pid PageId, ctx *wCtx) {
	n := pid.(*skiplist.Node)
	s.Skiplist.DeleteNode(n, s.cmp, ctx.buf, ctx.slSts)
}

func (s *Plasma) tryPageRemoval(pid PageId, pg Page, ctx *wCtx) {
	n := pid.(*skiplist.Node)
	itm := n.Item()
	prev, _, found := s.Skiplist.Lookup(itm, s.cmp, ctx.buf, ctx.slSts)
	// Somebody completed the removal already
	if !found {
		return
	}

	pPid := PageId(prev)
	pPg, err := s.ReadPage(pPid, ctx.pgRdrFn, true)
	if err != nil {
		s.logError(fmt.Sprintf("tryPageRemove: err=%v", err))
		return
	}

	var pgBuf = ctx.GetBuffer(0)
	var metaBuf = ctx.GetBuffer(1)
	var fdSz, staleFdSz int

	pPg.Merge(pg)

	var offsets []lssOffset
	var wbufs [][]byte
	var res lssResource

	if s.shouldPersist {
		metaBuf = marshalPageSMO(pg, metaBuf)
		pgBuf, fdSz, staleFdSz = pPg.MarshalFull(pgBuf)

		sizes := []int{
			lssBlockTypeSize + len(metaBuf),
			lssBlockTypeSize + len(pgBuf),
		}

		offsets, wbufs, res = s.lss.ReserveSpaceMulti(sizes)

		writeLSSBlock(wbufs[0], lssPageRemove, metaBuf)

		writeLSSBlock(wbufs[1], lssPageData, pgBuf)
		pPg.AddFlushRecord(offsets[1], fdSz, true)
	}

	if s.UpdateMapping(pPid, pPg) {
		s.unindexPage(pid, ctx)
		ctx.sts.MemSz += int64(pPg.GetMemUsed())

		if s.shouldPersist {
			ctx.sts.FlushDataSz += int64(fdSz) - int64(staleFdSz)
			s.lss.FinalizeWrite(res)
		}

	} else if s.shouldPersist {
		discardLSSBlock(wbufs[0])
		discardLSSBlock(wbufs[1])
		s.lss.FinalizeWrite(res)
	}
}

func (s *Plasma) isStartPage(pid PageId) bool {
	return pid.(*skiplist.Node) == s.Skiplist.HeadNode()
}

func (s *Plasma) StartPageId() PageId {
	return s.Skiplist.HeadNode()
}

func (s *Plasma) EndPageId() PageId {
	return s.Skiplist.TailNode()
}

func (s *Plasma) trySMOs(pid PageId, pg Page, ctx *wCtx, doUpdate bool) bool {
	var updated bool

	if pg.NeedCompaction(s.Config.MaxDeltaChainLen) {
		staleFdSz := pg.Compact()
		updated = s.UpdateMapping(pid, pg)
		if updated {
			ctx.sts.Compacts++
			ctx.sts.FlushDataSz -= int64(staleFdSz)
			ctx.sts.MemSz += int64(pg.GetMemUsed())
		} else {
			ctx.sts.CompactConflicts++
		}
	} else if pg.NeedSplit(s.Config.MaxPageItems) {
		splitPid := s.AllocPageId()

		var fdSz, splitFdSz int
		var pgBuf = ctx.GetBuffer(0)
		var splitPgBuf = ctx.GetBuffer(1)

		newPg := pg.Split(splitPid)

		// Skip split, but compact
		if newPg == nil {
			s.FreePageId(splitPid)
			staleFdSz := pg.Compact()
			if updated = s.UpdateMapping(pid, pg); updated {
				ctx.sts.FlushDataSz -= int64(staleFdSz)
				ctx.sts.MemSz += int64(pg.GetMemUsed())
			}
			return updated
		}

		var offsets []lssOffset
		var wbufs [][]byte
		var res lssResource

		// Replace one page with two pages
		if s.shouldPersist {
			pgBuf, fdSz = pg.Marshal(pgBuf)
			splitPgBuf, splitFdSz = newPg.Marshal(splitPgBuf)

			sizes := []int{
				lssBlockTypeSize + len(pgBuf),
				lssBlockTypeSize + len(splitPgBuf),
			}

			offsets, wbufs, res = s.lss.ReserveSpaceMulti(sizes)

			typ := pgFlushLSSType(pg)
			writeLSSBlock(wbufs[0], typ, pgBuf)
			pg.AddFlushRecord(offsets[0], fdSz, false)

			writeLSSBlock(wbufs[1], lssPageData, splitPgBuf)
			newPg.AddFlushRecord(offsets[1], splitFdSz, false)
		}

		s.CreateMapping(splitPid, newPg)
		if updated = s.UpdateMapping(pid, pg); updated {
			ctx.sts.MemSz += int64(pg.GetMemUsed())
			ctx.sts.MemSz += int64(newPg.GetMemUsed())
			s.indexPage(splitPid, ctx)
			ctx.sts.Splits++

			if s.shouldPersist {
				ctx.sts.FlushDataSz += int64(fdSz) + int64(splitFdSz)
				s.lss.FinalizeWrite(res)
			}
		} else {
			ctx.sts.SplitConflicts++
			s.FreePageId(splitPid)

			if s.shouldPersist {
				discardLSSBlock(wbufs[0])
				discardLSSBlock(wbufs[1])
				s.lss.FinalizeWrite(res)
			}
		}
	} else if !s.isStartPage(pid) && pg.NeedMerge(s.Config.MinPageItems) {
		pg.Close()
		if updated = s.UpdateMapping(pid, pg); updated {
			s.tryPageRemoval(pid, pg, ctx)
			ctx.sts.Merges++
			ctx.sts.MemSz += int64(pg.GetMemUsed())
		} else {
			ctx.sts.MergeConflicts++
		}
	} else if doUpdate {
		if updated = s.UpdateMapping(pid, pg); updated {
			ctx.sts.MemSz += int64(pg.GetMemUsed())
		}
	}

	return updated
}

func (s *Plasma) fetchPage(itm unsafe.Pointer, ctx *wCtx) (pid PageId, pg Page, err error) {
retry:
	if prev, curr, found := s.Skiplist.Lookup(itm, s.cmp, ctx.buf, ctx.slSts); found {
		pid = curr
	} else {
		pid = prev
	}

refresh:
	if pg, err = s.ReadPage(pid, ctx.pgRdrFn, true); err != nil {
		return nil, nil, err
	}

	if !pg.InRange(itm) {
		pid = pg.Next()
		goto refresh
	}

	if pg.NeedRemoval() {
		s.tryPageRemoval(pid, pg, ctx)
		goto retry
	}

	return
}

func (w *Writer) Insert(itm unsafe.Pointer) error {
retry:
	pid, pg, err := w.fetchPage(itm, w.wCtx)
	if err != nil {
		return err
	}

	pg.Insert(itm)

	if !w.trySMOs(pid, pg, w.wCtx, true) {
		w.sts.InsertConflicts++
		goto retry
	}
	w.sts.BytesIncoming += int64(w.itemSize(itm))
	w.sts.Inserts++
	w.sts.MemSz += int64(pg.GetMemUsed())

	return nil
}

func (w *Writer) Delete(itm unsafe.Pointer) error {
retry:
	pid, pg, err := w.fetchPage(itm, w.wCtx)
	if err != nil {
		return err
	}

	pg.Delete(itm)

	if !w.trySMOs(pid, pg, w.wCtx, true) {
		w.sts.DeleteConflicts++
		goto retry
	}
	w.sts.BytesIncoming += int64(w.itemSize(itm))
	w.sts.Deletes++
	w.sts.MemSz += int64(pg.GetMemUsed())

	return nil
}

func (w *Writer) Lookup(itm unsafe.Pointer) (unsafe.Pointer, error) {
	pid, pg, err := w.fetchPage(itm, w.wCtx)
	if err != nil {
		return nil, err
	}

	ret := pg.Lookup(itm)
	w.trySMOs(pid, pg, w.wCtx, false)
	return ret, nil
}

func (s *Plasma) fetchPageFromLSS(baseOffset lssOffset, ctx *wCtx) (*page, error) {
	pg := &page{
		storeCtx: s.storeCtx,
		inCache:  false,
	}

	offset := baseOffset
	data := ctx.GetBuffer(0)
loop:
	for {
		l, err := s.lss.Read(offset, data)
		if err != nil {
			return nil, err
		}

		typ := getLSSBlockType(data)
		switch typ {
		case lssPageData, lssPageReloc, lssPageUpdate:
			currPgDelta := &page{
				storeCtx: s.storeCtx,
			}
			data := data[lssBlockTypeSize:l]
			nextOffset, hasChain := currPgDelta.unmarshalDelta(data, ctx)
			currPgDelta.AddFlushRecord(offset, len(data), false)
			pg.Append(currPgDelta)
			offset = nextOffset

			if !hasChain {
				break loop
			}
		default:
			panic(fmt.Sprintf("Invalid page data type %d", typ))
		}
	}

	if pg.head != nil {
		pg.head.rightSibling = pg.getPageId(pg.head.hiItm, ctx)
	}

	pg.prevHeadPtr = unsafe.Pointer(uintptr(uint64(baseOffset) | evictMask))

	return pg, nil
}

func (s *Plasma) logError(err string) {
	fmt.Printf("Plasma: (fatal error - %s)\n", err)
}

func (w *Writer) CompactAll() {
	callb := func(pid PageId, partn RangePartition) error {
		if pg, err := w.ReadPage(pid, nil, false); err == nil {
			staleFdSz := pg.Compact()
			if updated := w.UpdateMapping(pid, pg); updated {
				w.wCtx.sts.FlushDataSz -= int64(staleFdSz)
				w.wCtx.sts.MemSz += int64(pg.GetMemUsed())
			}
		}
		return nil
	}

	w.PageVisitor(callb, 1)
}

func SetMemoryQuota(m int64) {
	atomic.StoreInt64(&memQuota, m)
}

func QuotaSwapper() bool {
	return MemoryInUse() >= int64(float64(atomic.LoadInt64(&memQuota))*0.7)
}

func MemoryInUse() (sz int64) {
	buf := dbInstances.MakeBuf()
	defer dbInstances.FreeBuf(buf)
	iter := dbInstances.NewIterator(ComparePlasma, buf)
	for iter.SeekFirst(); iter.Valid(); iter.Next() {
		db := (*Plasma)(iter.Get())
		sz += db.MemoryInUse()
	}

	return
}
