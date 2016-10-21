package plasma

import (
	"fmt"
	"github.com/t3rm1n4l/nitro/skiplist"
	"runtime"
	"sync"
	"unsafe"
)

type PageReader func(offset lssOffset) (Page, error)

const maxCtxBuffers = 3

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

	FlushDataSz int64
	MemSz       int64

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

	s.FlushDataSz += o.FlushDataSz
	s.MemSz += o.MemSz

	s.NumPagesSwapOut += o.NumPagesSwapOut
	s.NumPagesSwapIn += o.NumPagesSwapIn
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
		"flushdata_size    = %d\n"+
		"memory_size       = %d\n"+
		"num_pages_swapout = %d\n"+
		"num_pages_swapin  = %d\n",
		s.Inserts-s.Deletes,
		s.Compacts, s.Splits, s.Merges,
		s.Inserts, s.Deletes, s.CompactConflicts,
		s.SplitConflicts, s.MergeConflicts,
		s.InsertConflicts, s.DeleteConflicts,
		s.FlushDataSz, s.MemSz, s.NumPagesSwapOut,
		s.NumPagesSwapIn)
}

type Config struct {
	MaxDeltaChainLen int
	MaxPageItems     int
	MinPageItems     int
	Compare          skiplist.CompareFn
	ItemSize         ItemSizeFn

	MaxSize             int64
	File                string
	FlushBufferSize     int
	NumPersistorThreads int
	NumEvictorThreads   int

	LSSCleanerThreshold int
	AutoLSSCleaning     bool
	AutoSwapper         bool

	shouldSwap func() bool

	// TODO: Remove later
	MaxMemoryUsage int
}

func applyConfigDefaults(cfg Config) Config {
	if cfg.NumPersistorThreads == 0 {
		cfg.NumPersistorThreads = runtime.NumCPU()
	}

	if cfg.NumEvictorThreads == 0 {
		cfg.NumEvictorThreads = runtime.NumCPU()
	}

	// TODO: Remove later
	if cfg.shouldSwap == nil && cfg.MaxMemoryUsage > 0 {
		cfg.shouldSwap = func() bool {
			return ProcessRSS() >= int(0.7*float32(cfg.MaxMemoryUsage))
		}
	}
	return cfg
}

func New(cfg Config) (*Plasma, error) {
	sl := skiplist.New()
	s := &Plasma{
		Config:   applyConfigDefaults(cfg),
		Skiplist: sl,
	}

	ptWr := s.NewWriter()
	s.pageTable = newPageTable(sl, cfg.ItemSize, cfg.Compare, ptWr.wCtx.sts)

	lss, err := newLSStore(cfg.File, cfg.MaxSize, cfg.FlushBufferSize, 2)
	if err != nil {
		return nil, err
	}

	s.lss = lss
	s.doInit()
	err = s.doRecovery()

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

	return s, err
}

func (s *Plasma) doInit() {
	pid := s.StartPageId()
	pg := newSeedPage()
	s.CreateMapping(pid, pg)
}

func (s *Plasma) doRecovery() error {
	pg := &page{
		storeCtx: s.storeCtx,
	}

	w := s.NewWriter()

	var rmPg *page
	var splitKey unsafe.Pointer
	doSplit := false
	doRmPage := false
	doMerge := false
	rmFdSz := 0

	buf := w.wCtx.GetBuffer(0)

	fn := func(offset lssOffset, bs []byte) (bool, error) {
		typ := getLSSBlockType(bs)
		switch typ {
		case lssDiscard:
		case lssPageSplit:
			doSplit = true
			splitKey = unmarshalPageSMO(pg, bs[lssBlockTypeSize:])
			break
		case lssPageMerge:
			doRmPage = true
		case lssPageData, lssPageReloc:
			pg.Unmarshal(bs[lssBlockTypeSize:], w.wCtx)
			flushDataSz := len(bs)
			w.sts.FlushDataSz += int64(flushDataSz)

			var pid PageId
			if pg.low == skiplist.MinItem {
				pid = s.StartPageId()
			} else {
				pid = s.getPageId(pg.low, w.wCtx)
			}

			if pid == nil {
				pid = s.AllocPageId()
				s.CreateMapping(pid, pg)
				s.indexPage(pid, w.wCtx)
			} else {
				currPg, err := s.ReadPage(pid, w.wCtx.pgRdrFn, true)
				if err != nil {
					return false, err
				}
				// If same version, do prepend, otherwise replace.
				if currPg.GetVersion() == pg.GetVersion() || pg.IsEmpty() {
					pg.Append(currPg)
				} else {
					// Replace happens with a flushed page
					// Hence, no need to keep fdsize in basepage
					w.sts.FlushDataSz -= int64(currPg.GetFlushDataSize())
				}
			}

			if doSplit {
				splitPid := s.AllocPageId()
				newPg := pg.doSplit(splitKey, splitPid, -1)
				s.CreateMapping(splitPid, newPg)
				s.indexPage(splitPid, w.wCtx)
				w.wCtx.sts.Splits++
				doSplit = false
			} else if doRmPage {
				rmPg = new(page)
				*rmPg = *pg
				rmFdSz = flushDataSz
				doRmPage = false
				doMerge = true
				s.unindexPage(pid, w.wCtx)
			} else if doMerge {
				doMerge = false
				pg.Merge(rmPg)
				flushDataSz += rmFdSz
				w.wCtx.sts.Merges++
				rmPg = nil
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

	// TODO: Cleanup and fix error handling, rightsibling
	// Fix rightSibling node pointers
	// If crash occurs and we miss out some pages which are not persisted,
	// some ranges became orphans. Hence we need to fix hiItms for the pages
	itr := s.Skiplist.NewIterator(s.cmp, w.buf)
	defer itr.Close()
	p, _ := s.ReadPage(s.StartPageId(), w.wCtx.pgRdrFn, true)
	pg = p.(*page)
	if pg != nil && pg.head != nil {
		itms, _ := pg.collectItems(pg.head, nil, pg.head.hiItm)
		w.wCtx.sts.Inserts += int64(len(itms))
	}

	for itr.SeekFirst(); itr.Valid(); itr.Next() {
		n := itr.GetNode()
		pid := PageId(n)
		pg.head.rightSibling = pid
		pg.head.hiItm = n.Item()
		p, _ := s.ReadPage(pid, w.wCtx.pgRdrFn, true)
		pg = p.(*page)

		// TODO: Avoid need for full iteration for counting
		itms, _ := pg.collectItems(pg.head, nil, pg.head.hiItm)
		w.wCtx.sts.Inserts += int64(len(itms))
	}

	if pg != nil && pg.head != nil {
		pg.head.hiItm = skiplist.MaxItem
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

	s.lss.Close()
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

func (s *Plasma) GetStats() Stats {
	var sts Stats

	s.RLock()
	defer s.RUnlock()

	for _, w := range s.wlist {
		sts.Merge(w.sts)
	}

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
	s.Skiplist.Insert4(n, s.cmp, s.cmp, ctx.buf, n.Level(), false, ctx.slSts)
}

func (s *Plasma) unindexPage(pid PageId, ctx *wCtx) {
	n := pid.(*skiplist.Node)
	s.Skiplist.DeleteNode(n, s.cmp, ctx.buf, ctx.slSts)
}

func (s *Plasma) tryPageRemoval(pid PageId, pg Page, ctx *wCtx) {
	n := pid.(*skiplist.Node)
	itm := n.Item()
	prev, _, found := s.Skiplist.Lookup(itm, s.cmp, ctx.buf, ctx.slSts)
	// Somebody else removed the node
	if !found {
		return
	}

	shouldPersist := true

	pPid := PageId(prev)
	pPg, err := s.ReadPage(pPid, ctx.pgRdrFn, true)
	if err != nil {
		s.logError(fmt.Sprintf("tryPageRemove: err=%v", err))
		return
	}

	var rmPgBuf = ctx.GetBuffer(0)
	var pgBuf = ctx.GetBuffer(1)
	var metaBuf = ctx.GetBuffer(2)
	var fdSz, rmFdSz int

	if shouldPersist {
		rmPgBuf, fdSz = pg.Marshal(rmPgBuf)
		pgBuf, rmFdSz = pPg.Marshal(pgBuf)
		metaBuf = marshalPageSMO(pg, metaBuf)
		// Merge flushDataSize info of dead page to parent
		fdSz += rmFdSz
	}

	pPg.Merge(pg)

	var offsets []lssOffset
	var wbufs [][]byte
	var res lssResource

	if shouldPersist {
		sizes := []int{
			lssBlockTypeSize + len(metaBuf),
			lssBlockTypeSize + len(rmPgBuf),
			lssBlockTypeSize + len(pgBuf),
		}

		offsets, wbufs, res = s.lss.ReserveSpaceMulti(sizes)
		pPg.AddFlushRecord(offsets[0], fdSz, false)

		writeLSSBlock(wbufs[0], lssPageMerge, metaBuf)
		writeLSSBlock(wbufs[1], lssPageData, rmPgBuf)
		writeLSSBlock(wbufs[2], lssPageData, pgBuf)
	}

	if s.UpdateMapping(pPid, pPg) {
		s.unindexPage(pid, ctx)

		if shouldPersist {

			ctx.sts.FlushDataSz += int64(fdSz)
			s.lss.FinalizeWrite(res)
		}

	} else if shouldPersist {
		discardLSSBlock(wbufs[0])
		discardLSSBlock(wbufs[1])
		discardLSSBlock(wbufs[2])
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
		} else {
			ctx.sts.CompactConflicts++
		}
	} else if pg.NeedSplit(s.Config.MaxPageItems) {
		splitPid := s.AllocPageId()
		shouldPersist := true

		var fdSz int
		var pgBuf = ctx.GetBuffer(0)
		var splitMetaBuf = ctx.GetBuffer(1)

		if shouldPersist {
			pgBuf, fdSz = pg.Marshal(pgBuf)
		}

		newPg := pg.Split(splitPid)

		// Skip split, but compact
		if newPg == nil {
			s.FreePageId(splitPid)
			staleFdSz := pg.Compact()
			if updated = s.UpdateMapping(pid, pg); updated {
				ctx.sts.FlushDataSz -= int64(staleFdSz)
			}
			return updated
		}

		var offsets []lssOffset
		var wbufs [][]byte
		var res lssResource

		// Commit split information in lss
		if shouldPersist {
			splitMetaBuf = marshalPageSMO(newPg, splitMetaBuf)
			sizes := []int{
				lssBlockTypeSize + len(splitMetaBuf),
				lssBlockTypeSize + len(pgBuf),
			}

			offsets, wbufs, res = s.lss.ReserveSpaceMulti(sizes)
			pg.AddFlushRecord(offsets[0], fdSz, false)
			writeLSSBlock(wbufs[0], lssPageSplit, splitMetaBuf)
			writeLSSBlock(wbufs[1], lssPageData, pgBuf)
		}

		if s.UpdateMapping(pid, pg) {
			s.CreateMapping(splitPid, newPg)
			s.indexPage(splitPid, ctx)
			ctx.sts.Splits++

			if shouldPersist {
				ctx.sts.FlushDataSz += int64(fdSz)
				s.lss.FinalizeWrite(res)
			}
		} else {
			ctx.sts.SplitConflicts++
			s.FreePageId(splitPid)

			if shouldPersist {
				discardLSSBlock(wbufs[0])
				discardLSSBlock(wbufs[1])
				s.lss.FinalizeWrite(res)
			}
		}

	} else if !s.isStartPage(pid) && pg.NeedMerge(s.Config.MinPageItems) {
		pg.Close()
		if s.UpdateMapping(pid, pg) {
			s.tryPageRemoval(pid, pg, ctx)
			ctx.sts.Merges++
			updated = true
		} else {
			ctx.sts.MergeConflicts++
		}
	} else if doUpdate {
		updated = s.UpdateMapping(pid, pg)
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
	w.sts.Inserts++

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
	w.sts.Deletes++

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
		case lssPageSplit:
			splitKey := unmarshalPageSMO(pg, data[lssBlockTypeSize:])
			dataOffset := lssBlockEndOffset(offset, data[:l])
			if dataPg, err := s.fetchPageFromLSS(dataOffset, ctx); err == nil {
				dataPg.head = dataPg.newSplitPageDelta(splitKey, nil)
				dataPg.AddFlushRecord(offset, 0, false)
				pg.Append(dataPg)
			} else {
				return nil, err
			}
			break loop
		case lssPageMerge:
			mergeKey := unmarshalPageSMO(pg, data[lssBlockTypeSize:])
			rmPgDataOffset := lssBlockEndOffset(offset, data[:l])
			if rmPg, err := s.fetchPageFromLSS(rmPgDataOffset, ctx); err == nil {
				l, err := s.lss.Read(rmPgDataOffset, data)
				if err != nil {
					return nil, err
				}

				dataOffset := lssBlockEndOffset(rmPgDataOffset, data[:l])
				if dataPg, err := s.fetchPageFromLSS(dataOffset, ctx); err == nil {
					dataPg.head = dataPg.newMergePageDelta(mergeKey, rmPg.head)
					dataPg.AddFlushRecord(offset, 0, false)
					pg.Append(dataPg)
				} else {
					return nil, err
				}
			} else {
				return nil, err
			}
			break loop
		case lssPageData:
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
