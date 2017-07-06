// Copyright (c) 2017 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package plasma

import (
	"fmt"
	"github.com/couchbase/nitro/mm"
	"github.com/couchbase/nitro/skiplist"
	"github.com/golang/snappy"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

var ErrInvalidSnapshot = fmt.Errorf("Invalid plasma snapshot")

type PageReader func(LSSOffset, *wCtx, *allocCtx, *storeCtx) (*page, error)

const maxCtxBuffers = 9
const (
	bufEncPage int = iota
	bufEncMeta
	bufTempItem
	bufReloc
	bufCleaner
	bufRecovery
	bufFetch
	bufPersist
	bufDecompress
)

const recoverySMRInterval = 100

var (
	memQuota       int64
	maxMemoryQuota = int64(1024 * 1024 * 1024 * 1024)
	dbInstances    *skiplist.Skiplist
)

func init() {
	dbInstances = skiplist.New()
	SetMemoryQuota(maxMemoryQuota)
}

type Plasma struct {
	Config
	*skiplist.Skiplist
	wlist                           []*Writer
	lss                             LSS
	lssCleanerWriter                *wCtx
	persistWriters                  []*wCtx
	evictWriters                    []*wCtx
	stoplssgc, stopswapper, stopmon chan struct{}
	sync.RWMutex

	// MVCC data structures
	itemsCount   int64
	mvcc         sync.RWMutex
	currSn       uint64
	numSnCreated int
	gcSn         uint64
	currSnapshot *Snapshot

	lastMaxSn uint64

	rpSns          unsafe.Pointer
	rpVersion      uint16
	recoveryPoints []*RecoveryPoint

	hasMemoryResPressure bool
	hasLSSResPressure    bool

	clockHandle *clockHandle
	clockLock   sync.Mutex

	smrWg   sync.WaitGroup
	smrChan chan unsafe.Pointer

	*storeCtx

	wCtxLock sync.Mutex
	wCtxList *wCtx
	gCtx     *wCtx

	diagWr *Writer

	holePunch bool

	logPrefix string
	logger    Logger
}

func (s *Plasma) SetLogPrefix(prefix string) {
	s.logPrefix = prefix
}

func (s *Plasma) SetLogger(l Logger) {
	s.logger = l
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

	MemSz      int64
	MemSzIndex int64

	AllocSz   int64
	FreeSz    int64
	ReclaimSz int64

	NumRecordAllocs  int64
	NumRecordFrees   int64
	NumRecordSwapOut int64
	NumRecordSwapIn  int64
	AllocSzIndex     int64
	FreeSzIndex      int64
	ReclaimSzIndex   int64

	NumPages int64

	LSSFrag      int
	LSSDataSize  int64
	LSSUsedSpace int64
	NumLSSReads  int64
	LSSReadBytes int64

	NumLSSCleanerReads  int64
	LSSCleanerReadBytes int64

	CacheHits   int64
	CacheMisses int64

	ReaderCacheHits    int64
	ReaderCacheMisses  int64
	ReaderNumLSSReads  int64
	ReaderLSSReadBytes int64

	WriteAmp            float64
	WriteAmpAvg         float64
	CacheHitRatio       float64
	ReaderCacheHitRatio float64
	ResidentRatio       float64
	HolePunch           bool

	LSSThrottled    bool
	MemoryThrottled bool
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

	s.AllocSz += o.AllocSz
	s.FreeSz += o.FreeSz
	s.ReclaimSz += o.ReclaimSz

	s.AllocSzIndex += o.AllocSzIndex
	s.FreeSzIndex += o.FreeSzIndex
	o.ReclaimSzIndex += o.ReclaimSzIndex

	s.NumRecordAllocs += o.NumRecordAllocs
	s.NumRecordFrees += o.NumRecordFrees
	s.NumRecordSwapOut += o.NumRecordSwapOut
	s.NumRecordSwapIn += o.NumRecordSwapIn

	s.BytesIncoming += o.BytesIncoming

	s.NumLSSReads += o.NumLSSReads
	s.LSSReadBytes += o.LSSReadBytes

	s.CacheHits += o.CacheHits
	s.CacheMisses += o.CacheMisses
}

func (s Stats) String() string {
	return fmt.Sprintf("{\n"+
		"\"memory_quota\":         %d,\n"+
		"\"punch_hole_support\":   %v,\n"+
		"\"count\":                %d,\n"+
		"\"compacts\":             %d,\n"+
		"\"splits\":               %d,\n"+
		"\"merges\":               %d,\n"+
		"\"inserts\":              %d,\n"+
		"\"deletes\":              %d,\n"+
		"\"compact_conflicts\":    %d,\n"+
		"\"split_conflicts\":      %d,\n"+
		"\"merge_conflicts\":      %d,\n"+
		"\"insert_conflicts\":     %d,\n"+
		"\"delete_conflicts\":     %d,\n"+
		"\"swapin_conflicts\":     %d,\n"+
		"\"memory_size\":          %d,\n"+
		"\"memory_size_index\":    %d,\n"+
		"\"allocated\":            %d,\n"+
		"\"freed\":                %d,\n"+
		"\"reclaimed\":            %d,\n"+
		"\"reclaim_pending\":      %d,\n"+
		"\"allocated_index\":      %d,\n"+
		"\"freed_index\":          %d,\n"+
		"\"reclaimed_index\":      %d,\n"+
		"\"num_pages\":            %d,\n"+
		"\"num_rec_allocs\":       %d,\n"+
		"\"num_rec_frees\":        %d,\n"+
		"\"num_rec_swapout\":      %d,\n"+
		"\"num_rec_swapin\":       %d,\n"+
		"\"bytes_incoming\":       %d,\n"+
		"\"bytes_written\":        %d,\n"+
		"\"write_amp\":            %.2f,\n"+
		"\"write_amp_avg\":        %.2f,\n"+
		"\"lss_fragmentation\":    %d,\n"+
		"\"lss_data_size\":        %d,\n"+
		"\"lss_used_space\":       %d,\n"+
		"\"lss_num_reads\":        %d,\n"+
		"\"lss_read_bs\":          %d,\n"+
		"\"lss_gc_num_reads\":     %d,\n"+
		"\"lss_gc_reads_bs\":      %d,\n"+
		"\"cache_hits\":           %d,\n"+
		"\"cache_misses\":         %d,\n"+
		"\"cache_hit_ratio\":      %.5f,\n"+
		"\"rcache_hits\":          %d,\n"+
		"\"rcache_misses\":        %d,\n"+
		"\"rcache_hit_ratio\":     %.5f,\n"+
		"\"resident_ratio\":       %.5f,\n"+
		"\"mem_throttled\":        %v,\n"+
		"\"lss_throttled\":        %v\n}",
		atomic.LoadInt64(&memQuota),
		s.HolePunch,
		s.Inserts-s.Deletes,
		s.Compacts, s.Splits, s.Merges,
		s.Inserts, s.Deletes, s.CompactConflicts,
		s.SplitConflicts, s.MergeConflicts,
		s.InsertConflicts, s.DeleteConflicts,
		s.SwapInConflicts, s.MemSz, s.MemSzIndex,
		s.AllocSz, s.FreeSz, s.ReclaimSz,
		s.FreeSz-s.ReclaimSz,
		s.AllocSzIndex, s.FreeSzIndex, s.ReclaimSzIndex,
		s.NumPages, s.NumRecordAllocs, s.NumRecordFrees,
		s.NumRecordSwapOut, s.NumRecordSwapIn,
		s.BytesIncoming, s.BytesWritten,
		s.WriteAmp, s.WriteAmpAvg,
		s.LSSFrag, s.LSSDataSize, s.LSSUsedSpace,
		s.NumLSSReads, s.LSSReadBytes,
		s.NumLSSCleanerReads, s.LSSCleanerReadBytes,
		s.CacheHits, s.CacheMisses, s.CacheHitRatio,
		s.ReaderCacheHits, s.ReaderCacheMisses, s.ReaderCacheHitRatio,
		s.ResidentRatio, s.MemoryThrottled, s.LSSThrottled)
}

func New(cfg Config) (*Plasma, error) {
	var err error

	cfg = applyConfigDefaults(cfg)

	s := &Plasma{
		Config:      cfg,
		stopmon:     make(chan struct{}),
		stoplssgc:   make(chan struct{}),
		stopswapper: make(chan struct{}),
		logger:      &defaultLogger{},
	}

	slCfg := skiplist.DefaultConfig()
	if cfg.UseMemoryMgmt {
		s.smrChan = make(chan unsafe.Pointer, smrChanBufSize)
		slCfg.UseMemoryMgmt = true
		slCfg.Malloc = mm.Malloc
		slCfg.Free = mm.Free
		slCfg.BarrierDestructor = s.newBSDestroyCallback()
	}

	sl := skiplist.NewWithConfig(slCfg)
	s.Skiplist = sl

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

	s.storeCtx = newStoreContext(sl, cfg, cfGetter, lfGetter)

	s.gCtx = s.newWCtx()
	if s.useMemMgmt {
		s.smrWg.Add(1)
		go s.smrWorker(s.gCtx)
	}

	sbuf := dbInstances.MakeBuf()
	defer dbInstances.FreeBuf(sbuf)
	dbInstances.Insert(unsafe.Pointer(s), ComparePlasma, sbuf, &dbInstances.Stats)

	if s.shouldPersist {
		os.MkdirAll(cfg.File, 0755)
		s.holePunch = isHolePunchSupported(cfg.File)
		commitDur := time.Duration(cfg.SyncInterval) * time.Second
		s.lss, err = NewLSStore(cfg.File, cfg.LSSLogSegmentSize, cfg.FlushBufferSize, 2, cfg.UseMmap, s.holePunch, commitDur)
		if err != nil {
			return nil, err
		}

		s.lss.SetSafeTrimCallback(s.findSafeLSSTrimOffset)
		s.initLRUClock()
		err = s.doRecovery()
	}

	s.doInit()

	if s.shouldPersist {
		s.persistWriters = make([]*wCtx, runtime.NumCPU())
		s.evictWriters = make([]*wCtx, runtime.NumCPU())
		for i, _ := range s.persistWriters {
			s.persistWriters[i] = s.newWCtx()
			s.evictWriters[i] = s.newWCtx()
		}
		s.lssCleanerWriter = s.newWCtx()

		if cfg.AutoLSSCleaning {
			go s.lssCleanerDaemon()
		}

		if cfg.AutoSwapper {
			go s.swapperDaemon()
		}
	}

	if s.shouldPersist {
		go s.monitorMemUsage()
	}

	go s.runtimeStats()
	return s, err
}

func (s *Plasma) runtimeStats() {
	so := s.GetStats()
	for {
		select {
		case <-s.stopmon:
			return
		default:
		}

		time.Sleep(time.Second * 5)

		now := s.GetStats()
		bsOut := (float64(now.BytesWritten) - float64(so.BytesWritten))
		bsIn := (float64(now.BytesIncoming) - float64(so.BytesIncoming))
		if bsIn > 0 {
			s.gCtx.sts.WriteAmp = bsOut / bsIn
		}

		hits := now.CacheHits - so.CacheHits
		miss := now.CacheMisses - so.CacheMisses

		if tot := float64(hits + miss); tot > 0 {
			s.gCtx.sts.CacheHitRatio = float64(hits) / tot
		}

		rdrHits := now.ReaderCacheHits - so.ReaderCacheHits
		rdrMiss := now.ReaderCacheMisses - so.ReaderCacheMisses
		if tot := float64(rdrHits + rdrMiss); tot > 0 {
			s.gCtx.sts.ReaderCacheHitRatio = float64(rdrHits) / tot
		}
		so = now

		if now.NumRecordAllocs > 0 && s.hasMemoryResPressure &&
			now.NumRecordAllocs-now.NumRecordFrees <= 0 {
			s.logInfo(fmt.Sprintf("Warning: not enough memory to hold records in memory. Stats:%s\n", now.String()))
		}
	}
}

func (s *Plasma) monitorMemUsage() {
	sctx := s.newWCtx2().SwapperContext()

	for {
		select {
		case <-s.stopmon:
			return
		default:
		}

		// Avoid unnecessary cache line invalidation ?
		if v := s.TriggerSwapper(sctx); v != s.hasMemoryResPressure {
			s.hasMemoryResPressure = v
		}

		if v := s.TriggerLSSCleaner(s.Config.LSSCleanerMaxThreshold,
			s.Config.LSSCleanerThrottleMinSize); v != s.hasLSSResPressure {
			s.hasLSSResPressure = v
		}

		time.Sleep(time.Millisecond * 100)
	}
}

func (s *Plasma) doInit() {
	// Init seed page if page-0 does not exist even after recovery
	if s.Skiplist.GetStats().NodeCount == 0 {
		pid := s.StartPageId()
		if pid.(*skiplist.Node).Link == nil {
			pg := s.newSeedPage(s.gCtx)
			s.CreateMapping(pid, pg, s.gCtx)
		}
	}

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
	pg := newPage(s.gCtx, nil, nil).(*page)

	buf := s.gCtx.GetBuffer(bufRecovery)

	fn := func(offset LSSOffset, bs []byte) (bool, error) {
		typ := getLSSBlockType(bs)
		bs = bs[lssBlockTypeSize:]
		switch typ {
		case lssDiscard:
		case lssRecoveryPoints:
			s.rpVersion, s.recoveryPoints = unmarshalRPs(bs)
		case lssMaxSn:
			s.currSn = decodeMaxSn(bs)
		case lssPageRemove:
			rmPglow := getRmPageLow(bs)
			pid := s.getPageId(rmPglow, s.gCtx)
			if pid != nil {
				currPg, err := s.ReadPage(pid, false, s.gCtx)
				if err != nil {
					return false, err
				}

				// TODO: Store precomputed fdSize in swapout delta
				s.gCtx.sts.FlushDataSz -= int64(currPg.GetFlushDataSize())
				currPg.(*page).free(false)
				s.unindexPage(pid, s.gCtx)
			}
		case lssPageData, lssPageReloc, lssPageUpdate:
			pg.Unmarshal(bs, s.gCtx)
			flushDataSz := len(bs)

			newPageData := (typ == lssPageData || typ == lssPageReloc)
			if pid := s.getPageId(pg.low, s.gCtx); pid == nil {
				if newPageData {
					s.gCtx.sts.FlushDataSz += int64(flushDataSz)
					pg.AddFlushRecord(offset, flushDataSz, 1)
					pid = s.AllocPageId(s.gCtx)
					s.CreateMapping(pid, pg, s.gCtx)
					s.indexPage(pid, s.gCtx)
				} else {
					pg.free(false)
				}
			} else {
				s.gCtx.sts.FlushDataSz += int64(flushDataSz)

				currPg, err := s.ReadPage(pid, false, s.gCtx)
				if err != nil {
					return false, err
				}

				if newPageData {
					s.gCtx.sts.FlushDataSz -= int64(currPg.GetFlushDataSize())
					currPg.(*page).free(false)
					pg.AddFlushRecord(offset, flushDataSz, 1)
				} else {
					_, numSegments, _ := currPg.GetFlushInfo()
					pg.Append(currPg)
					pg.AddFlushRecord(offset, flushDataSz, numSegments+1)
				}

				pg.prevHeadPtr = currPg.(*page).prevHeadPtr
				s.UpdateMapping(pid, pg, s.gCtx)
			}
		}

		pg.Reset()
		err := s.tryEvictPages(s.gCtx)
		if err != nil {
			return false, err
		}
		s.trySMRObjects(s.gCtx, recoverySMRInterval)
		return true, nil
	}

	err := s.lss.Visitor(fn, buf)
	if err != nil {
		return err
	}

	s.trySMRObjects(s.gCtx, 0)

	// Initialize rightSiblings for all pages
	var lastPg Page
	callb := func(pid PageId, partn RangePartition) error {
		pg, err := s.ReadPage(pid, false, s.gCtx)
		if lastPg != nil {
			if err == nil && s.cmp(lastPg.MaxItem(), pg.MinItem()) != 0 {
				panic("found missing page")
			}

			lastPg.SetNext(pid)
		}

		lastPg = pg
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
	if s.EnableShapshots {
		// Force SMR flush
		s.NewSnapshot().Close()
	}

	close(s.stopmon)
	if s.Config.AutoLSSCleaning {
		s.stoplssgc <- struct{}{}
		<-s.stoplssgc
	}

	if s.Config.AutoSwapper {
		s.stopswapper <- struct{}{}
		<-s.stopswapper
	}

	if s.Config.shouldPersist {
		s.PersistAll()
		s.lss.Close()
	}

	sbuf := dbInstances.MakeBuf()
	defer dbInstances.FreeBuf(sbuf)
	dbInstances.Delete(unsafe.Pointer(s), ComparePlasma, sbuf, &dbInstances.Stats)

	if s.useMemMgmt {
		close(s.smrChan)
		s.smrWg.Wait()
		s.destroyAllObjects()
	}
}

func ComparePlasma(a, b unsafe.Pointer) int {
	return int(uintptr(a)) - int(uintptr(b))
}

type Writer struct {
	*wCtx
	count int64
}

type Reader struct {
	iter *MVCCIterator
}

type workerType int

const (
	genericWorker workerType = iota
	writerWorker
	readerWorker
)

// TODO: Refactor wCtx and Writer
type wCtx struct {
	*Plasma
	buf       *skiplist.ActionBuffer
	pgBuffers []*Buffer
	slSts     *skiplist.Stats
	sts       *Stats
	dbIter    *skiplist.Iterator

	pageReader PageReader

	pgAllocCtx *allocCtx

	reclaimList []reclaimObject

	next *wCtx

	safeOffset LSSOffset

	typ workerType
}

func (ctx *wCtx) SetWorkerType(t workerType) {
	ctx.typ = t
}

func (ctx *wCtx) GetWorkerType() workerType {
	return ctx.typ
}

func (ctx *wCtx) compress(data []byte, buf *Buffer) []byte {
	if ctx.Config.UseCompression {
		l := len(data)
		encLen := snappy.MaxEncodedLen(l)
		encBuf := buf.Get(l, encLen)
		data = snappy.Encode(encBuf, data)
	}

	return data
}

func (ctx *wCtx) decompress(data []byte, buf *Buffer) []byte {
	var err error
	if ctx.Config.UseCompression {
		decLen, _ := snappy.DecodedLen(data)
		bs := buf.Get(0, decLen)
		data, err = snappy.Decode(bs, data)
		if err != nil {
			panic(err)
		}
	}

	return data
}

func (ctx *wCtx) freePages(pages []pgFreeObj) {
	for _, pg := range pages {
		var hiItm unsafe.Pointer
		if pg.evicted {
			hiItm = pg.h.hiItm
		}

		nr, mr, size := computeMemUsed(pg.h, ctx.itemSize, ctx.cmp, hiItm)
		ctx.sts.FreeSz += int64(size)

		ctx.sts.NumRecordFrees += int64(nr)
		if pg.evicted {
			ctx.sts.NumRecordSwapOut += int64(mr)
		}

		if ctx.useMemMgmt {
			o := reclaimObject{typ: smrPage, size: uint32(size),
				ptr: unsafe.Pointer(pg.h)}
			ctx.reclaimList = append(ctx.reclaimList, o)
		}
	}
}

func (ctx *wCtx) SwapperContext() SwapperContext {
	return ctx.dbIter
}

func (s *Plasma) newWCtx() *wCtx {
	s.wCtxLock.Lock()
	defer s.wCtxLock.Unlock()

	ctx := s.newWCtx2()
	s.wCtxList = ctx
	return ctx
}

func (s *Plasma) newWCtx2() *wCtx {
	ctx := &wCtx{
		Plasma:     s,
		pgAllocCtx: new(allocCtx),
		buf:        s.Skiplist.MakeBuf(),
		slSts:      &s.Skiplist.Stats,
		sts:        new(Stats),
		pgBuffers:  make([]*Buffer, maxCtxBuffers),
		next:       s.wCtxList,
		safeOffset: expiredLSSOffset,
	}

	ctx.dbIter = dbInstances.NewIterator(ComparePlasma, ctx.buf)
	ctx.pageReader = s.fetchPageFromLSS

	return ctx
}

func (ctx *wCtx) GetBuffer(id int) *Buffer {
	if ctx.pgBuffers[id] == nil {
		ctx.pgBuffers[id] = newBuffer(maxPageEncodedSize)
	}

	return ctx.pgBuffers[id]
}

func (s *Plasma) NewWriter() *Writer {

	w := &Writer{
		wCtx: s.newWCtx(),
	}
	w.SetWorkerType(writerWorker)

	s.Lock()
	defer s.Unlock()

	s.wlist = append(s.wlist, w)
	if s.useMemMgmt {
		s.smrWg.Add(1)
		go s.smrWorker(w.wCtx)
	}

	return w
}

func (s *Plasma) NewReader() *Reader {
	iter := s.NewIterator().(*Iterator)
	iter.filter = &snFilter{}
	iter.SetWorkerType(readerWorker)

	return &Reader{
		iter: &MVCCIterator{
			Iterator: iter,
		},
	}
}

func (r *Reader) NewSnapshotIterator(snap *Snapshot) (*MVCCIterator, error) {
	if snap.db != r.iter.store {
		return nil, ErrInvalidSnapshot
	}

	snap.Open()
	r.iter.filter.(*snFilter).sn = snap.sn
	r.iter.filter.(*snFilter).lastItmPtr = nil
	r.iter.token = r.iter.BeginTx()
	r.iter.snap = snap
	return r.iter, nil
}

func (s *Plasma) MemoryInUse() int64 {
	var memSz int64
	for w := s.wCtxList; w != nil; w = w.next {
		memSz += w.sts.AllocSz - w.sts.FreeSz
		memSz += w.sts.AllocSzIndex - w.sts.FreeSzIndex
	}

	return memSz
}

func (s *Plasma) GetStats() Stats {
	var sts, rdrSts Stats

	sts.HolePunch = s.holePunch
	sts.MemoryThrottled = s.hasMemoryResPressure
	sts.LSSThrottled = s.hasLSSResPressure
	sts.NumPages = int64(s.Skiplist.GetStats().NodeCount + 1)
	for w := s.wCtxList; w != nil; w = w.next {
		sts.Merge(w.sts)
		if w.GetWorkerType() == readerWorker {
			rdrSts.Merge(w.sts)
		}
	}

	sts.MemSz = sts.AllocSz - sts.FreeSz
	sts.MemSzIndex = sts.AllocSzIndex - sts.FreeSzIndex
	if s.shouldPersist {
		sts.BytesWritten = s.lss.BytesWritten()
		sts.LSSFrag, sts.LSSDataSize, sts.LSSUsedSpace = s.GetLSSInfo()
		sts.NumLSSCleanerReads = s.lssCleanerWriter.sts.NumLSSReads
		sts.LSSCleanerReadBytes = s.lssCleanerWriter.sts.LSSReadBytes
		sts.ReaderLSSReadBytes = rdrSts.LSSReadBytes
		sts.ReaderNumLSSReads = rdrSts.NumLSSReads
		sts.ReaderCacheHits = rdrSts.CacheHits
		sts.ReaderCacheMisses = rdrSts.CacheMisses

		sts.CacheHitRatio = s.gCtx.sts.CacheHitRatio
		sts.ReaderCacheHitRatio = s.gCtx.sts.ReaderCacheHitRatio

		sts.WriteAmp = s.gCtx.sts.WriteAmp
		bsOut := float64(sts.BytesWritten)
		bsIn := float64(sts.BytesIncoming)
		if bsIn > 0 {
			sts.WriteAmpAvg = bsOut / bsIn
		}
		cachedRecs := sts.NumRecordAllocs - sts.NumRecordFrees
		lssRecs := sts.NumRecordSwapOut - sts.NumRecordSwapIn
		totalRecs := cachedRecs + lssRecs
		if totalRecs > 0 {
			sts.ResidentRatio = float64(cachedRecs) / float64(totalRecs)
		}
	}
	return sts
}

func (s *Plasma) LSSDataSize() int64 {
	var sz int64

	for w := s.wCtxList; w != nil; w = w.next {
		sz += w.sts.FlushDataSz
	}

	return sz
}

func (s *Plasma) indexPage(pid PageId, ctx *wCtx) {
	n := pid.(*skiplist.Node)
	if n.Item() == skiplist.MinItem {
		link := n.Link
		s.FreePageId(pid, ctx)
		n = s.StartPageId().(*skiplist.Node)
		n.Link = link
		return
	}
retry:
	if existNode, ok := s.Skiplist.Insert4(n, s.cmp, s.cmp, ctx.buf, n.Level(), false, false, ctx.slSts); !ok {
		if pg := newPage(ctx, nil, existNode.Link); pg.NeedRemoval() {
			runtime.Gosched()
			goto retry
		}
		panic("duplicate index node")
	}

	ctx.sts.AllocSzIndex += int64(s.itemSize(n.Item()) + uintptr(n.Size()))
}

func (s *Plasma) unindexPage(pid PageId, ctx *wCtx) {
	n := pid.(*skiplist.Node)
	s.Skiplist.DeleteNode2(n, s.cmp, ctx.buf, ctx.slSts)
	size := int64(s.itemSize(n.Item()) + uintptr(n.Size()))
	ctx.sts.FreeSzIndex += size

	if s.useMemMgmt {
		o := reclaimObject{typ: smrPageId, size: uint32(size), ptr: unsafe.Pointer(n)}
		ctx.reclaimList = append(ctx.reclaimList, o)
	}
}

func (s *Plasma) tryPageRemoval(pid PageId, pg Page, ctx *wCtx) {
	itm := pg.MinItem()
retry:
	parent, curr, found := s.Skiplist.Lookup(itm, s.cmp, ctx.buf, ctx.slSts)
	// Page has been removed already
	if !found || PageId(curr) != pid {
		return
	}

	pPid := PageId(parent)
	pPg, err := s.ReadPage(pPid, true, ctx)
	if err != nil {
		panic(err)
	}

	if pPg.NeedRemoval() {
		goto retry
	}

	// Parent might have got a split
	if pPg.GetNext() != pid {
		goto retry
	}

	var pgBuf = ctx.GetBuffer(bufEncPage)
	var metaBuf = ctx.GetBuffer(bufEncMeta)
	var fdSz, staleFdSz int

	var metaBS, pgBS []byte

	s.tryPageSwapin(pPg)
	pPg.Merge(pg)

	var offsets []LSSOffset
	var wbufs [][]byte
	var res LSSResource

	if s.shouldPersist {
		var numSegments int
		metaBS = marshalPageSMO(pg, metaBuf)
		pgBS, fdSz, staleFdSz, numSegments = pPg.Marshal(pgBuf, FullMarshal)

		sizes := []int{
			lssBlockTypeSize + len(metaBS),
			lssBlockTypeSize + len(pgBS),
		}

		offsets, wbufs, res = s.lss.ReserveSpaceMulti(sizes)

		writeLSSBlock(wbufs[0], lssPageRemove, metaBS)

		writeLSSBlock(wbufs[1], lssPageData, pgBS)
		pPg.AddFlushRecord(offsets[1], fdSz, numSegments)
	}

	if s.UpdateMapping(pPid, pPg, ctx) {
		s.unindexPage(pid, ctx)

		if s.shouldPersist {
			ctx.sts.FlushDataSz += int64(fdSz) - int64(staleFdSz)
			s.lss.FinalizeWrite(res)
		}

		return

	} else if s.shouldPersist {
		discardLSSBlock(wbufs[0])
		discardLSSBlock(wbufs[1])
		s.lss.FinalizeWrite(res)
	}

	goto retry
}

func (s *Plasma) isMergablePage(pid PageId, ctx *wCtx) bool {
	n := pid.(*skiplist.Node)
	if n == s.Skiplist.HeadNode() {
		return false
	}

	// Make sure that the page is visible in the index layer
	_, curr, ok := s.Skiplist.Lookup(n.Item(), s.cmp, ctx.buf, ctx.slSts)
	return ok && curr == n
}

func (s *Plasma) StartPageId() PageId {
	return s.Skiplist.HeadNode()
}

func (s *Plasma) EndPageId() PageId {
	return s.Skiplist.TailNode()
}

func (s *Plasma) trySMOs(pid PageId, pg Page, ctx *wCtx, doUpdate bool) bool {
	return s.trySMOs2(pid, pg, ctx, doUpdate, s.Config.MaxPageItems,
		s.Config.MinPageItems, s.Config.MaxDeltaChainLen, s.Config.MaxPageLSSSegments)
}

func (s *Plasma) trySMOs2(pid PageId, pg Page, ctx *wCtx, doUpdate bool,
	maxPageItems, minPageItems, maxDeltaChainLen, maxPageLSSSegments int) bool {

	var updated bool

	if pg.NeedCompaction(maxDeltaChainLen) {
		staleFdSz := pg.Compact()
		if updated = s.UpdateMapping(pid, pg, ctx); updated {
			ctx.sts.Compacts++
			ctx.sts.FlushDataSz -= int64(staleFdSz)
		} else {
			ctx.sts.CompactConflicts++
		}
	} else if pg.NeedSplit(maxPageItems) {
		splitPid := s.AllocPageId(ctx)

		var fdSz, splitFdSz, staleFdSz, numSegments, numSegmentsSplit int
		var pgBuf = ctx.GetBuffer(bufEncPage)
		var splitPgBuf = ctx.GetBuffer(bufEncMeta)
		var pgBS, splitPgBS []byte

		newPg := pg.Split(splitPid)

		// Skip split, but compact
		if newPg == nil {
			s.FreePageId(splitPid, ctx)
			staleFdSz := pg.Compact()
			if updated = s.UpdateMapping(pid, pg, ctx); updated {
				ctx.sts.FlushDataSz -= int64(staleFdSz)
			}
			return updated
		}

		var offsets []LSSOffset
		var wbufs [][]byte
		var res LSSResource

		// Replace one page with two pages
		if s.shouldPersist {
			pgBS, fdSz, staleFdSz, numSegments = pg.Marshal(pgBuf, maxPageLSSSegments)
			splitPgBS, splitFdSz, _, numSegmentsSplit = newPg.Marshal(splitPgBuf, 1)

			sizes := []int{
				lssBlockTypeSize + len(pgBS),
				lssBlockTypeSize + len(splitPgBS),
			}

			offsets, wbufs, res = s.lss.ReserveSpaceMulti(sizes)

			typ := pgFlushLSSType(pg, numSegments)
			writeLSSBlock(wbufs[0], typ, pgBS)
			pg.AddFlushRecord(offsets[0], fdSz, numSegments)

			writeLSSBlock(wbufs[1], lssPageData, splitPgBS)
			newPg.AddFlushRecord(offsets[1], splitFdSz, numSegmentsSplit)
		}

		s.CreateMapping(splitPid, newPg, ctx)
		if updated = s.UpdateMapping(pid, pg, ctx); updated {
			s.indexPage(splitPid, ctx)
			ctx.sts.Splits++

			if s.shouldPersist {
				ctx.sts.FlushDataSz += int64(fdSz) + int64(splitFdSz) - int64(staleFdSz)
				s.lss.FinalizeWrite(res)
			}
		} else {
			ctx.sts.SplitConflicts++
			s.FreePageId(splitPid, ctx)

			if s.shouldPersist {
				discardLSSBlock(wbufs[0])
				discardLSSBlock(wbufs[1])
				s.lss.FinalizeWrite(res)
			}
		}
	} else if pg.NeedMerge(minPageItems) && s.isMergablePage(pid, ctx) {
		// Closing a page makes it immutable. No writer will further append deltas
		// If it is a swapped out page, bring the page components back to memory
		s.tryPageSwapin(pg)
		pg.Close()

		if updated = s.UpdateMapping(pid, pg, ctx); updated {
			s.tryPageRemoval(pid, pg, ctx)
			ctx.sts.Merges++
		} else {
			ctx.sts.MergeConflicts++
		}
	} else if doUpdate {
		updated = s.UpdateMapping(pid, pg, ctx)
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
	if pg, err = s.ReadPage(pid, false, ctx); err != nil {
		return nil, nil, err
	}

	if !pg.InRange(itm) {
		pid = pg.GetNext()
		goto refresh
	}

	if pg.NeedRemoval() {
		s.tryPageRemoval(pid, pg, ctx)
		goto retry
	}

	s.updateCacheMeta(pid)

	return
}

func (w *Writer) Insert(itm unsafe.Pointer) error {
	w.tryThrottleForLSS()
retry:
	pid, pg, err := w.fetchPage(itm, w.wCtx)
	if err != nil {
		return err
	}

	nr := w.sts.NumLSSReads
	pg.Insert(itm)

	if !w.trySMOs(pid, pg, w.wCtx, true) {
		w.sts.InsertConflicts++
		goto retry
	}

	w.sts.BytesIncoming += int64(w.itemSize(itm))
	w.sts.Inserts++
	if w.sts.NumLSSReads-nr > 0 {
		w.sts.CacheMisses++
	} else {
		w.sts.CacheHits++
	}

	w.trySMRObjects(w.wCtx, writerSMRBufferSize)
	return nil
}

func (w *Writer) Delete(itm unsafe.Pointer) error {
	w.tryThrottleForLSS()
retry:
	pid, pg, err := w.fetchPage(itm, w.wCtx)
	if err != nil {
		return err
	}

	nr := w.sts.NumLSSReads
	pg.Delete(itm)

	if !w.trySMOs(pid, pg, w.wCtx, true) {
		w.sts.DeleteConflicts++
		goto retry
	}
	w.sts.BytesIncoming += int64(w.itemSize(itm))
	w.sts.Deletes++
	if w.sts.NumLSSReads-nr > 0 {
		w.sts.CacheMisses++
	} else {
		w.sts.CacheHits++
	}

	w.trySMRObjects(w.wCtx, writerSMRBufferSize)
	return nil
}

func (w *Writer) Lookup(itm unsafe.Pointer) (unsafe.Pointer, error) {
	pid, pg, err := w.fetchPage(itm, w.wCtx)
	if err != nil {
		return nil, err
	}

	nr := w.sts.NumLSSReads
	ret := pg.Lookup(itm)
	w.trySMOs(pid, pg, w.wCtx, false)
	if w.sts.NumLSSReads-nr > 0 {
		w.sts.CacheMisses++
	} else {
		w.sts.CacheHits++
	}

	return ret, nil
}

func (s *Plasma) fetchPageFromLSS(baseOffset LSSOffset, ctx *wCtx,
	aCtx *allocCtx, sCtx *storeCtx) (*page, error) {
	pg := newPage2(nil, nil, ctx, sCtx, aCtx).(*page)
	offset := baseOffset
	dataBuf := ctx.GetBuffer(bufFetch)
	numSegments := 0
loop:
	for {
		l, err := s.lss.Read(offset, dataBuf)
		if err != nil {
			return nil, err
		}

		ctx.sts.NumLSSReads++
		ctx.sts.LSSReadBytes += int64(l)

		data := dataBuf.Get(0, l)
		typ := getLSSBlockType(data)
		switch typ {
		case lssPageData, lssPageReloc, lssPageUpdate:
			currPgDelta := newPage2(nil, nil, ctx, sCtx, aCtx).(*page)
			data := data[lssBlockTypeSize:l]
			nextOffset, hasChain := currPgDelta.unmarshalDelta(data, ctx)
			currPgDelta.AddFlushRecord(offset, len(data), 1)
			pg.Append(currPgDelta)
			offset = nextOffset
			numSegments++

			if !hasChain {
				break loop
			}
		default:
			panic(fmt.Sprintf("Invalid page data type %d", typ))
		}
	}

	pg.SetNumSegments(numSegments)
	return pg, nil
}

func (s *Plasma) logError(err string) {
	if s.logger != nil {
		s.logger.Errorf("%Plasma: (fatal error - %s)\n", s.logPrefix, err)
	}
}

func (s *Plasma) logInfo(msg string) {
	if s.logger != nil {
		s.logger.Infof("%sPlasma: %s\n", s.logPrefix, msg)
	}
}

func (w *Writer) CompactAll() {
	callb := func(pid PageId, partn RangePartition) error {
		if pg, err := w.ReadPage(pid, false, w.wCtx); err == nil {
			staleFdSz := pg.Compact()
			if updated := w.UpdateMapping(pid, pg, w.wCtx); updated {
				w.wCtx.sts.FlushDataSz -= int64(staleFdSz)
				w.wCtx.sts.Compacts++
			}
		}
		return nil
	}

	w.PageVisitor(callb, 1)
}

func SetMemoryQuota(m int64) {
	atomic.StoreInt64(&memQuota, m)
}

func MemoryInUse() (sz int64) {
	buf := dbInstances.MakeBuf()
	defer dbInstances.FreeBuf(buf)

	ctx := dbInstances.NewIterator(ComparePlasma, buf)
	return MemoryInUse2(ctx)
}

func MemoryInUse2(ctx SwapperContext) (sz int64) {
	iter := (*skiplist.Iterator)(ctx)
	for iter.SeekFirst(); iter.Valid(); iter.Next() {
		db := (*Plasma)(iter.Get())
		sz += db.MemoryInUse()
	}

	return
}

func (s *Plasma) tryPageSwapin(pg Page) bool {
	var ok bool
	pgi := pg.(*page)
	if pgi.head != nil && pgi.head.state.IsEvicted() {
		pw := newPgDeltaWalker(pgi.head, pgi.ctx)
		// Force the pagewalker to read the swapout delta
		for ; !pw.End(); pw.Next() {
			if pw.Op() == opSwapoutDelta {
				pw.Next()
				break
			}
		}
		ok = pw.SwapIn(pgi)
		pw.Close()
	}

	return ok
}

func (s *Plasma) ItemsCount() int64 {
	return s.itemsCount
}

func (ctx *wCtx) tryThrottleForMemory() {
	if ctx.hasMemoryResPressure {
		for ctx.TriggerSwapper(ctx.SwapperContext()) {
			time.Sleep(swapperWaitInterval)
		}
	}
}

func (s *wCtx) tryThrottleForLSS() {
	if s.hasLSSResPressure {
		for s.TriggerLSSCleaner(s.Config.LSSCleanerMaxThreshold, s.Config.LSSCleanerThrottleMinSize) {
			runtime.Gosched()
		}
	}
}
