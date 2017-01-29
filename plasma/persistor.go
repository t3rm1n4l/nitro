package plasma

import (
	"encoding/binary"
	"fmt"
	//	"math/rand"
	"time"
	"unsafe"
)

// TODO: Make failsafe buffer
var maxPageEncodedSize = 1024 * 1024 * 1

type lssBlockType uint16

var lssBlockTypeSize = int(unsafe.Sizeof(*(new(lssBlockType))))

const (
	lssPageData lssBlockType = iota
	lssPageReloc
	lssPageUpdate
	lssPageRemove
	lssRecoveryPoints
	lssMaxSn
	lssDiscard
)

func discardLSSBlock(wbuf []byte) {
	binary.BigEndian.PutUint16(wbuf[:lssBlockTypeSize], uint16(lssDiscard))
}

func writeLSSBlock(wbuf []byte, typ lssBlockType, bs []byte) {
	copy(wbuf[lssBlockTypeSize:], bs)
	binary.BigEndian.PutUint16(wbuf[:lssBlockTypeSize], uint16(typ))
}

func getLSSBlockType(bs []byte) lssBlockType {
	return lssBlockType(binary.BigEndian.Uint16(bs))
}

func (s *Plasma) Persist(pid PageId, evict bool, ctx *wCtx) bool {
	var evicted bool
	buf := ctx.GetBuffer(0)
retry:

	// Never read from lss
	pg, _ := s.ReadPage(pid, nil, false)
	if pg.NeedsFlush() {
		bs, dataSz, staleFdSz, numSegments := pg.Marshal(buf, s.Config.MaxPageLSSSegments)
		offset, wbuf, res := s.lss.ReserveSpace(lssBlockTypeSize + len(bs))
		typ := pgFlushLSSType(pg, numSegments)
		writeLSSBlock(wbuf, typ, bs)

		var ok bool
		if evict {
			ok = s.EvictPage(pid, pg, offset)
			evicted = ok
		} else {
			pg.AddFlushRecord(offset, dataSz, numSegments)
			if ok = s.UpdateMapping(pid, pg); ok {
				ctx.sts.MemSz += int64(pg.GetMemUsed())
			}
		}

		if ok {
			s.lss.FinalizeWrite(res)
			ctx.sts.FlushDataSz += int64(dataSz) - int64(staleFdSz)
		} else {
			discardLSSBlock(wbuf)
			s.lss.FinalizeWrite(res)
			goto retry
		}
	} else if evict && pg.IsEvictable() {
		offset, _ := pg.GetLSSOffset()
		if evicted = s.EvictPage(pid, pg, offset); !evicted {
			goto retry
		}
	}

	return evicted
}

func (s *Plasma) PersistAll() {
	callb := func(pid PageId, partn RangePartition) error {
		s.Persist(pid, false, s.persistWriters[partn.Shard].wCtx)
		return nil
	}

	s.PageVisitor(callb, s.NumPersistorThreads)
	s.lss.Sync(false)
}

func (s *Plasma) EvictAll() {
	callb := func(pid PageId, partn RangePartition) error {
		s.Persist(pid, true, s.evictWriters[partn.Shard].wCtx)
		return nil
	}

	s.PageVisitor(callb, s.NumPersistorThreads)
}

func (s *Plasma) RunSwapper(proceed func() bool) {
	/*
		random := make([]*rand.Rand, s.NumEvictorThreads)
		for i, _ := range random {
			random[i] = rand.New(rand.NewSource(rand.Int63()))
		}

		callb := func(pid PageId, partn RangePartition) error {
			if proceed() && random[partn.Shard].Float32() <= 0.3 {
				s.Persist(pid, true, s.evictWriters[partn.Shard].wCtx)
			}
			return nil
		}

		s.PageVisitor(callb, s.NumEvictorThreads)
	*/

	s.tryEvictPages(s.evictWriters[0].wCtx)

}

func (s *Plasma) swapperDaemon() {
loop:
	for {
		if s.TriggerSwapper() && s.GetStats().NumCachedPages > 0 {
			fmt.Println("Swapper: started")
			numEvicted := s.GetStats().NumPagesSwapOut
			s.RunSwapper(s.ContinueSwapper)
			numEvicted = s.GetStats().NumPagesSwapOut - numEvicted
			fmt.Printf("Swapper: (evicted: %d blocks, rss: %d) finished\n", numEvicted, ProcessRSS())
			goto loop
		}

		time.Sleep(time.Second)
	}
}

func pgFlushLSSType(pg Page, numSegments int) lssBlockType {
	if numSegments > 0 {
		return lssPageUpdate
	}
	return lssPageData
}
