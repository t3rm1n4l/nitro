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
	"github.com/couchbase/nitro/skiplist"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

const (
	swapperWorkChanBufSize = 40
	swapperWorkBatchSize   = 16
	swapperWaitInterval    = time.Microsecond * 10
	swapperEvictionTimeout = time.Minute * 5
)

type clockHandle struct {
	buf []byte
	pos unsafe.Pointer
	itr *skiplist.Iterator
}

func (s *Plasma) acquireClockHandle() *clockHandle {
	s.clockLock.Lock()
	return s.clockHandle
}

func (s *Plasma) releaseClockHandle(h *clockHandle) {
	s.clockLock.Unlock()
}

func (s *Plasma) sweepClock(h *clockHandle) []PageId {
	pids := make([]PageId, 0, swapperWorkBatchSize)
	if h.pos == nil {
		pids = append(pids, s.StartPageId())
		h.itr.SeekFirst()
	} else {
		h.itr.Seek(h.pos)
	}

	for len(pids) < swapperWorkBatchSize && h.itr.Valid() {
		pid := h.itr.GetNode()
		pids = append(pids, pid)
		h.itr.Next()
	}

	if h.itr.Valid() {
		itm := h.itr.Get()
		sz := s.itemSize(itm)
		h.pos = unsafe.Pointer(&h.buf[0])
		memcopy(h.pos, itm, int(sz))
	} else {
		h.pos = nil
	}

	return pids
}

func (s *Plasma) tryEvictPages(ctx *wCtx) error {
	startEvictions := time.Now()
	sctx := ctx.SwapperContext()
	for s.TriggerSwapper(sctx) {
		h := s.acquireClockHandle()
		tok := ctx.BeginTxNoThrottle()
		pids := s.sweepClock(h)
		s.releaseClockHandle(h)
		for _, pid := range pids {
			if s.canEvict(pid) {
				s.Persist(pid, true, ctx)
			}
		}
		ctx.EndTx(tok)
		if time.Since(startEvictions) > swapperEvictionTimeout {
			return fmt.Errorf("Timeout: Unable to evict enough memory %v",
				s.MemoryInUse())
		}
	}
	return nil
}

func (s *Plasma) initLRUClock() {
	s.clockHandle = &clockHandle{
		buf: make([]byte, maxPageEncodedSize),
		itr: s.Skiplist.NewIterator2(s.cmp,
			s.Skiplist.MakeBuf()),
	}
}

func (s *Plasma) swapperDaemon() {
	var wg sync.WaitGroup

	killch := make(chan struct{})
	ddur := NewDecayInterval(swapperWaitInterval, time.Second)

	for i := 0; i < s.NumEvictorThreads; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			sctx := s.evictWriters[i].SwapperContext()
			for {
				select {
				case <-killch:
					s.trySMRObjects(s.evictWriters[i], 0)
					return
				default:
				}

				if s.TriggerSwapper(sctx) {
					s.tryEvictPages(s.evictWriters[i])
					s.trySMRObjects(s.evictWriters[i], swapperSMRInterval)
					ddur.Reset()
				} else {
					ddur.Sleep()
				}
			}
		}(i)
	}

	go func() {
		<-s.stopswapper
		close(killch)
	}()

	wg.Wait()

	s.stopswapper <- struct{}{}
}

type SwapperContext *skiplist.Iterator

func QuotaSwapper(ctx SwapperContext) bool {
	return MemoryInUse2(ctx) >= int64(float64(atomic.LoadInt64(&memQuota)))
}

func (s *Plasma) canEvict(pid PageId) bool {
	ok := true
	n := pid.(*skiplist.Node)
	ok = n.Cache == 0
	n.Cache = 0

	return ok
}

func (s *Plasma) updateCacheMeta(pid PageId) {
	pid.(*skiplist.Node).Cache = 1
}
