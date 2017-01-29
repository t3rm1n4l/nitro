package plasma

import "errors"

const (
	clockChanBufSize    = 40
	clockSweepBatchSize = 4
	minEvictSome        = 4
)

func (s *Plasma) clockVisitor() {
	var sweepBatch []PageId

	callb := func(pid PageId, partn RangePartition) error {
		if sweepBatch == nil {
			sweepBatch = make([]PageId, 0, clockSweepBatchSize)
		}

		sweepBatch = append(sweepBatch, pid)

		if len(sweepBatch) == cap(sweepBatch) {
			select {
			case s.clockCh <- sweepBatch:
				sweepBatch = nil
			case <-s.stopswapper:
				s.stopswapper <- struct{}{}
				return errors.New("swapper shutdown")
			}
		}
		return nil
	}

	for s.PageVisitor(callb, 1) == nil {
	}
}

func (s *Plasma) tryEvictPages(ctx *wCtx) {
	if s.TriggerSwapper != nil {
		for s.TriggerSwapper() && s.GetStats().NumCachedPages > 0 {
			pids := <-s.clockCh
			for _, pid := range pids {
				s.Persist(pid, true, ctx)
			}
		}
	}
}

func (w *Writer) EvictSome() {
	count := 0
retry:
	pids := <-w.clockCh
	for _, pid := range pids {
		if w.Persist(pid, true, w.wCtx) {
			count++
		}
		if count < minEvictSome {
			goto retry
		}
	}
}
