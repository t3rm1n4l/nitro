package plasma

import "errors"

const (
	clockChanBufSize    = 40
	clockSweepBatchSize = 4
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
		for s.TriggerSwapper() {
			pids := <-s.clockCh
			for _, pid := range pids {
				s.Persist(pid, true, ctx)
			}
		}
	}
}
