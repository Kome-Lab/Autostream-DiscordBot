package audioforward

import (
	"context"
	"time"
)

// BatchQueue is a bounded, single-consumer queue for ordered Opus delivery to
// one target. Separate targets must use separate queues so backpressure or a
// timeout on one HTTP route cannot stall another route or the Discord receive
// loop.
type BatchQueue struct {
	batches chan []OpusPacket
}

func NewBatchQueue(capacity int) *BatchQueue {
	if capacity < 1 {
		capacity = 1
	}
	return &BatchQueue{batches: make(chan []OpusPacket, capacity)}
}

// Enqueue takes ownership of a copy of batch and returns false rather than
// blocking when the configured bound is reached.
func (q *BatchQueue) Enqueue(batch []OpusPacket) bool {
	if q == nil || len(batch) == 0 {
		return false
	}
	owned := make([]OpusPacket, len(batch))
	for i, packet := range batch {
		owned[i] = packet
		owned[i].Opus = append([]byte(nil), packet.Opus...)
	}
	select {
	case q.batches <- owned:
		return true
	default:
		return false
	}
}

// Run delivers queued batches in order until ctx is canceled. The callback is
// not invoked for a request canceled by the parent context, preventing normal
// Stop/Disconnect handling from being counted as an upstream failure.
func (q *BatchQueue) Run(ctx context.Context, requestTimeout time.Duration, send func(context.Context, []OpusPacket) error, result func(packetCount int, err error)) {
	if q == nil || send == nil {
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case batch := <-q.batches:
			if ctx.Err() != nil {
				return
			}
			requestCtx := ctx
			cancel := func() {}
			if requestTimeout > 0 {
				requestCtx, cancel = context.WithTimeout(ctx, requestTimeout)
			}
			err := send(requestCtx, batch)
			cancel()
			if ctx.Err() != nil {
				return
			}
			if result != nil {
				result(len(batch), err)
			}
		}
	}
}
