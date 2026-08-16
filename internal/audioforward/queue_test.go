package audioforward

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestBatchQueueBlockedTargetDoesNotBlockIndependentQueue(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	caption := NewBatchQueue(2)
	encoder := NewBatchQueue(2)
	captionStarted := make(chan struct{})
	encoderSent := make(chan int, 1)
	var workers sync.WaitGroup
	workers.Add(2)
	go func() {
		defer workers.Done()
		caption.Run(ctx, time.Second, func(requestCtx context.Context, _ []OpusPacket) error {
			close(captionStarted)
			<-requestCtx.Done()
			return requestCtx.Err()
		}, nil)
	}()
	go func() {
		defer workers.Done()
		encoder.Run(ctx, time.Second, func(_ context.Context, packets []OpusPacket) error {
			encoderSent <- len(packets)
			return nil
		}, nil)
	}()

	if !caption.Enqueue([]OpusPacket{{Opus: []byte{1}}}) {
		t.Fatal("caption batch was not queued")
	}
	select {
	case <-captionStarted:
	case <-time.After(time.Second):
		t.Fatal("caption target did not enter the blocking send")
	}
	if !encoder.Enqueue([]OpusPacket{{Opus: []byte{2}}, {Opus: []byte{3}}}) {
		t.Fatal("encoder batch was not queued")
	}
	select {
	case packetCount := <-encoderSent:
		if packetCount != 2 {
			t.Fatalf("encoder packet count = %d, want 2", packetCount)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked caption target stalled the encoder target")
	}
	cancel()
	done := make(chan struct{})
	go func() {
		workers.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("cancel did not stop target workers")
	}
}

func TestBatchQueueRejectsOverflowWithoutBlocking(t *testing.T) {
	queue := NewBatchQueue(2)
	if !queue.Enqueue([]OpusPacket{{Opus: []byte{1}}}) || !queue.Enqueue([]OpusPacket{{Opus: []byte{2}}}) {
		t.Fatal("queue rejected a batch below its bound")
	}
	if queue.Enqueue([]OpusPacket{{Opus: []byte{3}}}) {
		t.Fatal("queue accepted a batch above its bound")
	}
}

func TestBatchQueueCancellationDropsQueuedBatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	queue := NewBatchQueue(2)
	started := make(chan struct{})
	done := make(chan struct{})
	calls := 0
	go func() {
		defer close(done)
		queue.Run(ctx, time.Second, func(requestCtx context.Context, _ []OpusPacket) error {
			calls++
			if calls == 1 {
				close(started)
			}
			<-requestCtx.Done()
			return requestCtx.Err()
		}, nil)
	}()
	if !queue.Enqueue([]OpusPacket{{Opus: []byte{1}}}) {
		t.Fatal("first batch was not queued")
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first batch did not start")
	}
	if !queue.Enqueue([]OpusPacket{{Opus: []byte{2}}}) {
		t.Fatal("second batch was not queued")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("cancel did not stop the queue")
	}
	if calls != 1 {
		t.Fatalf("queued batch crossed the cancellation boundary: calls=%d", calls)
	}
}

func TestBatchQueueBoundsEachRequest(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	queue := NewBatchQueue(1)
	result := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		queue.Run(ctx, 20*time.Millisecond, func(requestCtx context.Context, _ []OpusPacket) error {
			<-requestCtx.Done()
			return requestCtx.Err()
		}, func(_ int, err error) {
			result <- err
			cancel()
		})
	}()
	if !queue.Enqueue([]OpusPacket{{Opus: []byte{1}}}) {
		t.Fatal("batch was not queued")
	}
	select {
	case err := <-result:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("request error = %v, want deadline exceeded", err)
		}
	case <-time.After(time.Second):
		t.Fatal("request timeout did not fire")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("queue did not stop after cancellation")
	}
}
