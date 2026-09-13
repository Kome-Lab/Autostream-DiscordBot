package jobs

import (
	"context"
	"errors"
	"time"
)

func (f *fakeStreamStarter) StartStream(streamID string) error {
	f.mu.Lock()
	f.started = append(f.started, streamID)
	err := f.err
	if len(f.errs) > 0 {
		err = f.errs[0]
		f.errs = f.errs[1:]
	}
	f.mu.Unlock()
	if f.ch != nil {
		select {
		case f.ch <- streamID:
		default:
		}
	}
	return err
}

func (f *fakeStreamStopper) StopStream(streamID string) error {
	f.mu.Lock()
	f.stopped = append(f.stopped, streamID)
	err := f.err
	if len(f.errs) > 0 {
		err = f.errs[0]
		f.errs = f.errs[1:]
	}
	f.mu.Unlock()
	if f.ch != nil {
		select {
		case f.ch <- streamID:
		default:
		}
	}
	return err
}

func (f *blockingContextStreamStopper) StopStream(streamID string) error {
	return errors.New("context-aware auto-stop was not used")
}

func (f *blockingContextStreamStopper) StopStreamContext(ctx context.Context, streamID string) error {
	select {
	case f.started <- streamID:
	default:
	}
	select {
	case <-ctx.Done():
		select {
		case f.canceled <- streamID:
		default:
		}
		return ctx.Err()
	case <-time.After(time.Second):
		select {
		case f.committed <- streamID:
		default:
		}
		return nil
	}
}

func (f *commitAfterCancelStreamStopper) StopStream(streamID string) error {
	return errors.New("context-aware auto-stop was not used")
}

func (f *commitAfterCancelStreamStopper) StopStreamContext(ctx context.Context, streamID string) error {
	select {
	case f.started <- streamID:
	default:
	}
	<-ctx.Done()
	select {
	case f.canceled <- streamID:
	default:
	}
	<-f.release
	select {
	case f.committed <- streamID:
	default:
	}
	return nil
}

func (f *panelStopCallbackStreamStopper) StopStream(streamID string) error {
	return errors.New("context-aware auto-stop was not used")
}

func (f *panelStopCallbackStreamStopper) StopStreamContext(ctx context.Context, streamID string) error {
	select {
	case f.started <- streamID:
	default:
	}
	if err := f.manager.Stop(streamID); err != nil {
		return err
	}
	select {
	case f.callback <- streamID:
	default:
	}
	select {
	case <-ctx.Done():
		select {
		case f.canceled <- streamID:
		default:
		}
		return ctx.Err()
	default:
	}
	select {
	case f.ready <- streamID:
	default:
	}
	select {
	case <-ctx.Done():
		select {
		case f.canceled <- streamID:
		default:
		}
		return ctx.Err()
	case <-f.release:
		select {
		case f.returned <- streamID:
		default:
		}
		return nil
	}
}
