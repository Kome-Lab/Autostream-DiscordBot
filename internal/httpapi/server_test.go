package httpapi

import (
	"context"
	"errors"
	"github.com/example/autostream-discord-bot/internal/discord"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

const expectedStartJobRequestLimitBytes = 1_048_576
const expectedStartJobRequestLimitPlusOneBytes = 1_048_577

func TestStartJobRequestLimitIsOneMiB(t *testing.T) {
	if maxStartJobRequestBytes != expectedStartJobRequestLimitBytes {
		t.Fatalf(
			"maxStartJobRequestBytes = %d, want %d",
			maxStartJobRequestBytes,
			expectedStartJobRequestLimitBytes,
		)
	}
}

type httpFakeVoice struct {
	joined        discord.VoiceJob
	joinCalls     int
	sentMessages  []discord.OutboundMessage
	sendErr       error
	sendMessageID string
}

type httpReadCountingBody struct {
	reader io.Reader
	reads  int
}

func (b *httpReadCountingBody) Read(p []byte) (int, error) {
	b.reads++
	return b.reader.Read(p)
}

func (b *httpReadCountingBody) Close() error { return nil }

// httpPanelStopCallbackStopper models the Control Panel's nested callback to
// this Bot's authenticated /jobs/{id}/stop endpoint while the Bot's outbound
// auto-stop request is still awaiting the rest of the Panel dispatch.
type httpPanelStopCallbackStopper struct {
	handler     http.Handler
	token       string
	started     chan string
	callback    chan string
	ready       chan string
	release     chan struct{}
	returned    chan string
	ctxCanceled chan string
}

func (f *httpPanelStopCallbackStopper) StopStream(streamID string) error {
	return errors.New("context-aware auto-stop was not used")
}

func (f *httpPanelStopCallbackStopper) StopStreamContext(ctx context.Context, streamID string) error {
	select {
	case f.started <- streamID:
	default:
	}
	req := httptest.NewRequest(http.MethodPost, "/jobs/"+streamID+"/stop", nil)
	req.Header.Set("Authorization", "Bearer "+f.token)
	res := httptest.NewRecorder()
	f.handler.ServeHTTP(res, req)
	if res.Code != http.StatusAccepted {
		return errors.New("nested Control Panel stop callback was not accepted")
	}
	select {
	case f.callback <- streamID:
	default:
	}
	select {
	case <-ctx.Done():
		select {
		case f.ctxCanceled <- streamID:
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
		case f.ctxCanceled <- streamID:
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

func (f *httpFakeVoice) Connect() error { return nil }

func (f *httpFakeVoice) JoinVoice(job discord.VoiceJob) error {
	f.joinCalls++
	f.joined = job
	return nil
}

func (f *httpFakeVoice) LeaveVoice(streamID string) error { return nil }

func (f *httpFakeVoice) SendMessage(ctx context.Context, message discord.OutboundMessage) (discord.SentMessage, error) {
	f.sentMessages = append(f.sentMessages, message)
	if f.sendErr != nil {
		return discord.SentMessage{}, f.sendErr
	}
	messageID := f.sendMessageID
	if messageID == "" {
		messageID = "message-01"
	}
	return discord.SentMessage{MessageID: messageID}, nil
}

func (f *httpFakeVoice) Status() discord.Status {
	return discord.Status{Connected: f.joined.StreamID != "", VoiceConnected: f.joined.StreamID != ""}
}
