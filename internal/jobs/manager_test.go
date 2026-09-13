package jobs

import (
	"github.com/example/autostream-discord-bot/internal/discord"
	"sync"
)

type fakeVoice struct {
	mu            sync.Mutex
	status        discord.Status
	joined        discord.VoiceJob
	leftFor       string
	err           error
	joinCount     int
	joinCh        chan discord.VoiceJob
	sentMessages  []discord.OutboundMessage
	sendErr       error
	sendMessageID string
	sendStarted   chan struct{}
	sendRelease   chan struct{}
}

type autoStartTargetVoice struct {
	fakeVoice
	mu      sync.Mutex
	targets []discord.AutoStartVoiceTarget
}

func (f *autoStartTargetVoice) SetAutoStartVoiceTargets(targets []discord.AutoStartVoiceTarget) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.targets = append([]discord.AutoStartVoiceTarget(nil), targets...)
}

type snapshotVoice struct {
	fakeVoice
	snapshot discord.ParticipantSnapshot
	known    bool
}

type sequenceSnapshotVoice struct {
	fakeVoice
	mu        sync.Mutex
	snapshots []discord.ParticipantSnapshot
	next      int
}

type blockingSnapshotVoice struct {
	fakeVoice
	mu       sync.Mutex
	snapshot discord.ParticipantSnapshot
	calls    int
	started  chan struct{}
	release  chan struct{}
}

type fakeReporter struct {
	participantStreamID string
	participants        []Participant
	speakerStreamID     string
	speakerUserID       string
	speakerDisplayName  string
	speakerCallCount    int
	chatStreamID        string
	chatMessage         ChatMessage
	err                 error
}

type recordingParticipantReporter struct {
	mu                sync.Mutex
	calls             chan []Participant
	callCount         int
	failuresRemaining int
	failureErr        error
}

type retryableWorkerPublishError struct {
	status int
	class  string
}

func (e retryableWorkerPublishError) Error() string          { return "worker publish failed" }
func (e retryableWorkerPublishError) ErrorClass() string     { return e.class }
func (e retryableWorkerPublishError) HTTPStatusCode() int    { return e.status }
func (e retryableWorkerPublishError) RetryablePublish() bool { return true }

type retryingOverlayReporter struct {
	mu               sync.Mutex
	participantFails int
	speakerFails     int
	chatFails        int
	participants     [][]Participant
	speaking         []bool
	chatMessages     []ChatMessage
}

type activeSpeakerStateReporter struct {
	fakeReporter
	speaking []bool
}

type orderedOverlayReporter struct {
	mu                   sync.Mutex
	participantStartOnce sync.Once
	participantStarted   chan struct{}
	participantRelease   chan struct{}
	order                []string
}

func (f *orderedOverlayReporter) ParticipantsChanged(discord.VoiceJob, []Participant) error {
	f.participantStartOnce.Do(func() { close(f.participantStarted) })
	<-f.participantRelease
	f.mu.Lock()
	f.order = append(f.order, "participants")
	f.mu.Unlock()
	return nil
}

func (f *orderedOverlayReporter) ActiveSpeakerChanged(job discord.VoiceJob, userID, displayName string) error {
	return f.ActiveSpeakerStateChanged(job, userID, displayName, true)
}

func (f *orderedOverlayReporter) ActiveSpeakerStateChanged(discord.VoiceJob, string, string, bool) error {
	f.mu.Lock()
	f.order = append(f.order, "speaker")
	f.mu.Unlock()
	return nil
}

func (*orderedOverlayReporter) ChatMessageReceived(discord.VoiceJob, ChatMessage) error {
	return nil
}

func (f *activeSpeakerStateReporter) ActiveSpeakerStateChanged(job discord.VoiceJob, userID, displayName string, speaking bool) error {
	f.speakerStreamID = job.StreamID
	f.speakerUserID = userID
	f.speakerDisplayName = displayName
	f.speakerCallCount++
	f.speaking = append(f.speaking, speaking)
	return f.err
}

type fakeStreamStarter struct {
	mu      sync.Mutex
	started []string
	ch      chan string
	err     error
	errs    []error
}

type fakeStreamStopper struct {
	mu      sync.Mutex
	stopped []string
	ch      chan string
	err     error
	errs    []error
}

type blockingContextStreamStopper struct {
	started   chan string
	canceled  chan string
	committed chan string
}

type commitAfterCancelStreamStopper struct {
	started   chan string
	canceled  chan string
	release   chan struct{}
	committed chan string
}

// panelStopCallbackStreamStopper models the nested lifecycle call made by the
// Control Panel: the Bot initiates auto-stop, then the Panel calls this Bot's
// stop endpoint before it has dispatched the Encoder/Worker stops and rearmed
// the next waiting stream.
type panelStopCallbackStreamStopper struct {
	manager  *Manager
	started  chan string
	callback chan string
	ready    chan string
	release  chan struct{}
	returned chan string
	canceled chan string
}
