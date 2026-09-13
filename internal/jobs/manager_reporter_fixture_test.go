package jobs

import (
	"errors"
	"github.com/example/autostream-discord-bot/internal/discord"
)

func (f *recordingParticipantReporter) ParticipantsChanged(_ discord.VoiceJob, participants []Participant) error {
	f.mu.Lock()
	cloned := append([]Participant(nil), participants...)
	f.callCount++
	shouldFail := f.failuresRemaining > 0
	if shouldFail {
		f.failuresRemaining--
	}
	f.mu.Unlock()
	select {
	case f.calls <- cloned:
	default:
	}
	if shouldFail {
		if f.failureErr != nil {
			return f.failureErr
		}
		return errors.New("transient worker event failure")
	}
	return nil
}

func (f *recordingParticipantReporter) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.callCount
}

func (*recordingParticipantReporter) ActiveSpeakerChanged(discord.VoiceJob, string, string) error {
	return nil
}

func (*recordingParticipantReporter) ChatMessageReceived(discord.VoiceJob, ChatMessage) error {
	return nil
}

func (r *retryingOverlayReporter) ParticipantsChanged(_ discord.VoiceJob, participants []Participant) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.participantFails > 0 {
		r.participantFails--
		return retryableWorkerPublishError{status: 503, class: "http_status"}
	}
	r.participants = append(r.participants, append([]Participant(nil), participants...))
	return nil
}

func (r *retryingOverlayReporter) ActiveSpeakerChanged(job discord.VoiceJob, userID, displayName string) error {
	return r.ActiveSpeakerStateChanged(job, userID, displayName, true)
}

func (r *retryingOverlayReporter) ActiveSpeakerStateChanged(_ discord.VoiceJob, _ string, _ string, speaking bool) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.speakerFails > 0 {
		r.speakerFails--
		return retryableWorkerPublishError{status: 409, class: "http_status"}
	}
	r.speaking = append(r.speaking, speaking)
	return nil
}

func (r *retryingOverlayReporter) ChatMessageReceived(_ discord.VoiceJob, message ChatMessage) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.chatFails > 0 {
		r.chatFails--
		return retryableWorkerPublishError{status: 502, class: "http_status"}
	}
	r.chatMessages = append(r.chatMessages, message)
	return nil
}

func (f *fakeReporter) ParticipantsChanged(job discord.VoiceJob, participants []Participant) error {
	f.participantStreamID = job.StreamID
	f.participants = participants
	return f.err
}

func (f *fakeReporter) ActiveSpeakerChanged(job discord.VoiceJob, userID, displayName string) error {
	f.speakerStreamID = job.StreamID
	f.speakerUserID = userID
	f.speakerDisplayName = displayName
	f.speakerCallCount++
	return f.err
}

func (f *fakeReporter) ChatMessageReceived(job discord.VoiceJob, message ChatMessage) error {
	f.chatStreamID = job.StreamID
	f.chatMessage = message
	return f.err
}
