//go:build linux && ui188_chain

package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/cartridge-gg/discordgo"
	"github.com/example/autostream-discord-bot/internal/audioforward"
	"github.com/example/autostream-discord-bot/internal/discord"
)

// RFC6716 TOC duration at the RTP 48kHz clock; this observes the payload and
// rejects malformed duration instead of assuming every packet is 20ms.
func ui188OpusSamples(payload []byte) (int, error) {
	if len(payload) == 0 {
		return 0, errors.New("empty Opus")
	}
	config, code := int(payload[0]>>3), int(payload[0]&3)
	frame := 0
	if config >= 16 {
		frame = 120 << uint(config&3)
	} else if config >= 12 {
		frame = 480 << uint(config&1)
	} else {
		frame = []int{480, 960, 1920, 2880}[config&3]
	}
	count := 1
	if code == 1 || code == 2 {
		count = 2
	}
	if code == 3 {
		if len(payload) < 2 {
			return 0, errors.New("Opus frame count missing")
		}
		count = int(payload[1] & 63)
	}
	if count == 0 || frame*count > 5760 {
		return 0, errors.New("Opus duration invalid")
	}
	return frame * count, nil
}

var ui188AudioJournalMu sync.Mutex

func ui188AudioJournal(source string, value map[string]any) {
	directory := os.Getenv("UI188_EVIDENCE_DIRECTORY")
	if native := os.Getenv("UI188_AUDIO_JOURNAL_DIRECTORY"); native != "" {
		directory = native
	}
	if directory == "" {
		return
	}
	value["source"] = source
	body, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	ui188AudioJournalMu.Lock()
	defer ui188AudioJournalMu.Unlock()
	file, err := os.OpenFile(filepath.Join(directory, "audio-"+source+".jsonl"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	if _, err = file.Write(append(body, '\n')); err != nil {
		panic(err)
	}
}

type ui188TracedForwarder struct {
	client audioforward.Client
	source string
}

func (f ui188TracedForwarder) ForwardOpusRequestTimeout() time.Duration {
	return f.client.ForwardOpusRequestTimeout()
}
func (f ui188TracedForwarder) ForwardOpus(ctx context.Context, url, stream, source, token string, packets []audioforward.OpusPacket) error {
	start := time.Now()
	err := f.client.ForwardOpus(ctx, url, stream, source, token, packets)
	if len(packets) > 0 {
		first, last := packets[0], packets[len(packets)-1]
		ui188AudioJournal(f.source, map[string]any{"event": "actual_production_forward", "stream_id": stream, "job_generation": first.JobGeneration, "voice_generation": first.ConnectionGeneration, "packet_count": len(packets), "first_sequence": first.Sequence, "last_sequence": last.Sequence, "first_rtp": first.Timestamp, "last_rtp": last.Timestamp, "started_at": start.UTC().Format(time.RFC3339Nano), "duration_ms": float64(time.Since(start)) / float64(time.Millisecond), "actual_error": err != nil})
	}
	return err
}

func (v *ui188VoicePeer) audio(ctx context.Context, job discord.VoiceJob, receiver chan<- *discordgo.Packet) {
	mode := os.Getenv("UI188_AUDIO_MODE")
	if mode == "" {
		mode = "burst"
	}
	if mode != "burst" && mode != "packet" {
		panic("unknown controlled Voice audio mode")
	}
	frameSamples, err := ui188OpusSamples(v.packets[0])
	if err != nil {
		panic(err)
	}
	for _, payload := range v.packets {
		samples, err := ui188OpusSamples(payload)
		if err != nil || samples != frameSamples {
			panic("synthetic Opus duration is not uniform")
		}
	}
	count := 1
	if mode == "burst" {
		count = 5
	}
	interval := time.Duration(frameSamples*count) * time.Second / 48000
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	start := time.Now()
	var sequence uint16
	var rtp uint32
	index := 0
	totalSamples := uint64(0)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			first := sequence
			// Scheduled external-source samples must not disappear when Ticker
			// coalesces a delayed notification. Preserve payloads and RTP steps.
			due := int(time.Since(start)/interval)*count - index
			if due <= 0 {
				continue
			}
			for i := 0; i < due; i++ {
				packet := &discordgo.Packet{SSRC: 188, Sequence: sequence, Timestamp: rtp, Opus: append([]byte(nil), v.packets[index%len(v.packets)]...)}
				select {
				case receiver <- packet:
				case <-ctx.Done():
					return
				}
				sequence++
				rtp += uint32(frameSamples)
				index++
				totalSamples += uint64(frameSamples)
			}
			v.mu.Lock()
			v.sent += due
			v.mu.Unlock()
			ui188AudioJournal(v.source, map[string]any{"event": "external_voice_input", "stream_id": job.StreamID, "job_generation": job.JobGeneration, "mode": mode, "first_sequence": first, "last_sequence": sequence - 1, "next_rtp": rtp, "packet_samples": frameSamples, "total_samples": totalSamples, "total_packets": index, "planned_tick_elapsed_ms": float64(time.Duration(index/count)*interval) / float64(time.Millisecond), "actual_elapsed_ms": float64(time.Since(start)) / float64(time.Millisecond)})
		}
	}
}
