//go:build linux && ui188_chain

package main

// This test binary replaces only the external Discord Gateway/Voice peer.
// Panel selection, secret leases, assignment epochs, Manager, control.Client,
// Worker reporting, service authentication and audio HTTP ingest remain real.
import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cartridge-gg/discordgo"
	"github.com/example/autostream-discord-bot/internal/audioforward"
	"github.com/example/autostream-discord-bot/internal/control"
	discord "github.com/example/autostream-discord-bot/internal/discord"
	"github.com/example/autostream-discord-bot/internal/httpapi"
	"github.com/example/autostream-discord-bot/internal/jobs"
	"github.com/gorilla/websocket"
)

type ui188Peer struct {
	mu              sync.Mutex
	connections     map[*websocket.Conn]int
	members         map[string]map[string]any
	states          map[string]map[string]any
	sequence        int
	connectionsMade int
}

func newUI188Peer() *ui188Peer {
	return &ui188Peer{connections: map[*websocket.Conn]int{}, members: map[string]map[string]any{}, states: map[string]map[string]any{}}
}
func (p *ui188Peer) guild() map[string]any {
	members := []any{}
	states := []any{}
	for _, v := range p.members {
		members = append(members, v)
	}
	for _, v := range p.states {
		states = append(states, v)
	}
	return map[string]any{"id": "1001", "name": "UI188 isolated guild", "unavailable": false, "members": members, "voice_states": states, "channels": []any{map[string]any{"id": "1003", "guild_id": "1001", "type": 2, "name": "UI188 Voice"}, map[string]any{"id": "1002", "guild_id": "1001", "type": 0, "name": "UI188 Text"}}}
}
func (p *ui188Peer) gateway(w http.ResponseWriter, r *http.Request) {
	conn, err := (&websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}).Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()
	_ = conn.WriteJSON(map[string]any{"op": 10, "d": map[string]any{"heartbeat_interval": 10000}})
	var message struct {
		Op int `json:"op"`
	}
	if conn.ReadJSON(&message) != nil || message.Op != 2 {
		return
	}
	p.mu.Lock()
	p.connections[conn] = 1
	p.connectionsMade++
	_ = conn.WriteJSON(map[string]any{"op": 0, "t": "READY", "s": 1, "d": map[string]any{"v": 10, "session_id": "ui188-external-test-session", "resume_gateway_url": "ws://127.0.0.1:" + r.URL.Query().Get("port"), "user": map[string]any{"id": "1999", "username": "ui188-test-bot", "bot": true}, "guilds": []any{map[string]any{"id": "1001", "unavailable": true}}, "application": map[string]any{"id": "1999", "flags": 0}}})
	_ = conn.WriteJSON(map[string]any{"op": 0, "t": "GUILD_CREATE", "s": 2, "d": p.guild()})
	p.sequence = 2
	p.mu.Unlock()
	defer func() { p.mu.Lock(); delete(p.connections, conn); p.mu.Unlock() }()
	for conn.ReadJSON(&message) == nil {
		if message.Op == 1 {
			p.mu.Lock()
			_ = conn.WriteJSON(map[string]any{"op": 11, "d": nil})
			p.mu.Unlock()
		}
	}
}
func (p *ui188Peer) input(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "method", 405)
		return
	}
	var input struct {
		UserID      string `json:"user_id"`
		ChannelID   string `json:"channel_id"`
		IsBot       bool   `json:"is_bot"`
		Existing    bool   `json:"existing"`
		MemberKnown *bool  `json:"member_known"`
		MemberOnly  bool   `json:"member_only"`
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
	dec.DisallowUnknownFields()
	if dec.Decode(&input) != nil || input.UserID == "" {
		http.Error(w, "input", 400)
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	member := map[string]any{"user": map[string]any{"id": input.UserID, "username": "UI188 participant", "bot": input.IsBot}, "roles": []string{}}
	state := map[string]any{"guild_id": "1001", "channel_id": input.ChannelID, "user_id": input.UserID, "session_id": "ui188-controlled-voice", "member": member, "deaf": false, "mute": false, "self_deaf": false, "self_mute": false, "suppress": false}
	if input.MemberKnown != nil && !*input.MemberKnown {
		delete(p.members, input.UserID)
		delete(state, "member")
	} else {
		p.members[input.UserID] = member
	}
	if input.MemberOnly {
		member["guild_id"] = "1001"
		p.sequence++
		for conn := range p.connections {
			_ = conn.WriteJSON(map[string]any{"op": 0, "t": "GUILD_MEMBER_UPDATE", "s": p.sequence, "d": member})
		}
		w.WriteHeader(http.StatusAccepted)
		return
	}
	p.states[input.UserID] = state
	if input.Existing {
		p.sequence++
		for conn := range p.connections {
			_ = conn.WriteJSON(map[string]any{"op": 0, "t": "GUILD_CREATE", "s": p.sequence, "d": p.guild()})
		}
	} else {
		p.sequence++
		for conn := range p.connections {
			_ = conn.WriteJSON(map[string]any{"op": 0, "t": "VOICE_STATE_UPDATE", "s": p.sequence, "d": state})
		}
	}
	w.WriteHeader(http.StatusAccepted) // External Discord input admission, never an internal service receipt.
}

type ui188VoicePeer struct {
	*discord.RealClient
	peer                  *ui188Peer
	source                string
	packets               [][]byte
	mu                    sync.Mutex
	job                   discord.VoiceJob
	cancel                context.CancelFunc
	joins, sent, failures int
	notifications         []discord.OutboundMessage
}

func (v *ui188VoicePeer) JoinVoice(job discord.VoiceJob) error {
	if !v.RealClient.Status().Connected {
		return errors.New("external Gateway not connected")
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.job.StreamID != "" {
		return errors.New("external Voice already occupied")
	}
	receiver, err := v.RealClient.UI188ControlledVoice(job)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(context.Background())
	v.cancel = cancel
	v.job = job
	v.joins++
	go v.audio(ctx, job, receiver)
	return nil // Controlled external Voice peer accepted this join.
}
func (v *ui188VoicePeer) LeaveVoice(id string) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.job.StreamID != "" && v.job.StreamID != id {
		return errors.New("external Voice wrong stream")
	}
	if v.cancel != nil {
		v.cancel()
	}
	if err := v.RealClient.LeaveVoice(id); err != nil {
		return err
	}
	v.job = discord.VoiceJob{}
	return nil
}
func (v *ui188VoicePeer) Status() discord.Status {
	s := v.RealClient.Status()
	v.mu.Lock()
	defer v.mu.Unlock()
	s.VoiceConnected = v.job.StreamID != ""
	return s
}
func (v *ui188VoicePeer) SendMessage(ctx context.Context, message discord.OutboundMessage) (discord.SentMessage, error) {
	if err := ctx.Err(); err != nil {
		return discord.SentMessage{}, err
	}
	if message.ChannelID != "1002" || strings.TrimSpace(message.Content) == "" {
		return discord.SentMessage{}, &discord.SendMessageError{Code: discord.SendMessageCodeRequestRejected}
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	v.notifications = append(v.notifications, message)
	return discord.SentMessage{MessageID: fmt.Sprintf("ui188-r3-external-%d", len(v.notifications))}, nil
}

func (v *ui188VoicePeer) observations(w http.ResponseWriter, _ *http.Request) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.peer.mu.Lock()
	defer v.peer.mu.Unlock()
	s := v.RealClient.Status()
	_ = json.NewEncoder(w).Encode(map[string]any{"stream_id": v.job.StreamID, "generation": v.job.JobGeneration, "voice_joins": v.joins, "audio_packets": v.sent, "audio_failures": s.AudioForwardErrors, "actual_packets_received": s.AudioPacketsReceived, "actual_packets_forwarded": s.AudioPacketsForwarded, "actual_queue_drops": s.AudioForwardQueueDrops, "gateway_connections": v.peer.connectionsMade, "active_gateway_connections": len(v.peer.connections), "notifications": len(v.notifications)})
}
func (v *ui188VoicePeer) SnapshotVoiceParticipants(job discord.VoiceJob) (discord.ParticipantSnapshot, bool) {
	v.peer.mu.Lock()
	defer v.peer.mu.Unlock()
	snapshot := discord.ParticipantSnapshot{StreamID: job.StreamID, GuildID: job.GuildID, VoiceChannelID: job.VoiceChannelID, Revision: uint64(time.Now().UnixNano())}
	for id, state := range v.peer.states {
		if state["channel_id"] == job.VoiceChannelID {
			member, known := v.peer.members[id]
			if !known {
				continue
			}
			user := member["user"].(map[string]any)
			snapshot.Participants = append(snapshot.Participants, discord.VoiceParticipant{UserID: id, Username: "UI188 participant", IsBot: user["bot"].(bool)})
		}
	}
	return snapshot, true
}
func ui188Opus(path string) ([][]byte, error) {
	if !filepath.IsAbs(path) {
		return nil, errors.New("absolute synthetic Opus input required")
	}
	body, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var packets [][]byte
	var packet []byte
	for len(body) > 0 {
		if len(body) < 27 || string(body[:4]) != "OggS" {
			return nil, errors.New("invalid Ogg input")
		}
		segments := int(body[26])
		if len(body) < 27+segments {
			return nil, errors.New("truncated Ogg lacing")
		}
		lacing := body[27 : 27+segments]
		body = body[27+segments:]
		for _, size := range lacing {
			if len(body) < int(size) {
				return nil, errors.New("truncated Opus packet")
			}
			packet = append(packet, body[:int(size)]...)
			body = body[int(size):]
			if size < 255 {
				if !strings.HasPrefix(string(packet), "OpusHead") && !strings.HasPrefix(string(packet), "OpusTags") {
					packets = append(packets, packet)
				}
				packet = nil
			}
		}
	}
	if len(packets) == 0 {
		return nil, errors.New("empty Opus audio")
	}
	return packets, nil
}
func TestUI188R4ExternalOpusRejectsMalformed(t *testing.T) {
	p := filepath.Join(t.TempDir(), "bad.opus")
	if err := os.WriteFile(p, []byte("not Ogg"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := ui188Opus(p); err == nil {
		t.Fatal("malformed external audio accepted")
	}
	if _, err := ui188Opus("relative"); err == nil {
		t.Fatal("relative fixture accepted")
	}
}

func TestUI188R4ExternalGatewayHandshake(t *testing.T) {
	peer := newUI188Peer()
	mux := http.NewServeMux()
	mux.HandleFunc("/ws/", peer.gateway)
	server := httptest.NewServer(mux)
	defer server.Close()
	mux.HandleFunc("/gateway", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{"url": "ws" + strings.TrimPrefix(server.URL, "http") + "/ws"})
	})
	oldGateway := discordgo.EndpointGateway
	discordgo.EndpointGateway = server.URL + "/gateway"
	defer func() { discordgo.EndpointGateway = oldGateway }()
	client, err := discord.NewRealClient(discord.Config{BotToken: "UI188-ONLY-EXTERNAL-TEST"})
	if err != nil {
		t.Fatal(err)
	}
	if err = client.Connect(); err != nil {
		t.Fatalf("controlled external Gateway handshake: %v", err)
	}
	if !client.Status().Connected {
		t.Fatal("actual Gateway did not establish a connection")
	}
	if err = client.ReconcileGateway(discord.Config{}, false); err != nil {
		t.Fatal(err)
	}
	if client.Status().Connected {
		t.Fatal("unselected Gateway remained connected")
	}
	// Observe actual external socket opens after deselection; a false public
	// Connected flag alone does not prove that the SDK stopped reconnecting.
	time.Sleep(2 * time.Second)
	peer.mu.Lock()
	opens, active := peer.connectionsMade, len(peer.connections)
	for connection := range peer.connections {
		_ = connection.Close()
	}
	peer.mu.Unlock()
	if opens != 1 || active != 0 {
		t.Fatalf("unselected real Gateway reopened external transport: opens=%d active=%d", opens, active)
	}
}

func TestUI188R4NodeChild(t *testing.T) {
	if os.Getenv("AUTOSTREAM_UI188_R4_CHILD") != "bot" {
		t.Skip("owned Linux chain child only")
	}
	cfg := control.ConfigFromEnv()
	if cfg.ConfigError != "" || cfg.ServiceID == "" || cfg.Token == "" {
		t.Fatal("canonical test node configuration unavailable")
	}
	host, _, err := net.SplitHostPort(cfg.BindAddress)
	if err != nil || host != "127.0.0.1" {
		t.Fatal("child listener must be loopback")
	}
	packets, err := ui188Opus(os.Getenv("AUTOSTREAM_UI188_R4_OPUS"))
	if err != nil {
		t.Fatal("synthetic external audio unavailable")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	trace := newUI188CredentialTrace(cfg.ServiceID)
	defer trace.file.Close()
	heartbeatTransport, heartbeatRecovery, closeHeartbeatTrace, err := newUI188MC12Heartbeat(cfg.ServiceID, trace)
	if err != nil {
		t.Fatal("test-only bounded heartbeat policy unavailable")
	}
	defer closeHeartbeatTrace()
	client := control.Client{Config: cfg, HTTP: ui188HBHTTPClient(heartbeatTransport)}
	if err := client.Register(ctx); err != nil {
		t.Fatalf("real Panel registration: %v", err)
	}
	peer := newUI188Peer()
	external := &http.Server{Addr: os.Getenv("AUTOSTREAM_UI188_R4_EXTERNAL_ADDR"), ReadHeaderTimeout: 5 * time.Second}
	if !strings.HasPrefix(external.Addr, "127.0.0.1:") {
		t.Fatal("external peer must be loopback")
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/ws/", peer.gateway)
	mux.HandleFunc("/input", peer.input)
	mux.HandleFunc("/gateway", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{"url": "ws://" + external.Addr + "/ws"})
	})
	voice := &ui188VoicePeer{RealClient: discord.NewDormantRealClient(), peer: peer, source: cfg.ServiceID, packets: packets}
	voice.SetAudioForwarder(ui188TracedForwarder{client: audioforward.Client{Config: audioforward.ConfigFromEnv()}, source: cfg.ServiceID}, cfg.ServiceID)
	mux.HandleFunc("GET /observations", voice.observations)
	external.Handler = mux
	listener, err := net.Listen("tcp", external.Addr)
	if err != nil {
		t.Fatal(err)
	}
	go external.Serve(listener)
	defer external.Close()
	oldGateway, oldBot := discordgo.EndpointGateway, discordgo.EndpointGatewayBot
	discordgo.EndpointGateway = "http://" + external.Addr + "/gateway"
	discordgo.EndpointGatewayBot = discordgo.EndpointGateway
	t.Cleanup(func() { discordgo.EndpointGateway, discordgo.EndpointGatewayBot = oldGateway, oldBot })
	manager := jobs.NewManagerWithReporter(voice, buildWorkerReporter())
	r9 := &ui188R9HeldSink{Manager: manager, voice: voice.RealClient, starts: map[string]uint64{}}
	voice.SetEventSink(r9)
	mux.HandleFunc("/r9/stale-authority", r9.control)
	controller := controlStreamStarter{client: client}
	manager.SetStreamStarter(ui188R9CountedStarter{sink: r9, next: controller})
	manager.SetStreamStopper(controller)
	refresh := func() error {
		return manager.RefreshRuntimeConfig(func() error {
			cfg, err := client.RuntimeConfig(ctx)
			if err != nil {
				return err
			}
			trace.snapshot(cfg, manager, "before", false)
			if err = reconcileRuntimeGateway(ctx, client, manager, cfg); err != nil {
				trace.snapshot(cfg, manager, "after", true)
				return err
			}
			trace.snapshot(cfg, manager, "after", false)
			applyRuntimeConfigToManager(manager, cfg, reconnectPolicyFromEnv())
			return nil
		})
	}
	manager.SetAutoStartRefresher(refresh)
	if err := refresh(); err != nil {
		t.Fatalf("initial real runtime configuration: %v", err)
	}
	service := &http.Server{Addr: cfg.BindAddress, Handler: httpapi.NewServerWithRuntimeConfigAndUpdaterIdentity(control.ServiceType, manager, httpapi.TokenVerifierFromEnv(), client.RuntimeConfig, httpapi.NewUpdaterIdentityLatch(control.ServiceType)), ReadHeaderTimeout: 5 * time.Second}
	errs := make(chan error, 1)
	go func() { errs <- service.ListenAndServe() }()
	defer service.Close()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	fmt.Println("UI188_R4_BOT_CHILD_READY " + cfg.ServiceID)
	for {
		select {
		case err := <-errs:
			t.Fatal(err)
		case <-ticker.C:
			if err := refresh(); err != nil {
				t.Fatalf("real runtime refresh: %v", err)
			}
			if err := heartbeatRecovery.Tick(ctx, client, manager.CurrentStreamID(), manager.Metrics()); err != nil {
				t.Fatalf("bounded real heartbeat: %v", err)
			}
		}
	}
}
