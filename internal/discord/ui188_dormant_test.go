package discord

import "testing"

func TestUI188UnselectedBotHasNoGatewaySession(t *testing.T) {
	client := NewDormantRealClient()
	for range 3 {
		if err := client.ReconcileGateway(Config{}, false); err != nil {
			t.Fatal(err)
		}
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.session != nil || client.voice != nil || client.cfg.BotToken != "" {
		t.Fatal("unselected idle Bot retained Discord connection")
	}
}
