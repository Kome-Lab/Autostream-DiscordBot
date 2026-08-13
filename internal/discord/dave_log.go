package discord

import (
	"log"
	"sync"

	"github.com/cartridge-gg/discordgo/dave"
)

var installDAVELogSinkOnce sync.Once

// installSafeDAVELogSink redirects libdave's process-global native logger away
// from stderr. MLS logs can contain opaque sender/key material, so only the
// severity is retained for warning/error diagnostics; file names, messages,
// payloads, and URLs are intentionally discarded.
func installSafeDAVELogSink() {
	installDAVELogSinkOnce.Do(func() {
		dave.InstallLogSink(func(severity dave.LogSeverity, _ string, _ int, _ string) {
			switch severity {
			case dave.LogWarning:
				log.Printf("Discord DAVE log: severity=warning")
			case dave.LogError:
				log.Printf("Discord DAVE log: severity=error")
			}
		})
	})
}
