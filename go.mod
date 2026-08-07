module github.com/example/autostream-discord-bot

go 1.26.5

require github.com/cartridge-gg/discordgo v0.29.1-dave.25

// DiscordGo's DAVE fork carries the libdave CGO bindings and its pinned
// libdave submodule. Keep the source pinned in this repository so CI and
// release builds cannot silently resolve a different voice implementation.
replace github.com/cartridge-gg/discordgo => ./third_party/discordgo

require (
	github.com/gorilla/websocket v1.5.3 // indirect
	golang.org/x/crypto v0.54.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
)
