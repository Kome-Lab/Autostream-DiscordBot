module github.com/example/autostream-discord-bot

go 1.26.5

require github.com/cartridge-gg/discordgo v0.29.1-dave.25

require github.com/example/autostream-contracts v0.0.0

// DiscordGo's DAVE fork carries the libdave CGO bindings and its pinned
// libdave submodule. Keep the source pinned in this repository so CI and
// release builds cannot silently resolve a different voice implementation.
replace github.com/cartridge-gg/discordgo => ./third_party/discordgo

replace github.com/example/autostream-contracts => github.com/Kome-Lab/Autostream-Contracts v1.2.12-0.20260903202917-82716abd84f2

require (
	github.com/gorilla/websocket v1.5.3 // indirect
	github.com/santhosh-tekuri/jsonschema/v6 v6.0.3 // indirect
	golang.org/x/crypto v0.55.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.41.0 // indirect
)
