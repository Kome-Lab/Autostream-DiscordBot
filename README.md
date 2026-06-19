# autostream-discord-bot

AutoStream の Discord Bot service です。Discord Gateway / Voice に接続し、stream ごとに指定された guild / voice channel へ参加して、参加者状態、active speaker 状態、Discord VC audio packet を AutoStream の分散 service へ渡します。

この repository では Control Panel、Encoder/Recorder、Worker、Observability の責務を実装しません。

## 責務

- Control Panel へ service registration / heartbeat を送信する。
- Control Panel から stream job start / stop を受ける。
- Control Panel の runtime config から、自分の `service_id` に紐付く Discord Bot Config だけを取得する。
- Discord Bot token を Control Panel の runtime secret として取得する。`DISCORD_BOT_TOKEN` env は移行期間の fallback です。
- stream job に含まれる guild / voice channel へ参加する。
- Discord VC の Opus packet を Encoder/Recorder の audio ingest endpoint へ forward する。
- participant / active speaker 状態を Worker または Control Panel へ送る。

## Bootstrap Env

通常運用で必要な env は Control Panel 接続と inbound job 検証だけです。

```text
SERVICE_ID=discord-bot-01
SERVICE_NAME=Discord Bot 01
SERVICE_PUBLIC_URL=https://bot.example.com
CONTROL_PANEL_URL=https://control.example.com
CONTROL_PANEL_TOKEN=<SERVICE_TOKEN>
SERVICE_CONTROL_TOKEN_SHA256=<SHA256_OF_SERVICE_CALL_TOKEN>
DISCORD_RECONNECT_ENABLED=true
DISCORD_RECONNECT_MAX_ATTEMPTS=5
DISCORD_RECONNECT_BASE_DELAY=2s
DISCORD_RECONNECT_MAX_DELAY=30s
TZ=Asia/Tokyo
```

Production mode sets `AUTOSTREAM_ENV=production` or `AUTOSTREAM_REQUIRE_CONTROL_PANEL_RUNTIME_CONFIG=true`. In that mode the Discord Bot must register with Control Panel, fetch service-scoped runtime config, resolve `bot_token_secret_name` through `/services/runtime-secrets/resolve`, and initialize a real Discord client. It does not fall back to `DISCORD_BOT_TOKEN` or dry-run mode when Control Panel runtime config or runtime secret resolution fails. Dry-run Discord mode is for local migration checks only.

When the runtime config provider is configured, `/jobs/start` fails closed if the Control Panel runtime config refresh fails. Request-supplied guild or voice channel IDs do not bypass primary assignment validation.

Inbound Control Panel dispatch uses `SERVICE_CONTROL_TOKEN` or `SERVICE_CONTROL_TOKEN_SHA256`. `CONTROL_PANEL_TOKEN` is outbound-only; in production or runtime-config-required mode it must not authorize `/jobs/start`, `/jobs/{id}/stop`, or status mutation endpoints.

Control Panel の Discord Bot Config に `bot_token`、`guild_id`、`voice_channel_id`、`text_channel_id`、caption/STT 設定を登録してください。`DISCORD_BOT_TOKEN`、`DISCORD_GUILD_ID`、`DISCORD_VOICE_CHANNEL_ID` は互換 fallback または local dry-run 用に限定します。

## Runtime Config

起動時に service token で `/services/register` を呼び、その後 `/services/runtime-config?service_id=<SERVICE_ID>` を取得します。runtime config には raw secret は含まれず、`bot_token_secret_name` のような参照だけが含まれます。

`/jobs/start` を受けた時も Control Panel の runtime config を再取得し、対象 stream の `stream_discord_configs` から `guild_id`、`voice_channel_id`、`text_channel_id`、`caption_audio_url` を補完します。補完に使うのは `assignment_role=primary` の設定だけです。standby service は failover 候補として登録できますが、primary に昇格するまで start 用の stream config としては使いません。

voice disconnect 後の再参加 policy は bootstrap env を既定値にし、Control Panel の Discord Bot Config に `reconnect_enabled`、`reconnect_max_attempts`、`reconnect_base_delay`、`reconnect_max_delay` がある場合は runtime config を優先します。Gateway disconnect は Discord gateway resume に任せ、Bot 自身の VC 離脱や Opus receive close だけを voice rejoin 対象にします。

Bot は `/services/runtime-secrets/resolve` で、自分の runtime config に参照された bot token だけを解決します。別 service の config や secret は取得できません。

この解決には service token の `service.secret.resolve` scope が必要です。`service.config.read` だけの token は runtime config を読めますが、raw Bot token は取得できません。

Control Panel に登録する capability の `audio_capture` と `audio_stream_forward` は、env secret の有無ではなく runtime secret / stream ingest token に対応した実装能力を表します。標準運用では `DISCORD_BOT_TOKEN` や静的 audio token が env に無くても、Control Panel 管理 config が揃っていれば readiness を満たせます。

## 開発

```powershell
go test ./...
go build ./...
```

## Deployment

- Docker / Compose: `Dockerfile`、`docker-compose.yml`
- Host install: `docs/deployment/host.md`
- systemd unit: `systemd/autostream-discord-bot.service.example`

## Security

- Discord token、service token、audio token を log / API response / docs に出しません。
- Control Panel runtime secret resolve の raw value は process memory 内だけで使い、status response や error message に含めません。
- Encoder/Recorder への token-bearing request は redirect と unsafe HTTP を拒否します。
- Discord token を取得できない場合は dry-run mode で起動し、外部 Discord へ接続しません。
