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
- participant / active speaker / chat 状態を、stream job で指定された Worker へ送る。
- Control Panel が YouTube live を確定した後、runtime config で指定された stream の Discord text channel へ視聴 URL を冪等に投稿する。

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
AUTOSTREAM_BIND_ADDR=127.0.0.1:8083
AUTOSTREAM_CONFIG_REVISION=1
TZ=Asia/Tokyo
```

`AUTOSTREAM_CONFIG_REVISION` is a root-owned positive integer used by the local
executor to bind `/updater/version` to the applied service configuration.
It defaults to `1`; increment it after a configuration change. Invalid, signed,
fractional, padded, zero, or negative values stop Discord Bot before it starts
serving HTTP.

The Control Panel local executor writes managed bind/revision overrides to
`/opt/autostream/local-executor/ports/discord-bot.env`. systemd loads this optional root-owned
sidecar after `discord-bot.env`, so managed values win without breaking
existing hosts where the sidecar does not exist.

systemd 版の待受ポートは `/etc/autostream/discord-bot.env` の
`AUTOSTREAM_BIND_ADDR` で変更できます。ポートは非特権範囲の
`1024`～`65535` を指定してください。標準の env ファイルは IPv4
loopback の `127.0.0.1:8083` を明示します。変数自体がない既存環境では、
アップグレードだけでポートを移動しないようバイナリの従来値
`127.0.0.1:8080` を維持します。
例えば `127.0.0.1:18083` に変更した場合、`/health` と
`/updater/version` も同じ `18083` で待ち受けます。不正な形式、範囲外、
または特権ポートを指定した場合は Discord Bot が起動時に停止します。
IPv6 loopback を明示的に使う場合は `[::1]:18083` のように角括弧を含めて
指定し、プローブURLも `http://[::1]:18083/...` とします。

Docker 版ではホスト公開ポートを `AUTOSTREAM_DISCORD_BOT_PORT`、
コンテナ内の待受ポートを `AUTOSTREAM_DISCORD_BOT_CONTAINER_PORT` で
個別に変更できます。既定値はそれぞれ `8083` と `8080` で、どちらも
`1024`～`65535` を使用してください。

Compose published ports are a host/reverse-proxy responsibility. The Control
Panel Updater manages only host ports `1024` through `65535`; manually
publishing a privileged or conflicting Docker host port is outside the managed
update contract.

The production health authority is the host Local Executor. These Compose files
intentionally omit an in-container `healthcheck`: the runtime image has no
purpose-built HTTP probe client, and the image contract does not add or repurpose `curl`, `wget`, or another unrelated executable solely for container health.
For managed Docker changes, the Local Executor probes the loopback published port for both `/health` and `/updater/version`; the published port is the health port.
A recreate is accepted only when health, service identity, version, and
`AUTOSTREAM_CONFIG_REVISION` match; otherwise the executor rolls back or reports
`rollback_failed`.

```powershell
$env:AUTOSTREAM_DISCORD_BOT_PORT = "18083"
$env:AUTOSTREAM_DISCORD_BOT_CONTAINER_PORT = "18080"
docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d
```

Production mode sets `AUTOSTREAM_ENV=production` or `AUTOSTREAM_REQUIRE_CONTROL_PANEL_RUNTIME_CONFIG=true`. In that mode the Discord Bot must register with Control Panel, fetch service-scoped runtime config, resolve `bot_token_secret_name` through `/services/runtime-secrets/resolve`, and initialize a real Discord client. It does not fall back to `DISCORD_BOT_TOKEN` or dry-run mode when Control Panel runtime config or runtime secret resolution fails. Dry-run Discord mode is for local migration checks only.

When the runtime config provider is configured, `/jobs/start` fails closed if the Control Panel runtime config refresh fails. Request-supplied guild or voice channel IDs do not bypass the saved stream Discord config. Stream auto-start candidates are distributed only when the selected Discord Bot Config points to this service ID.

Inbound Control Panel dispatch uses `SERVICE_CONTROL_TOKEN` or `SERVICE_CONTROL_TOKEN_SHA256`. `CONTROL_PANEL_TOKEN` is outbound-only; in production or runtime-config-required mode it must not authorize `/jobs/start`, `/jobs/{id}/stop`, or status mutation endpoints.

Control Panel の Discord Bot Config に `bot_token`、`guild_id`、`voice_channel_id`、`text_channel_id`、caption/STT 設定を登録してください。`DISCORD_BOT_TOKEN`、`DISCORD_GUILD_ID`、`DISCORD_VOICE_CHANNEL_ID` は互換 fallback または local dry-run 用に限定します。

## Discord Gateway Intents

Bot は Gateway 接続時に `Guild Voice States`、`Guild Messages`、`Message Content` を要求します。`Guild Voice States` は VC参加者、名前・アイコン、発話状態を取得するための通常 Intent です。Discord Developer Portal の privileged toggle ではありません。

配信オーバーレイへ Discord チャットを表示するには、コード側の `Guild Messages` と `Message Content` に加えて、Discord Developer Portal の **Bot → Privileged Gateway Intents → `Message Content Intent`** を有効にしてください。有効化後は `autostream-discord-bot` を再起動して Gateway へ再接続します。これを無効にしたままでも VC参加者、発話状態、VC音声は動作しますが、チャットだけが空のままになります。

## Runtime Config

起動時に service token で `/services/register` を呼び、その後 `/services/runtime-config?service_id=<SERVICE_ID>` を取得します。runtime config には raw secret は含まれず、`bot_token_secret_name` のような参照だけが含まれます。

`/jobs/start` を受けた時も Control Panel の runtime config を再取得し、対象 stream の `stream_discord_configs` から `guild_id`、`voice_channel_id`、`text_channel_id` を補完します。Streams で選ばれた Discord Bot Config の `service_id` がこの Bot service と一致する待機streamが候補になります。Control Panel は VC参加による開始要求を受けた時、保存済みconfig、`streams.start` scope、auto-start trigger、待機状態を確認し、開始直前に対象streamへ primary Discord Bot assignment を作成します。明示的に別Botが primary assigned されているstreamは勝手に上書きしません。

voice disconnect 後の再参加 policy は bootstrap env を既定値にし、Control Panel の Discord Bot Config に `reconnect_enabled`、`reconnect_max_attempts`、`reconnect_base_delay`、`reconnect_max_delay` がある場合は runtime config を優先します。Gateway disconnect は Discord gateway resume に任せ、Bot 自身の VC 離脱や Opus receive close だけを voice rejoin 対象にします。

Bot は `/services/runtime-secrets/resolve` で、自分の runtime config に参照された bot token だけを解決します。別 service の config や secret は取得できません。

この解決には service token の `service.secret.resolve` scope が必要です。`service.config.read` だけの token は runtime config を読めますが、raw Bot token は取得できません。

Control Panel に登録する capability の `audio_capture`、`audio_stream_forward`、`caption_audio_forward` は、env secret の有無ではなく runtime secret / job-scoped token に対応した実装能力を表します。標準運用では env に固定の audio token を置かず、Control Panel 管理 config と `/jobs/start` の `stream_ingest_token` / `caption_audio_token` で転送します。

Worker event の送信先と token は配信枠で primary assigned された Worker から Control Panel が解決し、`/jobs/start` の `worker_events_url` / `worker_events_token` として渡します。Discord Bot の env に固定の `WORKER_URL` や `WORKER_TOKEN` は置きません。

字幕音声の送信先は、配信枠で選択された primary WorkerからControl Panelが解決し、`/jobs/start` の `caption_audio_url` として渡します。認証には同じジョブで渡される短命な `caption_audio_token` だけを使い、送信先やtokenをDiscordプロファイル、runtime config、status、logには保存しません。

## YouTube Live Notification API

`POST /streams/{id}/notifications/youtube-live` は inbound service token を必須とし、次の JSON だけを受け付けます。channel ID は request から受け取らず、毎回 Control Panel runtime config の primary assignment と `text_channel_id` を再検証します。

```json
{
  "event_id": "youtube-live-stream-01-20260715T120000Z",
  "watch_url": "https://www.youtube.com/watch?v=example"
}
```

送信前に同じ stream の live job が稼働中であり、その job の text channel が runtime config と一致することを確認します。`watch_url` は HTTPS の `youtube.com`、`www.youtube.com`、`m.youtube.com`、`youtu.be` だけを許可し、userinfo、port、fragment を拒否します。Discord message は固定文面と URL のみで、allowed mentions はすべて無効です。

成功時は `200 OK` で `status`、`message_id`、`already_sent` を返します。同一 process 内で同じ `event_id` の成功 receipt を保持するため、再送では Discord へ再投稿せず `already_sent: true` を返します。Discord rate limit は `429` と `Retry-After`、一時障害は retryable な `502`、権限不足は `403` と `discord_missing_permissions` を返します。

## 開発

```powershell
go test ./...
go build ./...
```

## Linux DAVE build

The production voice binary uses the pinned DiscordGo fork in
`third_party/discordgo` and its nested `libdave` submodule. Clone with
recursive submodules; Linux CI and release Actions build the native library
before running Go tests or producing artifacts. A plain checkout without
recursive submodules cannot build the production voice binary. Local Windows
validation is not release proof; use the GitHub-hosted Linux CI job for the
CGO/libdave build.

## Deployment

- Docker / Compose: `Dockerfile`、`docker-compose.yml`
- systemd unit: `systemd/autostream-discord-bot.service.example`
- Detailed deployment, Discord audio, and security documentation is maintained in the `autostream-docs` repository.

## Security

- Discord token、service token、audio token を log / API response / docs に出しません。
- Control Panel runtime secret resolve の raw value は process memory 内だけで使い、status response や error message に含めません。
- Encoder/Recorder への token-bearing request は redirect と unsafe HTTP を拒否します。
- Discord token を取得できない場合は dry-run mode で起動し、外部 Discord へ接続しません。
