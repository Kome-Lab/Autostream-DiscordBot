# AutoStream Discord Bot Host Install

This archive contains the Linux binary, systemd example, and placeholder environment file for the AutoStream Discord Bot.

## Requirements

- Linux amd64 or arm64 matching the archive name.
- `jq`, `sha256sum`, `tar`, `flock`, and systemd. The installer creates the
  dedicated `autostream` service account when it is absent.
- An operator machine with `gh` authenticated to GitHub for release-attestation
  verification. The server does not need `gh`, a GitHub token, or outbound
  GitHub access.
- Discord application credentials supplied outside Git.
- Network access to the Control Panel and Discord.

## Discord Gateway intents

The Bot requests `Guild Voice States`, `Guild Messages`, and `Message Content`
when it connects to the Gateway. `Guild Voice States` is the standard intent
used for VC participant names, avatars, and speaking state; it is distinct from
Discord's privileged Message Content setting.

For Discord chat in the stream overlay, enable **Bot → Privileged Gateway
Intents → `Message Content Intent`** for the application in the Discord
Developer Portal. Restart `autostream-discord-bot` after changing the toggle so
the Gateway session reconnects. If it is disabled, VC participant and speaking
state continue to work, but the chat overlay remains empty.

## Verify and transfer one archive

On the operator machine, download and attest the exact amd64 archive:

```bash
mkdir -p ./autostream-discord-bot-vX.Y.Z
gh release download vX.Y.Z --repo Kome-Lab/Autostream-DiscordBot --pattern 'autostream-discord-bot_vX.Y.Z_linux_amd64.tar.gz' --dir ./autostream-discord-bot-vX.Y.Z
gh attestation verify ./autostream-discord-bot-vX.Y.Z/autostream-discord-bot_vX.Y.Z_linux_amd64.tar.gz --repo Kome-Lab/Autostream-DiscordBot --signer-workflow Kome-Lab/Autostream-DiscordBot/.github/workflows/release-host.yml --deny-self-hosted-runners
scp ./autostream-discord-bot-vX.Y.Z/autostream-discord-bot_vX.Y.Z_linux_amd64.tar.gz user@server:/tmp/
```

Transfer only `autostream-discord-bot_vX.Y.Z_linux_amd64.tar.gz`. Do not
transfer an archive checksum sidecar, `release-manifest.json`, or its sidecar.
The automatic updater still uses those separately published compatibility
assets, but this manual installer never reads them.

## Install on the server

Move the uploaded archive into a root-owned staging directory:

```bash
sudo install -d -o root -g root -m 0755 /opt/autostream/releases
sudo install -d -o root -g root -m 0755 /opt/autostream/releases/artifacts
sudo install -o root -g root -m 0644 /tmp/autostream-discord-bot_vX.Y.Z_linux_amd64.tar.gz /opt/autostream/releases/artifacts/autostream-discord-bot_vX.Y.Z_linux_amd64.tar.gz
cd /opt/autostream/releases/artifacts
```

Extract that root-owned archive without renaming its top-level directory. Keep
the unchanged archive beside the extracted directory until installation
finishes:

```bash
sudo test ! -e autostream-discord-bot_vX.Y.Z_linux_amd64
sudo test ! -L autostream-discord-bot_vX.Y.Z_linux_amd64
sudo tar --no-same-owner --no-same-permissions -xzf autostream-discord-bot_vX.Y.Z_linux_amd64.tar.gz
cd autostream-discord-bot_vX.Y.Z_linux_amd64
sudo ./install-autostream-discord-bot
```

For arm64, replace `amd64` with `arm64` in the archive and directory names.

The installer fixes a stable root-owned copy of the adjacent archive, enforces
the archive size limit, records its calculated SHA-256, and verifies archive
layout, inner checksums, exact `artifact-manifest.json`, architecture, and the
binary version, commit, and build date before persistent host mutation. Missing,
stale, or corrupt external checksum and release-manifest files are ignored. It
seeds the verified rollback release, preserves an existing environment file
byte for byte, installs the systemd unit, and exposes
`/usr/local/bin/autostream-discord-bot` plus the `discord-bot` alias. It does
not install packages, write Node configuration, or start the service.
Legacy public binaries are backed up only under the root-owned
`/var/backups/autostream/install-migrations/discord-bot` tree.

`/opt/autostream/discord-bot/releases` and its `current` link are
installer-owned implementation details used by managed update and rollback.
Do not create or edit their release directories or marker files manually.

Edit `/etc/autostream/discord-bot.env` with operational environment-specific
values. The panel-managed node config must select
`listener.credential: node-listener.json`. The systemd unit loads the
root-owned `/opt/autostream/local-executor/ports/discord-bot.json` source as
that credential. Its exact schema is `schema_version`, `service_type`,
`bind_address`, and `config_revision`; use schema version `2`, service type `discord_bot`, a
positive revision, and an unprivileged bind port from `1024` through `65535`.
The documented host endpoint is `127.0.0.1:8083`. Public `api.host` /
`api.port` values are independent and never fall back as the local listener.
A missing or invalid credential stops Discord Bot before it listens. Keep the
non-listener environment file root-owned and mode `0640`.

In Control Panel, create a `discord_bot` Node and run the exact one-time Auto
Configure command generated by the Panel. It writes the Node identity and
runtime token to `/etc/autostream-discord-bot/config.yml`.

Then start and inspect the service:

```bash
sudo systemctl enable --now autostream-discord-bot
sudo systemctl status autostream-discord-bot
autostream-discord-bot --version
```

When migrating an installation that is already active, restart it after the
installer finishes instead of using the first-start command:

```bash
sudo systemctl restart autostream-discord-bot
sudo systemctl status autostream-discord-bot
autostream-discord-bot --version
```

Use the host and port configured by node config when checking
`/health` and `/updater/version`. This guide intentionally avoids the older
variable-heavy probe forms `PROBE_HOST="${PROBE_HOST:-127.0.0.1}"` and
`PROBE_HOST='[::1]'`; for IPv6, keep the address in brackets in the URL.

`/updater/version` is the unauthenticated, minimal endpoint used only to prove
the running binary and local service identity to the update helper. Its exact
response fields are version, service_id, service_type, and config_revision.
Block this exact path at any public reverse proxy.

Do not fabricate `.artifact-sha256`, `.version`, `artifact-manifest.json`, or
`checksums.txt` from a local binary. Publish a new immutable release instead of
modifying an existing release asset.

Do not commit real `.env` files, provider credentials, tokens, logs, screenshots, or verification record.
