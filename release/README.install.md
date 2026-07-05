# AutoStream Discord Bot Host Install

This archive contains the Linux binary, systemd example, and placeholder environment file for the AutoStream Discord Bot.

## Requirements

- Linux amd64 or arm64 matching the archive name.
- A dedicated `autostream` user and group.
- Discord application credentials supplied outside Git.
- Network access to the Control Panel and Discord.

## Install

```bash
sudo install -o root -g root -m 0755 bin/autostream-discord-bot /usr/local/bin/autostream-discord-bot
sudo ln -sf /usr/local/bin/autostream-discord-bot /usr/local/bin/discord-bot
sudo install -d -o autostream -g autostream /var/lib/autostream/discord-bot
sudo install -o root -g root -m 0644 systemd/autostream-discord-bot.service.example /etc/systemd/system/autostream-discord-bot.service
sudo install -o root -g root -m 0640 .env.example /etc/autostream/discord-bot.env
```

Edit `/etc/autostream/discord-bot.env` with real environment-specific values, then run:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now autostream-discord-bot
```

Do not commit real `.env` files, provider credentials, tokens, logs, screenshots, or verification record.
