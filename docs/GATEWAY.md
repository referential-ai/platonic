# Platonic Discord gateway: first reply and replay

This guide starts with the Platonic command bundle installed and ends with a
Discord reply recorded in the local ledger and read back with `plato replay`.
Complete the [Quickstart](QUICKSTART.md#0-one-time-setup) first, including one
successful provider-backed run. The
[Discord gateway reference](../README.md#discord-gateway) remains the canonical
description of gateway behavior; this page is the setup walkthrough.

The commands below assume Bash on Unix. Use a test server where you can install
an app, register one small Git workspace with the host server, and use that same
directory for the gateway, TUI, and replay commands.

## 1. Create and install the Discord bot

1. Open the [Discord Developer Portal](https://discord.com/developers/applications),
   select **New Application**, and create the application. New applications
   include a bot user.
2. On the application's **Bot** page, reset and copy the bot token. Treat it as
   a password; do not put it in TOML, shell history, a repository, or proof
   output.
3. Still on **Bot**, enable **Message Content Intent** under **Privileged Gateway
   Intents**, then save. Discord documents this requirement in
   [Gateway Intents](https://docs.discord.com/developers/events/gateway#message-content-intent).
4. On **Installation**, enable **Guild Install** and use a Discord-provided
   install link. Select the `bot` and `applications.commands` scopes.
5. Grant only **View Channels**, **Send Messages**, **Add Reactions**, and
   **Read Message History**. Add **Send Messages in Threads** only if the bot
   will answer in threads. Do not grant **Administrator**. Discord's
   [permissions reference](https://docs.discord.com/developers/topics/permissions)
   defines these flags.
6. Copy the install link, add the app to the test server, and confirm the bot
   can see the channel where you will test it.

Discord's
[first-bot guide](https://docs.discord.com/developers/quick-start/getting-started)
owns the current Developer Portal installation flow.

## 2. Store the token in the approved file

The gateway reads the token from an environment variable, but the durable local
copy belongs at `~/.config/plato/discord-bot-token` with mode `0600`. This Bash
sequence prompts without echoing the token:

```bash
TOKEN_FILE="$HOME/.config/plato/discord-bot-token"
install -d -m 700 "$(dirname "$TOKEN_FILE")"
read -rsp "Discord bot token: " TOKEN
printf '\n'
(umask 077; printf '%s' "$TOKEN" >"$TOKEN_FILE")
unset TOKEN
chmod 600 "$TOKEN_FILE"
test "$(stat -c '%a' "$TOKEN_FILE")" = 600
```

Never print or inspect the token to verify it. A successful final `test`
command verifies the file mode without revealing the contents.

## 3. Add the principal and channel context map

In Discord, enable **User Settings > Advanced > Developer Mode**, then
right-click your own user and select **Copy User ID**. Use the numeric ID of the
human account that will send messages, not the application, bot, server, or
channel ID. Right-click the text channel used for this walkthrough and select
**Copy Channel ID**.

Principal authority must be in the canonical home config
`~/.config/plato/config.toml`:

```toml
[principals.discord."123456789"]
name = "jerome"
```

The quoted key is the Discord user ID. `name` is the stable actor recorded for
gateway-originated approvals and coordinator spawns. The remote ceiling defaults
to `prompt`. Add `remote_ceiling = "yolo"` only for a deliberately high-trust
principal that may control yolo threads.

Create `~/.config/plato/gateway.toml` with the routing configuration selected
explicitly by `--config`. Keep `thread_123` as a placeholder until the next
step starts the server and creates the durable thread:

```toml
[gateway.discord]
api_key_env = "DISCORD_BOT_TOKEN"

[gateway.discord.channel_threads]
"111111111111111111" = "thread_123"
```

Replace the quoted numeric channel ID with the copied test channel ID and
`thread_123` with the existing thread. The token itself does not belong in
either config. `channel_threads` must have at least one entry. A DM must be
added by its numeric DM channel ID too. Channels select thread context only;
they never grant identity authority.

```bash
GATEWAY_CONFIG="$HOME/.config/plato/gateway.toml"
touch "$GATEWAY_CONFIG"
chmod 600 "$GATEWAY_CONFIG"
"${EDITOR:-vi}" "$GATEWAY_CONFIG"
```

Passing this file with `--config` admits gateway routing, but cannot supply
principal authority. The gateway always reads principals from the canonical
home config, ignoring principal definitions in `--config` or `PLATO_CONFIG`.
An auto-discovered workspace `plato.toml` rejects both `[gateway]` and
`[principals]`. The
[configuration reference](../README.md#configuration) owns resolution order
and provider settings. The
[Discord gateway reference](../README.md#discord-gateway) owns channel-mapping
behavior; do not place these settings in an auto-discovered workspace
`plato.toml`.

## 4. Start the server and gateway

Create the scratch workspace once:

```bash
export PLATO_DISCORD_WORKSPACE="$HOME/plato-discord-workspace"
mkdir -p "$PLATO_DISCORD_WORKSPACE"
cd "$PLATO_DISCORD_WORKSPACE"
git init
git -c user.name='Platonic Gateway' \
  -c user.email='gateway@invalid' commit --allow-empty -m 'Initial workspace'
```

In terminal 1, load the configured provider credential as shown in the
[Quickstart](QUICKSTART.md#0-one-time-setup), make sure the Discord token is not
present, and start the one host server:

```bash
cd "$HOME/plato-discord-workspace"
unset DISCORD_BOT_TOKEN
platonic serve
```

Leave it running. Provider credentials belong only in this server environment.
The [platform decision map](https://github.com/referential-ai/platonic-workspace/issues/83)
owns the architecture: the server, not the gateway, owns runs, tools, policy,
approvals, and the ledger.

In terminal 2, register the workspace, use the configured provider credential
to create the mapped thread, then remove provider credentials, expose only the
Discord token, and start the gateway:

```bash
cd "$HOME/plato-discord-workspace"
platonic workspace create discord "$PWD"
# Approve the prompt, then replace thread_123 in gateway.toml with the printed id.
plato thread spawn --model '~openai/gpt-latest' --reasoning-effort none
unset OPENAI_API_KEY OPENROUTER_API_KEY
export DISCORD_BOT_TOKEN="$(tr -d '\r\n' < "$HOME/.config/plato/discord-bot-token")"
platonic gateway discord --workspace "$PWD" \
  --config "$HOME/.config/plato/gateway.toml"
```

Also unset any custom provider credential variable named by your config. The
gateway fails closed if it can see a provider credential. Leave the gateway
running. Before the first Discord REST request or WebSocket connection, both
the wrapper and direct gateway require a bounded server `hello`, the exact
workspace ID, all seven server capabilities consumed by the connector, and a
successful authority readback for every mapped thread.

## 5. Receive the first reply

From the allowlisted human account, send a simple text-only prompt in the test
channel, such as:

```text
Reply with one short greeting.
```

Wait for the bot's final reply before continuing. Messages from identities not
listed in the home principal map are silently denied before channel lookup,
content scanning, daemon access, or effects. An admitted principal in an
unmapped channel is separately ignored. Each mapped channel or DM sends to its
configured durable thread.

## Gateway approvals

If a Discord turn proposes an approval-gated tool, Discord receives a bounded
notification naming the effect and preview. Use `/approve` or `/deny` in that
same channel. The command is bound to the exact pending run and tool-call IDs,
and the home-config principal name is recorded as the actor. The attributed
actor does not grant authority by itself; the pending-operation lookup,
principal ceiling, thread authority, and descendant subset gate still apply.
Typing approval language as ordinary message content has no effect.

## 6. Replay the reply

After the reply is complete, stop the gateway with `Ctrl-C` and unset
`DISCORD_BOT_TOKEN` in that terminal. Replay is an offline direct read and does
not require stopping the host server.

From the same scratch workspace:

```bash
cd "$HOME/plato-discord-workspace"
unset DISCORD_BOT_TOKEN OPENAI_API_KEY OPENROUTER_API_KEY
plato replay
```

The latest session readback should report `final_phase: Finished` and include
the Discord user's message and the bot's final assistant message. Replay makes
no provider request and executes no tool. The
[run-event log reference](../README.md#workspace-ledgers) owns the other replay
forms and ledger details.

## Troubleshooting

### Bot ignores the principal: wrong user ID

An unlisted sender is denied before server access, so there is no reply and no
new run to replay. Copy the sending human account's **User ID** again with
Developer Mode enabled, replace the quoted key under `principals.discord` in
the canonical home config, and restart the gateway. Do not substitute a display
name or an application, bot, server, or channel ID.

### Bot ignores the principal: channel is not mapped

Messages and interactions are accepted only when their numeric channel ID is a
key in `gateway.discord.channel_threads`. Add the text, Discord thread, or DM
channel ID and its durable Platonic thread ID to the authorized gateway config,
then restart the gateway.

### Gateway closes with code 4014: Message Content intent is disabled

Platonic requests Discord's privileged Message Content intent. If it is disabled,
Discord can close the Gateway connection with fatal code `4014`; ordinary guild
messages can also arrive without content and be ignored. On the application's
**Bot** page, enable **Message Content Intent**, save, and restart the gateway.
For verified apps, follow Discord's linked intent-approval requirements.
