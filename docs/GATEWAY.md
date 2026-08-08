# Discord gateway: first reply and replay

This guide starts with a working Plato Agent installation and ends with a
Discord reply recorded in the local ledger and read back with `plato replay`.
Complete the [Quickstart](QUICKSTART.md#0-one-time-setup) first, including one
successful provider-backed run. The
[Discord gateway reference](../README.md#discord-gateway) remains the canonical
description of gateway behavior; this page is the setup walkthrough.

The commands below assume Bash on Unix. Use a test server where you can install
an app, and use the same empty workspace directory for the daemon, gateway, TUI,
and replay commands.

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

## 3. Add the numeric owner and channel allowlists

In Discord, enable **User Settings > Advanced > Developer Mode**, then
right-click your own user and select **Copy User ID**. Use the numeric ID of the
human account that will send messages, not the application, bot, server, or
channel ID. Right-click the text channel used for this walkthrough, select
**Copy Channel ID**, and choose a provider config already proven by the
[Quickstart](QUICKSTART.md#0-one-time-setup) for runs from that channel.

Use an authorized Plato config selected explicitly with `--config`. Create
`~/.config/plato/gateway.toml`, retain the provider settings already proven in
the [Quickstart configuration](QUICKSTART.md#0-one-time-setup), and add:

```toml
[gateway.discord]
api_key_env = "DISCORD_BOT_TOKEN"
owner_user_ids = [123456789]

[gateway.discord.channel_configs]
"111111111111111111" = "~/.config/plato/test-channel.toml"
```

Replace the first example with your user ID; it must remain an unquoted,
positive TOML integer. Replace the quoted numeric channel ID with the copied
test channel ID, and replace the mapped path with the mode-`0600` authorized
provider config for that channel. The token itself does not belong in either
config. `channel_configs` must have at least one entry and is the gateway's
complete channel allowlist. A DM must be added by its numeric DM channel ID too;
an owner message or interaction in any unmapped channel is ignored before
content scanning or remote and daemon side effects.

```bash
GATEWAY_CONFIG="$HOME/.config/plato/gateway.toml"
touch "$GATEWAY_CONFIG"
chmod 600 "$GATEWAY_CONFIG"
"${EDITOR:-vi}" "$GATEWAY_CONFIG"
```

Passing this file with `--config` makes it an authorized config. The entire
`[gateway]` table, including the token variable name and owner IDs, is rejected
from an auto-discovered workspace `plato.toml`. The
[configuration reference](../README.md#configuration) owns resolution order
and provider settings. The
[Discord gateway reference](../README.md#discord-gateway) owns channel-mapping
behavior; do not place these settings in an auto-discovered workspace
`plato.toml`.

## 4. Start the daemon and gateway

Create the scratch workspace once:

```bash
export PLATO_DISCORD_WORKSPACE="$HOME/plato-discord-workspace"
mkdir -p "$PLATO_DISCORD_WORKSPACE"
cd "$PLATO_DISCORD_WORKSPACE"
```

In terminal 1, load the configured provider credential as shown in the
[Quickstart](QUICKSTART.md#0-one-time-setup), make sure the Discord token is not
present, and start the daemon:

```bash
cd "$HOME/plato-discord-workspace"
unset DISCORD_BOT_TOKEN
platonic serve
```

Leave it running. Provider credentials belong only in this daemon environment.
The [runtime topology](ARCHITECTURE.md#runtime-topology) explains why the daemon,
not the gateway, owns runs, tools, policy, approvals, and the ledger.

In terminal 2, expose only the Discord token and start the gateway from the same
workspace:

```bash
cd "$HOME/plato-discord-workspace"
unset OPENAI_API_KEY OPENROUTER_API_KEY
export DISCORD_BOT_TOKEN="$(tr -d '\r\n' < "$HOME/.config/plato/discord-bot-token")"
platonic gateway discord --config "$HOME/.config/plato/gateway.toml"
```

Also unset any custom provider credential variable named by your config. The
gateway fails closed if it can see a provider credential. Leave the gateway
running. Before the first Discord REST request or WebSocket connection, both
the wrapper and direct gateway require a bounded daemon `hello` with the exact
workspace ID and all six daemon capabilities consumed by the connector.

## 5. Receive the first reply

From the allowlisted human account, send a simple text-only prompt in the test
channel, such as:

```text
Reply with one short greeting.
```

Wait for the bot's final reply before continuing. Messages from every other
user ID are silently ignored. During one gateway process, the first allowed
message in a mapped channel or DM starts one daemon session; later messages in
that same channel or DM continue that session. A different mapped channel or DM
starts a separate session.

## Local-only approvals

Use a read-only prompt for the first-reply check so no approval is needed. If a
later Discord run proposes an approval-gated tool, Discord receives a bounded
notification, but it cannot grant or deny the request. Attach a TUI on the same
machine and workspace:

```bash
plato-tui --workspace "$HOME/plato-discord-workspace"
```

Grant or deny in that local TUI. The gateway never sends an approval decision;
typing approval language into Discord has no effect. See the
[Quickstart approval boundary](QUICKSTART.md#2-test-the-approval-boundary) and
[TUI controls](../README.md#tui) for the canonical behavior.

## 6. Replay the reply

After the reply is complete, stop the gateway with `Ctrl-C`, unset
`DISCORD_BOT_TOKEN` in that terminal, and stop the daemon with `Ctrl-C`. Replay
is an offline direct read, so the daemon must release its workspace lock first.

From the same scratch workspace:

```bash
cd "$HOME/plato-discord-workspace"
unset DISCORD_BOT_TOKEN OPENAI_API_KEY OPENROUTER_API_KEY
plato replay
```

The latest session readback should report `final_phase: Finished` and include
the Discord user's message and the bot's final assistant message. Replay makes
no provider request and executes no tool. The
[SQLite ledger reference](../README.md#sqlite-ledgers) owns the other replay
forms and ledger details.

## Troubleshooting

### Bot ignores the owner: wrong owner ID

An unlisted sender is ignored before daemon access, so there is no reply and no
new run to replay. Copy the sending human account's **User ID** again with
Developer Mode enabled, replace the numeric `owner_user_ids` entry, and restart
the gateway so it reloads the config. Do not substitute a display name or an
application, bot, server, or channel ID.

### Bot ignores the owner: channel is not mapped

Messages and interactions are accepted only when their numeric channel ID is a
key in `gateway.discord.channel_configs`. Add the text, thread, or DM channel ID
and its provider config path to the authorized gateway config, then restart the
gateway.

### Gateway closes with code 4014: Message Content intent is disabled

Plato requests Discord's privileged Message Content intent. If it is disabled,
Discord can close the Gateway connection with fatal code `4014`; ordinary guild
messages can also arrive without content and be ignored. On the application's
**Bot** page, enable **Message Content Intent**, save, and restart the gateway.
For verified apps, follow Discord's linked intent-approval requirements.
