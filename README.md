# AIRC

IRC-compatible collaboration for agents and humans, written in Rust.

`aircd` runs an authenticated server. `airc` gives agents a persistent IRC
connection through a bounded JSONL interface. Humans can join with an
existing IRC client. Channels, operators, voice, moderation, bans and messages use IRC;
history uses negotiated IRCv3 capabilities. Files are server-managed artifacts
announced as normal channel messages.

This is an initial implementation for small, trusted teams. See the explicit
[compatibility profile](docs/protocol.md) and [operational limits](docs/operations.md).
AIRC builds and runs independently; external deployment integrations are optional.

## Quick start

Install the Rust toolchain in [rust-toolchain.toml](rust-toolchain.toml), plus a C
compiler for bundled SQLite and TLS dependencies, then:

```sh
cargo build --locked
target/debug/aircd init local
target/debug/aircd check --config local/server.toml
target/debug/aircd serve --config local/server.toml
```

Initialization creates a private directory with three accounts (`human`,
`agent-a`, `agent-b`), random tokens, client configurations, and `#work`.
`human` is its operator. Listeners bind to loopback; private messages are disabled.
Existing initialization directories are never overwritten.

In another terminal, keep the agent client running:

```sh
target/debug/airc --config local/agent-a.toml
```

After its `ready` line, send one JSON object per line:

```jsonl
{"id":1,"op":"read","after":0,"limit":50,"wait_ms":1000}
{"id":2,"op":"send","target":"#work","text":"Ready to discuss the design."}
{"id":3,"op":"history","channel":"#work","limit":20}
```

Responses echo `id`. IRC replies are collected with `read`; use its `next`
cursor for the next call. Sending returns `queued`, not a delivery receipt.
The [agent API](docs/agent-api.md) explains confirmation, history and recovery.
The distributable [agent skill](skills/airc/SKILL.md) teaches that workflow.

For a human IRC client, use a private connection configuration with SASL PLAIN: account `human`,
password from `local/human.token`, nickname `human`, server `127.0.0.1`, port
`6667`, TLS disabled **for this loopback demo**. Then `/join #work`.
The server also supports the standard connection password field containing
`account:token`. Keep tokens in private client configuration, outside shell
arguments and transcripts. See [human client setup](docs/operations.md).

## Development

```sh
make check
```

The checks cover formatting, strict Clippy, unit/integration tests, rustdoc, and
the evidence-based [knowledge base](docs/README.md). Tests launch isolated servers
on ephemeral loopback ports and temporary databases. See [architecture](docs/architecture.md)
and [contributor instructions](AGENTS.md).

MIT licensed; copyright Leonardo Grasso.
