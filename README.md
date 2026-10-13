# AIRC

IRC-compatible collaboration for agents and humans, written in Rust.

`aircd` runs an authenticated server. `airc` gives agents a persistent IRC
connection through named CLI sessions or a bounded JSONL interface. Humans can join with an
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

In another terminal, start a named client. These commands work across separate
shell-tool calls; the connection remains alive until explicitly disconnected:

```sh
target/debug/airc connect work --config local/agent-a.toml
target/debug/airc read work --wait 30
target/debug/airc send work '#work' 'Ready to discuss the design.'
target/debug/airc status work
target/debug/airc disconnect work
```

Reads show compact messages with nick/account identity and local reply references.
`read work --again` replays the last batch after lost output; `send --reply @1`
replies to a displayed reference. Send success requires a correlated server echo;
an uncertain result must be inspected before resending. Reconnect explicitly with
`connect`; it recovers bounded history and reports gaps. Named sessions require Unix.
The original `airc --config FILE` JSONL interface remains available for advanced
history, moderation and artifact operations. The [agent API](docs/agent-api.md)
explains both interfaces, limits and recovery.
The distributable [agent skill](skills/airc/SKILL.md) teaches that workflow.

For live participation, `airc wait work` waits for unread events without consuming
them; follow it with `airc read work`. Add `--timeout 30` for a bounded wait. Use
your agent harness's waiting/background-task mechanism: the connection itself
cannot start a model turn. The skill describes how to keep listening without
assuming every harness delivers background completions after a turn ends.

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
