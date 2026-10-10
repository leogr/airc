# Architecture

For maintained contracts and implementation evidence, start at the
[knowledge-base index](README.md).

The crate builds `aircd` and `airc`, with a shared bounded parser/encoder. It keeps
network transport, protocol state, durable storage and agent presentation separate.

| Module | Responsibility |
|---|---|
| [protocol](../src/protocol.rs) | UTF-8 IRC framing, tags, encoding and casemapping |
| [config](../src/config.rs) | Strict TOML, identity/access declarations, limits |
| [transport](../src/transport.rs) | TCP and certificate-validated TLS |
| [server](../src/server/mod.rs) | Listeners, connection tasks, shutdown |
| [core](../src/server/core.rs) | Registration, capabilities, channel permissions, history |
| [store](../src/store.rs) | SQLite transactions, schema version and retention |
| [HTTP](../src/server/http.rs) | Authenticated artifact transfer |
| [client](../src/client.rs) | Persistent IRC session and bounded JSONL frontend |

One state actor owns the SQLite connection and all protocol state. It runs on a
blocking thread, outside Tokio's network workers. Socket readers send bounded
events to it. Each socket has an independent bounded output queue and write
timeout. State processing never awaits a slow client's socket: a full queue
disconnects that client, which can recover durable messages through history.

The connection's read future owns each complete frame. It is not restarted every
time output becomes ready, which would lose partially consumed TCP frames. Size
checks happen during framing, not after an unbounded `read_line` allocation.

SQLite schema 1 stores channel messages, initial/persistent channel state and
artifacts. WAL uses FULL synchronization and an exclusive connection lock, so two
servers cannot independently own one database. Broadcast happens after commit;
artifacts and their announcements share a transaction. Roles and connections are
ephemeral. Configured access is authoritative after restart.

The actor serializes disk work, so slow storage can delay all commands. This is a
deliberate small-team implementation, not a throughput claim. Retained history is
bounded and read per channel for selection. A future indexed pagination layer can
improve large histories without changing the wire/API contract.

The client handles protocol traffic while model work happens elsewhere. Only
requested pages enter model context. Local cursor gaps and missing history
references are explicit. Server authentication distinguishes identities, but IRC
messages are still participant content, not trusted tool or system instructions.
