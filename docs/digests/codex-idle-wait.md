# Codex wait-for-idle delivery

Verification: `f5b539c057a9ab62763dbef4166f0a4a41218a3e` and accompanying changes.

[FACT] `airc-codex --wait-for-idle` is opt-in; without it the adapter runs
`codex queue` at once as before and installs no signal handlers.
`--idle-timeout S` requires it; 0, the default, waits until idle, a signal or
a definite refusal. The batch, header ID and subscriber parent are checked
first; only then does the wait start. [1](#source-1) [2](#source-2)

[FACT] The endpoint is `$CODEX_HOME/app-server-control/app-server-control.sock`
(home defaulting to `~/.codex`). It must be a symlink owned by the effective
user whose target is exactly `<canonical /tmp>/codex-daemon-<euid>/<sha256 of
the canonical alias path>`. That directory must be a 0700 directory owned by
the user, and the target a socket owned by the user. A missing alias, Codex
home, directory or socket is transient. Any other mismatch is a definite
refusal. No process scan, other home, remote endpoint, daemon start or restart
is attempted; this matches Codex's own local-daemon check. [3](#source-3)
[7](#source-7) [8](#source-8) [11](#source-11)

[FACT] Each attempt connects with a 5 s bound, requires the peer UID to equal
the effective UID, re-resolves the socket identity (device and inode), and
performs a WebSocket handshake to `ws://localhost/rpc` with 4 MiB frame and
message limits. It sends `initialize` (stable capabilities, no opt-outs),
`initialized`, then `thread/read {threadId, includeTurns: false}`; replies
have a 5 s bound. JSON-RPC carries no `jsonrpc` field. [3](#source-3)
[9](#source-9) [10](#source-10)

[FACT] Only a matching reply whose `thread.id` is the target and whose
`status.type` is `idle` releases the batch; `active` waits. `notLoaded`,
`systemError`, other types, another thread, a reply without a thread, an RPC
error, a reply to an unknown ID, malformed JSON and binary messages are definite
refusals. A `thread/status/changed` for the target marks the status stale, and
so does a 30 s timer that notifications do not reset; either sends a fresh
read, never trusting the event's status. Other notifications and server
requests are dropped without being answered or logged. [3](#source-3)

[FACT] On idle, the adapter rechecks its subscriber parent (gone: refusal) and
the socket identity (changed: transient), then closes the connection.
Connect, handshake and read deadlines, EOF, reset and a stream dropped without
a close frame are transient. The same adapter and batch retry after 1, 2, 4,
8, 15, then every 30 s, reset by a valid status read, not by an accepted
connection. A positive `--idle-timeout` is absolute across reconnects and
backoff; one too large for a deadline is refused before connecting. Each new transient reason is printed once. [3](#source-3)
[4](#source-4)

[FACT] Refusal, deadline or a SIGTERM, SIGINT or SIGHUP during the wait exits 3
without starting `codex queue`. After release, the same argv, 30 s queue
deadline, receipt match and exit 0/4 classification apply. A signal during the
queue call kills and reaps it and exits 4. [1](#source-1) [2](#source-2)

[FACT] `subscribe --sink-timeout 0` sets no adapter deadline (positive values
stay 1–3600, default 60; `wait --deliver` keeps 60). Until the adapter is
reaped, dropping its future, as when the subscriber handles SIGTERM or SIGINT,
kills the adapter's own process group; descendants that left it, or a subscriber
killed by SIGKILL, are not covered. Bounded sinks keep killing only the direct child.
The consumer connection has no read deadline while a sink runs, so the
consumer stays registered. [5](#source-5) [6](#source-6)

[FACT] While a batch is held, later mentions stay above the delivery cursor;
the subscribe test shows the held batch alone, then one batch carrying them in
order. Nothing reserves idle or reads queue depth between the idle read and the
queue call. [3](#source-3) [6](#source-6)

[FACT] Tests use a mock Unix WebSocket server in the Codex layout: initialize
and read order, already idle, idle event, quiet recheck, refusals, hang-up and
silent connections, a missing then bound socket, dangling alias deadline, an
oversized deadline,
unsafe endpoints, a replaced socket, a gone subscriber, idle-to-active racing
and the full adapter flow with a fake `codex`. Binary tests cover the flag
pairing, deadline, oversized deadline and signals while waiting. A subscribe test covers the
deadline-free hold, the two-batch catch-up, group kill and unchanged bounded
cleanup. No live Codex daemon is exercised. [2](#source-2) [4](#source-4)
[6](#source-6)

## Sources

1. <a id="source-1"></a> [src/bin/airc-codex.rs](../../src/bin/airc-codex.rs#L1-L419) — `Args`, `deliver`, `wait_for_idle`, `run_bounded` with `cancel`.
2. <a id="source-2"></a> [tests/codex_adapter.rs](../../tests/codex_adapter.rs#L220-L319) — `spawn_waiting` and wait-mode binary tests; flow tests in `src/bin/airc-codex.rs` from L565.
3. <a id="source-3"></a> [src/bin/airc-codex/idle.rs](../../src/bin/airc-codex/idle.rs#L1-L520) — `Endpoint::resolve`, `wait`, `observe`, `thread_status`, `Rpc`, `classify`, failure classes.
4. <a id="source-4"></a> [src/bin/airc-codex/idle.rs](../../src/bin/airc-codex/idle.rs#L522-L972) — mock server and unit tests.
5. <a id="source-5"></a> [src/agent_session/mod.rs](../../src/agent_session/mod.rs#L727-L744) — `DeliverOptions::sink_timeout`; validation at L791–795; `run_sink` and `ProcessGroup` at L949–1069.
6. <a id="source-6"></a> [tests/named_sessions.rs](../../tests/named_sessions.rs#L3995-L4167) — `unbounded_sink_holds_its_batch_and_its_group_stops_with_the_subscriber`.
7. <a id="source-7"></a> [Codex rust-v0.164.0 `app_server_control_socket_path`](https://raw.githubusercontent.com/openai/codex/rust-v0.164.0/codex-rs/app-server-transport/src/transport/mod.rs) — alias path under the Codex home.
8. <a id="source-8"></a> [Codex rust-v0.164.0 `shared_daemon_socket_directory`](https://raw.githubusercontent.com/openai/codex/rust-v0.164.0/codex-rs/uds/src/daemon_directory.rs) — `/tmp/codex-daemon-<euid>`, mode 0700.
9. <a id="source-9"></a> [Codex rust-v0.164.0 JSON-RPC types](https://raw.githubusercontent.com/openai/codex/rust-v0.164.0/codex-rs/app-server-protocol/src/rpc.rs) — no `jsonrpc` field; optional params.
10. <a id="source-10"></a> [Codex rust-v0.164.0 `thread/read`](https://raw.githubusercontent.com/openai/codex/rust-v0.164.0/codex-rs/app-server/src/request_processors/thread_processor.rs) — status overlay from the thread watch manager.
11. <a id="source-11"></a> [Codex rust-v0.164.0 `protected_socket_path` and `verified_local_daemon_socket`](https://raw.githubusercontent.com/openai/codex/rust-v0.164.0/codex-rs/app-server-transport/src/transport/unix_socket.rs) — alias target hash and directory check; compression only on request.
