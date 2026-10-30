# Codex app-server delivery

Verification: `8a03beadfa18dfcc958711652547e1acdc3569c8` and accompanying changes.

[FACT] `airc-codex --app-server --app-server-socket PATH` is separate from
and conflicts with `--wait-for-idle`. The existing queue mode remains the
default. It requires the direct subscriber parent, delivery ID and private
`AIRC_DELIVERY_STATE_DIR`; ordinary size/header checks precede dispatch.
[1](#source-1) [4](#source-4)

[FACT] The explicit endpoint must be an owned 0600 Unix socket named by 64
lowercase hex characters directly under the current user's owned 0700
`<canonical /tmp>/codex-daemon-<euid>` directory. Its device/inode is captured,
peer UID is checked, and the path identity is rechecked before mutation.
No endpoint scan, daemon creation, thread resume or execution-setting override
is attempted. WebSocket frames/messages are capped at 4 MiB; connect, handshake,
write and response each have a 5-second bound. [2](#source-2)

[FACT] Initialization requests the experimental API. `thread/read` must return
the exact assigned thread as idle or active. Idle selects `turn/start`. Active
discovers one `inProgress` ID through at most 64 pages of 16 turns, refusing
unsupported statuses, malformed/repeated IDs or invalid cursors, then selects
`turn/steer` with `expectedTurnId`. Notifications and server requests, including
approval requests, are ignored without answering or logging them. [2](#source-2)

[FACT] Before a mutation frame, the adapter rechecks its parent and endpoint and
syncs the batch/thread/endpoint/method/request/expected-turn identity. Only the
exact `-32600`, `no active turn to steer` rejection establishes no effect and
allows one reclassification. A start receipt must name an in-progress turn;
a steer receipt must name the expected turn. A matching receipt is synced before
exit 0. All other post-submission errors remain uncertain, with no queue fallback,
interrupt or automatic resend. [2](#source-2) [3](#source-3)

[FACT] Replay protection is a separate strict version-1 ledger, leaving named
state schemas unchanged. The private directory is opened without following its
final component, records use descriptor-relative no-follow I/O, and one stable
owner lock covers the invocation. Writes use exclusive temporary files, file
sync, rename and directory sync. Records are capped at 4 KiB and the directory
at 65,536 entries, refusing rather than evicting old protection. Records contain
no message body or credentials. [3](#source-3)

[FACT] An accepted batch replay settles from its saved receipt without opening a
socket. An in-flight record, changed target or unreadable/corrupt record refuses
without submission. Batch UUID and target thread bind replay; the stored
original-payload digest is audit evidence because explicit retry rendering can
change the header/settings notice. Signals before submission are unsent; after
permission they are uncertain and close only the client transport. [2](#source-2)
[3](#source-3)

[FACT] Simulated Unix WebSocket tests cover routes, pagination, completion and
replacement races, retained receipts/uncertainty, malformed answers, socket
replacement, private storage and journal exclusivity. Existing queue tests stay
in place. These tests do not establish live Codex approval routing, UI visibility
or a processing deadline. The client answers no approval requests; approval
routing remains untested. [5](#source-5) [6](#source-6) [7](#source-7)

## Sources

1. <a id="source-1"></a> [Adapter](../../src/bin/airc-codex.rs#L1) — `Args`, `main`, existing batch/parent validation and queue route.
2. <a id="source-2"></a> [App-server client](../../src/bin/airc-codex/app_server.rs#L28-L498) — `Identity`, `Rpc`, `route`, `deliver`, `run`.
3. <a id="source-3"></a> [Journal](../../src/bin/airc-codex/app_server/journal.rs#L43-L242) — `Attempt::validate`, `Journal::open`, `load`, `save`.
4. <a id="source-4"></a> [Subscriber](../../src/agent_session/mod.rs#L955-L1077) — `SinkBatch`, `run_sink`, canonical state directory environment.
5. <a id="source-5"></a> [New route tests](../../src/bin/airc-codex/app_server/tests.rs#L1) — simulated server and journal tests.
6. <a id="source-6"></a> [Default adapter tests](../../tests/codex_adapter.rs#L1-L319) — unchanged queue route and optional wait-mode integration tests.

7. <a id="source-7"></a> [Adapter process tests](../../tests/codex_app_server.rs#L125-L275) — accepted replay, ordered start/steer and SIGTERM after mutation, without a live Codex conversation.
