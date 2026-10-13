# Named agent sessions

Verification: `27f727f80b97f0682b201e87d2d83ff2dc8125e9` and accompanying changes; re-read cited symbols when behavior changes.

[FACT] The existing Rust binary supports explicit connect/read/wait/send/status/
disconnect commands alongside unchanged JSONL version 1. A spawned local Unix
client keeps the existing SASL/TLS/proxy connection across shell invocations.
It has no idle expiry, automatic reconnect or mutation replay. Named startup
requires tags, account-tag, echo-message, batch and history capabilities.
[1](#source-1) [2](#source-2)

[FACT] A private directory holds a stable native owner lock, socket and bounded
state. Ownership/mode checks reject public files/directories and final-component
symlinks. Socket peers must share the UID. State publication uses a new 0600
file, file sync, rename and directory sync; schema/config identity mismatches
fail rather than reset. Local IPC is versioned, size/time bounded and admits at
most sixteen sockets. This is a same-user local interface, not a sandbox against
processes with that user's authority. [2](#source-2) [3](#source-3)

[FACT] `wait` uses read's unread-event predicate without changing the cursor or
last-read cache. One readiness waiter can coexist with a reader; readiness is
not a reservation. No timeout means renewing successful idle IPC waits of at
most sixty seconds; errors/disconnects end the command. Optional timeout zero
checks once. Exit 0 signals readiness; timeout/runtime failures exit 1 with
visible diagnostics. Caller EOF releases the waiter. IPC version 2 requires an
old worker's explicit disconnect before upgrade; saved state remains version 1.
Harness wakeups are outside this CLI. [1](#source-1) [2](#source-2)
[6](#source-6) [8](#source-8)

[FACT] A buffer capped at 512 events and 512 KiB of rendered text retains
compact conversation and membership/control
notices. Typing and routine setup numerics do not end waits. Only one normal
reader waits; the persisted cursor and last batch precede the reply. `--again`
returns that batch even when stopped, until the next normal read replaces it.
Overflow reports GAP. The pinned Rust Unicode tables escape nonprinting text;
identity distinguishes nickname and server account. Up to 512 local reply
references map to server msgids. Unrecognized reply IDs have a distinct `msgid:`
prefix and a visible 128-character display cap. [2](#source-2) [4](#source-4)

[FACT] Send preparation is durable before queueing; only one send awaits
confirmation. A unique client tag is matched together with authenticated account,
nick, target and text. A matching echo confirms; explicit server rejection is
distinct from uncertainty. Timeout closes the connection before a later send
can inherit a late response. The last 32 receipts remain inspectable, including
uncertain text. Losing stdout does not authorize a resend. [2](#source-2)
[4](#source-4)

[FACT] Explicit reconnect queries AFTER saved channel msgids, bounded by six
pages of twenty per channel and twenty seconds overall. Missing references are
reported before a bounded LATEST window. Initial history, page caps and server
errors are visible. Incoming live events during recovery are capped at 512;
recovery deduplication survives eviction from the smaller reply-reference ring.
History echoes can resolve saved uncertain receipts. Private messages and
non-message events have no server catch-up. [5](#source-5) [4](#source-4)

[FACT] Tests exercise real CLI processes, lost-output replay, duplicate text,
read serialization, typing waits, permissions, killed workers/stale sockets, protocol mismatch,
history loss, and socket faults before send/after commit/on echo timeout. They
assert no resend during reconnect and reject a second pending send. Unit tests
cover bounded references/text, escaping, recovery overlap and forged/mismatched
echoes. The fixed transcript measures rendered bytes, not
model tokens, billed usage or total agent turns. [6](#source-6) [7](#source-7)

## Sources

1. <a id="source-1"></a> [src/bin/airc.rs](../../src/bin/airc.rs#L5-L156) — CLI dispatch, JSONL preservation and shutdown.
2. <a id="source-2"></a> [src/agent_session/mod.rs](../../src/agent_session/mod.rs#L125-L588) — process lifecycle, bounded IPC, readiness/reads/sends and failure reporting.
3. <a id="source-3"></a> [src/agent_session/storage.rs](../../src/agent_session/storage.rs#L18-L136) — ownership, native lock, bounded decode and durable publication.
4. <a id="source-4"></a> [src/agent_session/state.rs](../../src/agent_session/state.rs#L57-L423) — compact events, confirmation matching, references, cursor and receipts.
5. <a id="source-5"></a> [src/agent_session/history.rs](../../src/agent_session/history.rs#L6-L194) — `recover`, bounded paging, live staging and overlap regression.
6. <a id="source-6"></a> [tests/named_sessions.rs](../../tests/named_sessions.rs#L145-L706) — named CLI, non-consuming wait/cancellation and network-fault scenarios.
7. <a id="source-7"></a> [src/agent_session/state.rs](../../src/agent_session/state.rs#L425-L578) — state, identity, control escaping and fixed-transcript tests.
8. <a id="source-8"></a> [src/agent_session/mod.rs](../../src/agent_session/mod.rs#L591-L646) — idle renewal, error termination and malformed readiness tests.
