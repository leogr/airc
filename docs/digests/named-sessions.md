# Named agent sessions

Verification: `a9d1d9bbf24858c64e1ce2c398ee7a2ec1e0c5a9` and accompanying changes; re-read cited symbols when behavior changes.

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
most sixteen sockets. It is a same-user interface, not a sandbox. [2](#source-2) [3](#source-3)

[FACT] `wait` evaluates the attention engine over retained events without
changing the cursor or last-read cache. Each event carries a persisted class
(context, message with channel/addressed flags, or critical) and an in-process
observation time; restored events have no time and an absent class counts as
critical. Membership notices and routine connection/history labels are context,
local problems critical, chat classified against the session's nick and account
plus a bounded set of its confirmed msgids. A pending reader is served before
the waiter in each loop pass.
[11](#source-11) [12](#source-12) [4](#source-4) [2](#source-2)
The waiter keeps the IPC contract below: One readiness waiter can coexist with a reader; readiness is
not a reservation. No timeout means renewing successful idle IPC waits of at
most sixty seconds; errors/disconnects end the command. Optional timeout zero
checks once. Exit 0 signals readiness; timeout/runtime failures exit 1 with
visible diagnostics. Caller EOF releases the waiter. IPC version 2 requires an
old worker's explicit disconnect before upgrade; saved state version 2 adds
channel state (see [channel controls](named-channels.md)), version 3 adds the
separate delivery cursor (see [delivery](delivery.md)), version 4 is written
only once a [receipt request](delivery-receipts.md) is saved, and older versions
upgrade on explicit connect. Worker-observed registration status and optional handle bookkeeping are covered
by [monitor evidence](monitor.md). Harness wakeups are outside this CLI. [1](#source-1) [2](#source-2)
[6](#source-6) [8](#source-8)

[FACT] A buffer capped at 512 events and 4 MiB of rendered text retains
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

[FACT] Explicit reconnect queries AFTER saved channel msgids for channels it
joined, bounded by six pages of at most twenty msgid-bearing messages per
channel and twenty seconds overall. Only an empty page ends paging, because server pages can be byte-bounded.
Fallback lines without msgids are ingested but neither counted nor used as
anchors. The worker negotiates draft/multiline, so batches arrive assembled. Missing references are
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

[FACT] Multiline sends use the shared builder: newlines or text beyond one
relay line become one batch when negotiated, CR/NUL and over-limit text are
refused locally. The echo deadline grows 12 ms per frame, capped at 50 seconds.
Receipts keep a 512-byte preview and match by size/SHA-256. Records frame
continuation lines with `| `, escaping each; renderings above 1 MiB become an
omission notice. Bounds: 512 events/4 MiB retained, 1 MiB per read (at least one
event), 16 MiB state, 2 MiB IPC, 256 KiB text; `--stdin` drops one final newline. [4](#source-4) [2](#source-2) [9](#source-9) [10](#source-10)

[FACT] Mentions match explicit @nick/@account, standalone @all/@here (see
[delivery](delivery.md)), DMs, verified replies,
and first-line direct/group vocatives with a delimiter inside 64 bytes. Bare
mid-sentence names do not match; no sender-role inference occurs. Named away/back
validate a 1–200-byte reason, serialize with sends and await 306/305; lost replies
are unconfirmed, timeout closes the connection and reconnect never replays the
claim. Peer AWAY is readable context. No wait changes explicit availability or
read cursors. [11](#source-11) [13](#source-13) [14](#source-14)

## Sources

1. <a id="source-1"></a> [src/bin/airc.rs](../../src/bin/airc.rs#L1-L488) — CLI dispatch, JSONL preservation and shutdown.
2. <a id="source-2"></a> [src/agent_session/mod.rs](../../src/agent_session/mod.rs#L467-L1689) — process lifecycle, bounded IPC, readiness/reads/sends and failure reporting.
3. <a id="source-3"></a> [src/agent_session/storage.rs](../../src/agent_session/storage.rs#L18-L136) — ownership, native lock, bounded decode and durable publication.
4. <a id="source-4"></a> [src/agent_session/state.rs](../../src/agent_session/state.rs#L30-L1142) — events with attention stamps, framing, confirmation matching, references, cursor, receipts and `readiness`.
5. <a id="source-5"></a> [src/agent_session/history.rs](../../src/agent_session/history.rs#L465-L694) — `recover`, bounded paging, live staging and overlap regression.
6. <a id="source-6"></a> [tests/named_sessions.rs](../../tests/named_sessions.rs#L172-L768) — named CLI, non-consuming wait/cancellation and network-fault scenarios.
7. <a id="source-7"></a> [src/agent_session/state.rs](../../src/agent_session/state.rs#L1143-L1704) — state, identity, control escaping, framing, version and fixed-transcript tests.
8. <a id="source-8"></a> [src/agent_session/mod.rs](../../src/agent_session/mod.rs#L1690-L1859) — idle renewal, error termination, malformed readiness and request strictness tests.
9. <a id="source-9"></a> [src/multiline.rs](../../src/multiline.rs#L147-L179) — `outbound` single-line versus batch preparation; `src/bin/airc.rs` `read_stdin_text` at L473–488.
10. <a id="source-10"></a> [tests/named_sessions.rs](../../tests/named_sessions.rs#L770-L998) — multiline fallback recovery, multiline send/read/recovery and wake-class regressions.
11. <a id="source-11"></a> [src/agent_session/attention.rs](../../src/agent_session/attention.rs#L1-L1129) — `classify`, `readiness`, debounce/high-water/gap rules and unit tests.
12. <a id="source-12"></a> [src/agent_session/mod.rs](../../src/agent_session/mod.rs#L43-L52) — `Wake` modes; reader-before-waiter evaluation in `serve` at L1120–1600.

13. <a id="source-13"></a> [src/agent_session/mod.rs](../../src/agent_session/mod.rs#L454) — `WaitingAvailability`, `validate`, `call`, confirmed availability and timeout; `state.rs` `ingest` records peer AWAY as context.
14. <a id="source-14"></a> [tests/named_sessions.rs](../../tests/named_sessions.rs#L1000-L1112) — explicit claims, unchanged waits, rejected input, dropped/timed-out acknowledgment and reconnect without replay.
