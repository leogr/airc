# Named channel controls

Verification: `62636d072453eff4f2f3aeff493d73e51137d623` and accompanying changes; re-read cited symbols when behavior changes.

[FACT] Saved state version 2 adds a required `channels` object: followed
display names sorted by IRC fold, current-epoch membership, refusals,
quarantine, topic snapshots, at most one pending control and 32 control
receipts. Validation bounds the union of channel keys to 128, checks folded
keys, operation IDs, epochs, 200-byte topics and 256-byte reasons, and requires
the pending control to match exactly one pending receipt. Version 1 must omit
the object and stays readable; `upgrade` seeds follows from the original
configured channels only on an explicit connect. A new connection clears
membership/topic evidence. [1](#source-1) [2](#source-2)

[FACT] The worker fingerprints the original configuration, then passes only
the followed, unquarantined channels to `connect_named`. Named setup validates
the registration server name and recipient, bounds each JOIN to 10 seconds,
requires an authenticated self JOIN before the matching 366 and records 403/474
refusals without failing the connection. Other startup refusals fail the whole
connection; live controls accept the wider refusal set below. The outer
20-second connection deadline still bounds all setup together. Generic startup
keeps its strict refusal behavior. Recovery requests history only for
channels still joined; a KICK tracked while buffering stops that channel, and
every FAIL, batch opener and replay target must name the requested channel.
[3](#source-3) [4](#source-4) [5](#source-5)

[FACT] IPC version 2 gains strict `join`, `part` and `topic` operations with a
required 32-hex `operation_id`; topic text distinguishes absent, empty and
nonempty, and null is rejected. The CLI generates a random ID per invocation.
`begin` returns a saved outcome for a retained identical ID, refuses a
conflicting binding, decides idempotent JOIN/PART and refusals locally, and
otherwise saves the receipt and intent before the worker writes once. Busy,
unknown-membership and setup-pending refusals are bound receipts too. A lost
reply names the ID for `status`. [1](#source-1) [6](#source-6) [7](#source-7)

[FACT] `resolve` settles only on this connection's own prefix (nick and
account) with the same channel, or a numeric from the registered server to this
nick for that channel: JOIN, refusals 403/405/471/473–477, PART, 442/403, 331/332
for queries, an exact self TOPIC for set/clear, and 403/442/482/417 for topics.
Refusal restores the prior follow preference. Sends, availability and controls
share one pending slot; an unconfirmed control closes the connection after 10
seconds and `interrupt` marks it uncertain and quarantines join/part targets.
[1](#source-1) [6](#source-6)

[FACT] `track` treats a fresh self JOIN as joined with setup pending for 10
seconds. Initial 331/332 update the topic only while no newer authenticated
TOPIC arrived; 366 completes setup; expiry marks setup and topic unknown, and a
late 366 cannot clear it. Topic queries are refused unsent while setup is
pending or unknown. A self KICK removes membership, keeps the preference and
records refusal plus quarantine. [1](#source-1)

[FACT] `JoinHistory` queues at most 128 fresh JOINs (16 KiB of names), sends one
LATEST 20 request at a time with a 20-second deadline including queueing, and
correlates the batch opener, FAIL target and each replay target. More than 20
messages or 2 MiB, a stray target, timeout or uncorrelated reply makes the page
incomplete; the latter two stop fetching for the connection and discard up to
seven late batches. Replay records are `history` context and keep a newer live
checkpoint. Critical notices report gaps; after history quarantine, each later
JOIN reports another gap until an explicit reconnect. [8](#source-8) [2](#source-2)

[FACT] Unit tests cover strict state and receipts, ID/text validation,
idempotence, forged confirmations, refusals, KICK quarantine, both review
schedules at the state level, setup expiry, interruption and journal bounds.
Integration tests run real CLI processes for controls, a dropped final IPC reply
with an identical second command, partial and all-refused startup, KICK,
an unconfirmed PART and a delayed setup with a newer topic. [9](#source-9)
[10](#source-10) [11](#source-11)

## Sources

1. <a id="source-1"></a> [src/agent_session/channels.rs](../../src/agent_session/channels.rs#L19-L1033) — records, `Request::validate`, `Channels::validate`, `interrupt`, `reset`, `started`, `track`, `expire`, `begin`, `settle`, `resolve` and `describe`.
2. <a id="source-2"></a> [src/agent_session/state.rs](../../src/agent_session/state.rs#L30-L176) — version 2 field and validation; `ingest_with` replay classes at L263-L420; `upgrade`, `start_channels`, `track`, `ingest_live` and `channel_tick` at L575-L720.
3. <a id="source-3"></a> [src/agent_session/mod.rs](../../src/agent_session/mod.rs#L457-L519) — `worker` identity check, upgrade, named startup, recovery and save order.
4. <a id="source-4"></a> [src/client.rs](../../src/client.rs#L181-L494) — `JoinRefusal`, `connect_named`, `own_prefix`, `server_numeric` and partial/generic `open_connection`; JSONL topic encoding and version 1 refusal at L1011 and L1065-L1085.
5. <a id="source-5"></a> [src/agent_session/history.rs](../../src/agent_session/history.rs#L340-L512) — `recover` target correlation, KICK tracking and `replay_live`.
6. <a id="source-6"></a> [src/agent_session/mod.rs](../../src/agent_session/mod.rs#L62-L140) — control operations and `operation_id`; `call` lost-reply text at L277-L372, `validate` at L429-L455, `serve` control slot, settlement and timeout at L521-L782.
7. <a id="source-7"></a> [src/bin/airc.rs](../../src/bin/airc.rs#L66-L84) — `join`, `part` and `topic` commands; dispatch with fresh IDs at L175-L224.
8. <a id="source-8"></a> [src/agent_session/history.rs](../../src/agent_session/history.rs#L12-L338) — bounds, `Disposition` and `JoinHistory`.
9. <a id="source-9"></a> [src/agent_session/channels.rs](../../src/agent_session/channels.rs#L1034-L1390) — channel state and control unit tests.
10. <a id="source-10"></a> [src/agent_session/history.rs](../../src/agent_session/history.rs#L560-L667) — `JoinHistory` tests; state tests in `src/agent_session/state.rs` at L930-L987 and IPC strictness in `src/agent_session/mod.rs` at L872-L908.
11. <a id="source-11"></a> [tests/named_sessions.rs](../../tests/named_sessions.rs#L1083-L1568) — channel integration tests; PART-timeout and delayed-setup faults in `fault_proxy` at L556-L640.
