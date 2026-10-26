# Named delivery and context lookup

Verification: `7da41509514601ec60d4b7a97ec3dedf5ad090db` and accompanying changes.

[FACT] Named state version 3 has an optional delivery record: a separate
cursor, loss summary, one pending batch at most and the last outcome. Versions
1 and 2 upgrade only on explicit connect. Reads and plain `wait` never move
the delivery cursor. A first consumer starts at the read cursor.
[1](#source-1) [2](#source-2) [6](#source-6)

[FACT] Readiness reuses the attention engine above the delivery cursor.
Preparation packs complete relevant records within `--max-bytes` (default
8192, 1024–1 MiB) minus the header bound and settings notice, counts skipped
unaddressed messages as `context`, and otherwise advances only an in-memory
scan floor. A single oversized record
becomes a UTF-8-safe prefix and truncation notice naming `airc show`. `gap`
reports evicted relevant events above the pending or accepted batch, once.
[1](#source-1) [2](#source-2) [6](#source-6)

[FACT] The compact header is `[airc id=UUID session=NAME…]` plus `events`
unless 1 (gap-only: `events=0`), `context` unless 0, and bare `more`, `gap`,
`retry` when set. Each new batch saves `compact` metadata: escaped account and
nick, wake, max-bytes, min-interval, wrap, and whether its first rendering
shows the `! delivery settings:` notice. The first batch after registration
(an in-memory flag that a revert restores) and gap batches show it; retries
and `--show` always do, from the saved settings. Batches without `compact`
(full fields, either marker) render verbatim with `retry=yes|no`.
Continuations start with `| `, so a body cannot forge a header.
[1](#source-1) [2](#source-2)

[FACT] A batch is saved as pending before the reply. `Accept` or `DropPending`
moves only the delivery cursor and records the outcome; a caller that leaves
before any reply reverts the batch. While a batch is pending, a new consumer is
refused with its ID and recovery commands; only an explicit `--retry-pending ID`
redelivers it, flagged `retry`. [1](#source-1) [3](#source-3) [7](#source-7)

[FACT] One consumer per worker is bound to its registering IPC connection
until it closes, without timer expiry. A second consumer, and operator
accept/drop, are refused meanwhile. Registration declares wake, max-bytes, min-interval and wrap; an
older client's first deliver request fixes them with wrap `unknown`, and a
request with other settings is refused. `status` prints `delivery:` and
`delivery consumer:` lines with cursor, pending ID/range, last outcome, kind,
pid, request times and settings (`settings=unknown` before they are known).
[3](#source-3) [4](#source-4) [7](#source-7)

[FACT] `wait --deliver` prints, flushes, then accepts. `subscribe` spawns its
adapter argv without a shell (own process group, `kill_on_drop`): batch on
stdin, `AIRC_SESSION`, `AIRC_DELIVERY_ID`, `AIRC_DELIVERY_RETRY` and
`AIRC_SUBSCRIBER_PID` set, stdout inherited, last 4 KiB of stderr kept. Exit 0 within `--sink-timeout`
accepts; any other outcome leaves the batch pending and ends `subscribe` with
exit 1. Only the direct child is killed; its descendants are not.
[4](#source-4) [7](#source-7)

[FACT] `show` resolves an `m:` reference, `msgid:ID` or a raw msgid (see
[message references](message-references.md)) and walks up to eight
reply ancestors through retained records, else one correlated `CHATHISTORY
AROUND <channel> msgid=<id> 1` per missing channel ancestor, claimed only by
its requesting show; private ancestors are reported, never fetched. Lookups
never enter the transcript or checkpoints, are refused while history is
quarantined, report unretained messages and run one at a time.
[3](#source-3) [5](#source-5) [8](#source-8)

[FACT] `@all`/`@here` match only as standalone words outside backtick spans.
[9](#source-9)

[FACT] `airc-codex --thread UUID` reads at most 64 KiB including the header,
accepts both markers, checks the header ID against `AIRC_DELIVERY_ID` and its parent against
`AIRC_SUBSCRIBER_PID`, then runs `codex queue --thread ID --message BATCH` with
literal argv. It accepts only a clean exit whose bounded stdout matches
`Queued message … for thread <ID>.`, exits 3 when nothing was sent and 4 when
uncertain, kills the queue call after 30 seconds and escapes output. [10](#source-10) [11](#source-11)

[FACT] `subscribe --wrap N` (64–65536) splits each post-header line longer
than N bytes at char boundaries into `| ` continuations before the sink.
[2](#source-2) [12](#source-12)

[FACT] The `airc-delivery` Claude Code monitor launcher is silent without
`AIRC_DELIVERY_SESSION`. It polls `status` for `connected;` (1–30 s
backoff), pauses 2 s once if a consumer is still registered, runs
`subscribe --wake ${AIRC_DELIVERY_WAKE:-mentions} --min-interval 15
--max-bytes 2560 --wrap 480 -- -` (stderr merged), forwards HUP/INT/TERM and
prints one `AIRC delivery stopped: …` line after it exits. [13](#source-13) [14](#source-14)

[FACT] Tests cover cursors, context counts, compact omissions, settings
notices and budgets, legacy restore, splitting/truncation, gaps, upgrade,
failed sinks, ID-bound recovery, consumer settings and exclusivity, adapter
environment, show gaps and wrapped stdout. Adapter and launcher tests use
fakes; the operations guide records live checks. [6](#source-6) [7](#source-7) [8](#source-8) [11](#source-11) [14](#source-14) [15](#source-15)

## Sources

1. <a id="source-1"></a> [src/agent_session/state.rs](../../src/agent_session/state.rs#L652-L862) — delivery methods; `VERSION`, `notice_due`, `upgrade` at L39, L77, L947–959.
2. <a id="source-2"></a> [src/agent_session/delivery.rs](../../src/agent_session/delivery.rs#L1-L573) — batch, `Compact`, `Settings`, `Rendering`, header, `wrap`, tests.
3. <a id="source-3"></a> [src/agent_session/mod.rs](../../src/agent_session/mod.rs#L1288-L1396) — `serve` delivery and show operations; `revert_delivery` call at L1602.
4. <a id="source-4"></a> [src/agent_session/mod.rs](../../src/agent_session/mod.rs#L301-L342) — consumer status and refusals; `ConsumerSettings` at L187–198; `deliver` and `run_sink` at L734–1006.
5. <a id="source-5"></a> [src/agent_session/history.rs](../../src/agent_session/history.rs#L127-L151) — lookups; AROUND request at L210; test at L792.
6. <a id="source-6"></a> [src/agent_session/state.rs](../../src/agent_session/state.rs#L1763-L2193) — delivery unit tests.
7. <a id="source-7"></a> [tests/named_sessions.rs](../../tests/named_sessions.rs#L1658-L1906) — delivery regressions; `subscribe` and consumer-settings tests at L2064 and L2197.
8. <a id="source-8"></a> [tests/named_sessions.rs](../../tests/named_sessions.rs#L1983-L2061) — show regression; `ShowWalk` and `claim_lookup` in `src/agent_session/mod.rs` at L349–456.
9. <a id="source-9"></a> [src/agent_session/attention.rs](../../src/agent_session/attention.rs#L153-L175) — `broadcast`; test `only_explicit_bounded_all_and_here_broadcast` at L588.
10. <a id="source-10"></a> [src/bin/airc-codex.rs](../../src/bin/airc-codex.rs#L1-L294) — adapter bounds, checks, queue call, receipts.
11. <a id="source-11"></a> [tests/codex_adapter.rs](../../tests/codex_adapter.rs#L1-L218) — fake-`codex` tests; unit tests in `src/bin/airc-codex.rs` from L295.
12. <a id="source-12"></a> [src/agent_session/mod.rs](../../src/agent_session/mod.rs#L704-L716) — `DeliverOptions::wrap`; checked at L751–758, applied at L855–858.
13. <a id="source-13"></a> [integrations/claude-plugin/scripts/airc-delivery](../../integrations/claude-plugin/scripts/airc-delivery#L1-L41) — launcher, run by `monitors/monitors.json`.
14. <a id="source-14"></a> [tests/claude_plugin.rs](../../tests/claude_plugin.rs#L1-L238) — launcher tests with a fake `airc`.
15. <a id="source-15"></a> [tests/named_sessions.rs](../../tests/named_sessions.rs#L2261-L2340) — `subscribe_wrap_splits_long_stdout_lines_into_continuations`.
