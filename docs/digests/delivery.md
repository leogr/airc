# Named delivery and context lookup

Verification: `b1e88a82d83554b94ff53a04cb2fa8563bf6e37b` and accompanying changes; re-read cited symbols when behavior changes.

[FACT] Saved named state version 3 adds one optional delivery record: its own
cursor, a lost-relevance summary, at most one pending batch and the last
outcome. Versions 1 and 2 upgrade only on explicit connect. Reads, `--again`
and plain `wait` use the read cursor; delivery moves only its own. A first
consumer starts at the read cursor, so history already read is not delivered.
[1](#source-1) [2](#source-2) [6](#source-6)

[FACT] Delivery readiness reuses the attention engine above the delivery
cursor. Batch preparation includes complete relevant records within
`--max-bytes` (default 8192, 1024–1 MiB, header included), counts skipped
unaddressed messages as `context`, and advances only an in-memory scan floor
when nothing relevant exists. A single oversized record becomes a UTF-8-safe
prefix plus an explicit truncation notice naming `airc show`. `gap=yes` reports
evicted relevant events above the pending or accepted batch, once.
[1](#source-1) [2](#source-2) [6](#source-6)

[FACT] The client writes one header line with a random UUID, session,
escaped account and nick, wake mode, counts, record bytes and `more`/`gap`/
`retry` flags. A record body cannot start a line with `[` because continuation
lines start with `| `. Header tests bound every field value. [2](#source-2)

[FACT] A batch is saved as pending before the reply. `Accept` or `DropPending`
moves only the delivery cursor and records the outcome; a caller that leaves
before any reply reverts the batch. While a batch is pending, a new consumer is
refused with its ID and recovery commands; only an explicit `--retry-pending ID`
redelivers it, with `retry=yes`. [1](#source-1) [3](#source-3) [7](#source-7)

[FACT] One consumer is registered per worker, bound to the IPC connection that
registered it; connection close releases it and no timer expires it. A second
consumer is refused, and operator accept/drop are refused while any consumer is
registered. `status` prints `delivery:` and `delivery consumer:` lines with
cursor, pending ID/range, last outcome, kind, pid and request times.
[3](#source-3) [4](#source-4) [7](#source-7)

[FACT] `wait --deliver` prints, flushes, then accepts. `subscribe` spawns its
adapter argv directly (no shell, own process group, `kill_on_drop`), batch on stdin, `AIRC_SESSION`, `AIRC_DELIVERY_ID`,
`AIRC_DELIVERY_RETRY` and `AIRC_SUBSCRIBER_PID` in the environment, inherited
stdout and the last 4 KiB of stderr captured. Exit 0 within `--sink-timeout`
accepts; any other outcome leaves the batch pending and ends `subscribe` with
exit 1. Only the direct child is killed; its descendants are not.
[4](#source-4) [7](#source-7)

[FACT] `show` resolves `@N`, `msgid:ID` or a raw msgid and walks up to eight
reply ancestors through retained records, else queues one correlated
`CHATHISTORY AROUND <channel> msgid=<id> 1` per missing channel ancestor;
private-message ancestors are reported, never fetched. Only the requesting show
claims a result, by msgid. Fallback lines assemble within the join bound.
Lookups never enter the transcript or checkpoints, are refused while history is
quarantined, report unretained messages as errors and run one at a time.
[3](#source-3) [5](#source-5) [8](#source-8)

[FACT] `@all`/`@here` match only as a standalone word preceded by start,
whitespace or `(` and followed by end, whitespace or `,.:;!?)`, outside
backtick spans. [9](#source-9)

[FACT] `airc-codex --thread UUID` reads at most 64 KiB including the header,
checks the header ID against `AIRC_DELIVERY_ID` and its parent against
`AIRC_SUBSCRIBER_PID`, then runs `codex queue --thread ID --message BATCH` with
literal argv. It accepts only a clean exit whose bounded stdout matches
`Queued message … for thread <ID>.`, exits 3 when nothing was sent and 4 when
uncertain, kills the queue call after 30 seconds and escapes bounded output. [10](#source-10) [11](#source-11)

[FACT] `subscribe --wrap N` (64–65536) splits each line after the header
longer than N bytes at char boundaries before the sink sees it; continuations
start with `| `, so no split begins a record- or header-like line. `bytes=`
counts unwrapped records. [2](#source-2) [12](#source-12)

[FACT] The `airc-delivery` Claude Code plugin's monitor launcher is silent
without `AIRC_DELIVERY_SESSION`. It polls `status` for `connected;` (1–30 s
backoff), pauses 2 s once if a consumer is still registered, runs
`subscribe --wake ${AIRC_DELIVERY_WAKE:-mentions} --min-interval 15
--max-bytes 2560 --wrap 480 -- -` (stderr merged), forwards HUP/INT/TERM and
prints one `AIRC delivery stopped: …` line after it exits. [13](#source-13) [14](#source-14)

[FACT] Tests cover cursors, context counts, record splitting and truncation,
pending IDs, eviction gaps, upgrade, reads during delivery, failed sinks,
pending batches across reconnect, ID-bound recovery, consumer exclusivity,
adapter environment, show gaps and wrapped stdout. Adapter and launcher tests
use fakes, not live turns; the operations guide records live checks. [6](#source-6) [7](#source-7) [8](#source-8) [11](#source-11) [14](#source-14) [15](#source-15)

## Sources

1. <a id="source-1"></a> [src/agent_session/state.rs](../../src/agent_session/state.rs#L591-L796) — delivery state methods; `VERSION` and `upgrade` at L33 and L879–893.
2. <a id="source-2"></a> [src/agent_session/delivery.rs](../../src/agent_session/delivery.rs#L1-L320) — batch types, header, `prefix`, `wrap` and unit tests.
3. <a id="source-3"></a> [src/agent_session/mod.rs](../../src/agent_session/mod.rs#L1232-L1357) — `serve` delivery and show operations; `revert_delivery` call at L1539.
4. <a id="source-4"></a> [src/agent_session/mod.rs](../../src/agent_session/mod.rs#L283-L320) — consumer status and refusals; `deliver` and `run_sink` at L716–983.
5. <a id="source-5"></a> [src/agent_session/history.rs](../../src/agent_session/history.rs#L127-L151) — lookups; AROUND request at L210; test at L792.
6. <a id="source-6"></a> [src/agent_session/state.rs](../../src/agent_session/state.rs#L1419-L1704) — delivery unit tests.
7. <a id="source-7"></a> [tests/named_sessions.rs](../../tests/named_sessions.rs#L1621-L1853) — delivery regressions; `subscribe` test at L1918.
8. <a id="source-8"></a> [tests/named_sessions.rs](../../tests/named_sessions.rs#L1854-L1917) — show regression; `ShowWalk` and `claim_lookup` in `src/agent_session/mod.rs` at L326–445.
9. <a id="source-9"></a> [src/agent_session/attention.rs](../../src/agent_session/attention.rs#L153-L175) — `broadcast`; test `only_explicit_bounded_all_and_here_broadcast` at L588.
10. <a id="source-10"></a> [src/bin/airc-codex.rs](../../src/bin/airc-codex.rs#L1-L297) — adapter bounds, checks, queue call and receipt classification.
11. <a id="source-11"></a> [tests/codex_adapter.rs](../../tests/codex_adapter.rs#L1-L183) — fake-`codex` adapter tests; unit tests in `src/bin/airc-codex.rs` from L298.
12. <a id="source-12"></a> [src/agent_session/mod.rs](../../src/agent_session/mod.rs#L686-L698) — `DeliverOptions::wrap`; checked at L733–740, applied at L831–834.
13. <a id="source-13"></a> [integrations/claude-plugin/scripts/airc-delivery](../../integrations/claude-plugin/scripts/airc-delivery#L1-L41) — launcher, run by `monitors/monitors.json`.
14. <a id="source-14"></a> [tests/claude_plugin.rs](../../tests/claude_plugin.rs#L1-L236) — launcher tests with a fake `airc`.
15. <a id="source-15"></a> [tests/named_sessions.rs](../../tests/named_sessions.rs#L2030-L2096) — `subscribe_wrap_splits_long_stdout_lines_into_continuations`.
