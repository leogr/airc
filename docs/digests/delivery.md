# Named delivery and context lookup

Verification: `a9d1d9bbf24858c64e1ce2c398ee7a2ec1e0c5a9` and accompanying changes; re-read cited symbols when behavior changes.

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
adapter argv directly (no shell) in its own process group with
`kill_on_drop`, batch on stdin, `AIRC_SESSION`, `AIRC_DELIVERY_ID`,
`AIRC_DELIVERY_RETRY` and `AIRC_SUBSCRIBER_PID` in the environment, inherited
stdout and the last 4 KiB of stderr captured. Exit 0 within `--sink-timeout`
accepts; any other outcome leaves the batch pending and ends `subscribe` with
exit 1. Only the direct child is killed; its descendants are not.
[4](#source-4) [7](#source-7)

[FACT] `show` resolves `@N`, `msgid:ID` or a raw msgid, walks up to eight reply
ancestors through retained records, and otherwise queues one correlated
`CHATHISTORY AROUND <channel> msgid=<id> 1` per missing channel ancestor; a
private-message ancestor is reported, never fetched. Each lookup result carries
its msgid and only the show that requested it claims it, so a stale result is
discarded. Non-multiline fallback lines are assembled within the join byte
bound. Lookups never enter the transcript or checkpoints, are refused when
history is quarantined, and report unretained messages as errors. One show
fetches at a time.
[3](#source-3) [5](#source-5) [8](#source-8)

[FACT] `@all`/`@here` match only as a standalone word preceded by start,
whitespace or `(` and followed by end, whitespace or `,.:;!?)`, outside
backtick spans. [9](#source-9)

[FACT] `airc-codex --thread UUID` reads at most 64 KiB including the header,
checks the header ID against `AIRC_DELIVERY_ID` and its parent against
`AIRC_SUBSCRIBER_PID`, then runs `codex queue --thread ID --message BATCH` with
literal argv. It accepts only a clean exit whose bounded stdout matches
`Queued message … for thread <ID>.`. It exits 3 when nothing was sent and 4
when the outcome is uncertain; the queue call is killed after 30 seconds, and
output is bounded and escaped. [10](#source-10) [11](#source-11)

[FACT] Tests cover cursor separation, mentions context counts, complete-record
splitting, truncation, durable unique pending IDs, eviction gaps,
version upgrade, manual reads during delivery, failed sinks, a pending batch
kept across explicit disconnect/connect, ID-bound retry/accept/drop, consumer exclusivity, adapter
environment and show history gaps. Adapter tests use a fake `codex` and do not
themselves establish live turns; the operations guide records the separate live
codex-cli 0.163.0 check and its limits. [6](#source-6) [7](#source-7) [8](#source-8) [11](#source-11)

## Sources

1. <a id="source-1"></a> [src/agent_session/state.rs](../../src/agent_session/state.rs#L591-L796) — `begin_delivery`, `delivery_readiness`, `prepare_delivery`, `finish_delivery`, `revert_delivery`, `describe_delivery`; `VERSION` and `upgrade` at L33 and L879–893.
2. <a id="source-2"></a> [src/agent_session/delivery.rs](../../src/agent_session/delivery.rs#L1-L254) — `Delivery`, `Lost`, `Batch`, `Header::open`/`bound`, `prefix` and unit tests.
3. <a id="source-3"></a> [src/agent_session/mod.rs](../../src/agent_session/mod.rs#L1217-L1342) — `serve` handling of `Consume`, `Deliver`, `Accept`, `DropPending`, `Deliveries` and `Show`; `revert_delivery` call at L1524.
4. <a id="source-4"></a> [src/agent_session/mod.rs](../../src/agent_session/mod.rs#L283-L320) — `Consumer`, `consumer_status`, `pending_refusal`, `recovery`; `deliver` and `run_sink` at L713–968.
5. <a id="source-5"></a> [src/agent_session/history.rs](../../src/agent_session/history.rs#L127-L151) — `lookup`, `take_lookup`; AROUND request at L210; correlation test at L792.
6. <a id="source-6"></a> [src/agent_session/state.rs](../../src/agent_session/state.rs#L1419-L1704) — delivery unit tests from `delivering` to `evicting_a_pending_batchs_own_events_is_not_a_gap`.
7. <a id="source-7"></a> [tests/named_sessions.rs](../../tests/named_sessions.rs#L1621-L1853) — `wait_deliver_prints_only_mentions_and_manual_reads_never_steal`, `failed_sinks_leave_one_pending_batch_until_explicit_id_bound_recovery`; `subscribe_feeds_each_batch_to_its_adapter_and_owns_the_single_consumer_slot` at L1918.
8. <a id="source-8"></a> [tests/named_sessions.rs](../../tests/named_sessions.rs#L1854-L1917) — `show_prints_reply_ancestors_without_consuming_and_reports_history_gaps`; `ShowWalk`, DM short-circuit and `claim_lookup` in `src/agent_session/mod.rs` at L326–445; r2 regressions in its tests from L1690.
9. <a id="source-9"></a> [src/agent_session/attention.rs](../../src/agent_session/attention.rs#L153-L175) — `broadcast`; test `only_explicit_bounded_all_and_here_broadcast` at L588.
10. <a id="source-10"></a> [src/bin/airc-codex.rs](../../src/bin/airc-codex.rs#L1-L297) — constants, `main`, `classify`, `run_bounded`, `read_batch`, `check_delivery_id`, `check_subscriber`, `queued_receipt`.
11. <a id="source-11"></a> [tests/codex_adapter.rs](../../tests/codex_adapter.rs#L1-L183) — fake-`codex` argv, refusal, lost-subscriber, uncertain, noisy-stderr and lingering-descendant tests; unit tests in `src/bin/airc-codex.rs` from L298.
