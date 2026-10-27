# Named delivery receipts

Verification: `19db126da853896fb81d9eed2cb0295436955b5a` and accompanying changes; source anchors re-located, same symbols, at `f5b539c057a9ab62763dbef4166f0a4a41218a3e` and accompanying changes.

[FACT] `subscribe --receipt-timeout S --receipt-scope UUID` is opt-in: 0, the
default, disables it; enabled values are 1–3600 seconds and at least
`--min-interval`, with one exact lowercase hyphenated UUID as scope, and never
with `--wrap`; the worker also refuses a wrapped `--retry-pending` of a batch
that requested a receipt, whatever the new consumer requests. Client and worker
check these; settings travel in an optional strictly decoded `receipt` field.
[1](#source-1) [2](#source-2) [3](#source-3)

[FACT] The worker keeps the canonical state directory that `connect` passed
it, if printable: absolute, without control or non-printing characters, with
`DIR/SESSION/client.sock` below the 100-byte `Storage::open` bound. Before
anything registers or changes, receipt opt-in is refused without such a
directory, or when the widest header, settings notice and receipt notice would
leave fewer than 256 bytes for records within `--max-bytes`. [2](#source-2)
[4](#source-4) [5](#source-5)

[FACT] A batch made for such a consumer saves its request (store, session,
scope, timeout). First delivery, retry and inspection all render, after the
header and any settings notice, `! receipt requested: scope=UUID timeout=Ss;
check the scope, then report receipt once before other work: airc --state-dir
DIR deliveries SESSION --mark-received ID --receipt-scope UUID`. DIR is the
saved store as one POSIX shell word: bare for ASCII letters, digits and
`/._-`, else single-quoted with `'` as `'\''`. Its bytes are reserved with the
settings notice. Saving a request sets state version 4 for good; state that
never saved one stays version 3. [4](#source-4) [5](#source-5)

[FACT] `deliveries --mark-received ID --receipt-scope UUID` (IPC
`mark_received`) works from any same-user connection except the consumer's
own, which only delivers and accepts, also while a consumer runs. It matches
the exact pending batch first, then retained records. Another scope, an
unknown or evicted ID, or a batch without a request is refused unchanged.
The first report time is kept; repeats change nothing. A report on a pending
batch never accepts it or moves the cursor. The worker saves before replying;
a failed save stops it without a reply. [5](#source-5) [6](#source-6)

[FACT] Accepting a requested batch, by sink or `--mark-accepted`, appends a
metadata record: ID, scope, UTC acceptance, timeout, UTC deadline (acceptance
plus timeout), first report and release. It starts a hold unless the batch was
already reported, which releases it as `received` at acceptance. Dropping or
reverting discards the request and its report. At most 32 records are kept,
the oldest released one is evicted, and only the newest may be unreleased.
Validation refuses duplicates, inconsistent deadlines, bad stores, scopes or
bounds, a `received` release without a report and a pending ID among them.
[4](#source-4) [5](#source-5)

[FACT] The hold ends on its exact report (`received`) or when its monotonic
deadline passes (`expired`, checked every 50 ms worker tick, with or without a
consumer). When a batch is made while it holds, a pending critical or lost
relevant event releases it as `urgent`, else the attention high-water mark
above the delivery floor releases it as `pressure`. Urgent events also bypass
`--min-interval`; pressure does not. Irrelevant-only traffic makes no batch
and ends nothing. A late report for an older record leaves a newer hold.
[5](#source-5) [6](#source-6) [7](#source-7)

[FACT] The hold is session state, honored by every consumer. A new worker
epoch restores it for the time left until the saved UTC deadline, clamped to
zero and the original timeout; that bounds each epoch only, so elapsed time
plus a wall-clock rollback, or repeated restarts, can exceed the timeout.
[5](#source-5)

[FACT] Status adds `receipt-scope=… received=…` to a pending batch and a
`delivery receipt:` line for the newest record; `deliveries` lists every
record, newest first, and the consumer line appends its receipt settings.
`airc features` lists `named-delivery-receipts`. [3](#source-3)
[5](#source-5) [8](#source-8)

[FACT] Tests cover rendering at the longest store that fits the smallest
bound, quoting through `sh`, transitions, ledger bounds, malformed state,
upgrade, pressure, opt-in and wrapped-retry refusals that change nothing,
holds, expiry, bypasses, early reports before a failed sink, failed saves,
connection and IPC strictness, and the printed command run verbatim from
another HOME, XDG_STATE_HOME and directory for default, relative and hostile
stores. A counting harness drives `airc-codex` with a fake `codex queue` and a
simulated FIFO receiver. These are mock checks, not live Codex or model runs.
[9](#source-9) [10](#source-10) [11](#source-11)

## Sources

1. <a id="source-1"></a> [src/bin/airc.rs](../../src/bin/airc.rs#L64-L101) — `Subscribe` flags; receipt pairing in dispatch at L337–374, `Deliveries` at L119–139 and L394–412.
2. <a id="source-2"></a> [src/agent_session/mod.rs](../../src/agent_session/mod.rs#L1070-L1183) — `validate`, including the `--wrap` refusal at L1142–1146; `ConsumerSettings` and `ReceiptSettings` at L191–218; `deliver` at L762–803; worker store at L1211; `serve` opt-in and wrapped-retry refusals at L1372–1403.
3. <a id="source-3"></a> [src/agent_session/mod.rs](../../src/agent_session/mod.rs#L333-L354) — `consumer_status`; `deliveries` at L1185–1199.
4. <a id="source-4"></a> [src/agent_session/delivery.rs](../../src/agent_session/delivery.rs#L25-L327) — limits, `Delivery`, `ReceiptRequest::notice`, `Credit`, `Reason`, `validate_scope`, `valid_store`, `shell_word`; `render`/`validate` at L352–383, `ReceiptOptions` at L419–433; `SOCKET_PATH_BYTES` in `storage.rs` at L14.
5. <a id="source-5"></a> [src/agent_session/state.rs](../../src/agent_session/state.rs#L721-L1138) — `set_receipt_store`, `receipt_refusal`, `reserved`, `prepare_delivery`, `finish_delivery`, `mark_received`, hold methods, `delivery_pressure`, status lines; versions at L38–43, `validate` at L195–241, `begin_epoch`/`restore_hold` at L257–278, `receipt_request` at L1463–1475.
6. <a id="source-6"></a> [src/agent_session/mod.rs](../../src/agent_session/mod.rs#L1485-L1494) — `serve` saves a report before replying; tick expiry and hold gating at L1689–1732; consumer-connection restriction in `handle` at L1841–1848.
7. <a id="source-7"></a> [src/agent_session/attention.rs](../../src/agent_session/attention.rs#L289-L299) — shared `high_water`, used by `readiness` at L379.
8. <a id="source-8"></a> [src/lib.rs](../../src/lib.rs#L14-L41) — `FEATURES`.
9. <a id="source-9"></a> [src/agent_session/state.rs](../../src/agent_session/state.rs#L2568-L2949) — receipt unit tests; `delivery.rs` tests at L780 and L858.
10. <a id="source-10"></a> [tests/named_sessions.rs](../../tests/named_sessions.rs#L2477-L3100) — helpers that run the printed command, hold, expiry, bypass, early-report, failed-save and IPC regressions; later ones at L3486–3993.
11. <a id="source-11"></a> [tests/named_sessions.rs](../../tests/named_sessions.rs#L3102-L3484) — `replay` counting harness with drain mode, `receipt_credit_reduces_records_queued_for_a_busy_receiver` and the ignored `receipt_measurement_matrix`.
