# Local waiter and monitor bookkeeping

Verification: `a9d1d9bbf24858c64e1ce2c398ee7a2ec1e0c5a9` and accompanying changes; re-read cited symbols when behavior changes.

[FACT] The named worker registers at most one readiness waiter, containing its
wake mode, registration UTC time, monotonic deadline and reply channel. Status
reports `waiter: armed wake=MODE since=UTC deadline=UTC` only while that channel
is open and the deadline has not elapsed, otherwise `waiter: none`. A stopped or
unreachable worker reports `waiter: unknown` with its reason, not a saved live
claim. Readiness, expiry, caller EOF and worker teardown release the registration.
No process-name search or agent activity inference is used. [1](#source-1)

[FACT] These times describe one bounded IPC request. The outer `airc wait`
without timeout renews idle requests of at most 60 seconds, leaving brief gaps
between registrations. Status is an observation, not a reservation, guarantee
that the outer command is alive or promise of a future model turn. [1](#source-1)
[2](#source-2)

[FACT] Delivery consumers are tracked separately by the worker and reported on
`delivery consumer:` status lines (see [delivery](delivery.md)); the helper is
not used for them. The optional Python skill helper has only `record`, `handling`, and
`status`. It stores one bounded harness handle with session, executable, skill,
optional state-directory/thread and chosen wake mode. An identical record is
idempotent; conflicting retained handles/configuration are refused. `handling`
requires the exact handle and a caller-reported completed/cancelled/failed
outcome, then clears it. It never launches, signals, inspects or waits for jobs,
executes recorded paths, sends chat, queues turns, reconnects or retries. Its
stdout explicitly identifies its data as bookkeeping, not wake evidence.
[3](#source-3)

[FACT] Helper records live in an existing private directory. Descriptor-relative
no-follow traversal rejects symlinks in ancestors and leaves; file ownership,
mode, regular type and single-link checks precede use. A stable native lock
serializes operations; bounded JSON uses a new 0600 file, fsync, rename and
parent fsync. Unknown schema/fields and malformed records fail without reset.
Tests exercise conflicting handles, observed outcome handling, symlinks,
hardlinks, public files/directories, oversized/corrupt state and lock contention.
[3](#source-3) [4](#source-4)

## Sources

1. <a id="source-1"></a> [src/agent_session/mod.rs](../../src/agent_session/mod.rs#L256-L282) — `WaitingNotice` and `waiter_status`; stopped/unavailable fallback at L542–565; `serve` registration at L1320–1328 and release/teardown at L1422–1442.
2. <a id="source-2"></a> [src/agent_session/mod.rs](../../src/agent_session/mod.rs#L642-L683) — `wait` bounded renewal; `tests/named_sessions.rs:383–440` — observed registration timestamps, cancellation and teardown; stopped-worker recovery assertion at L562.
3. <a id="source-3"></a> [skills/airc/scripts/airc-monitor.py](../../skills/airc/scripts/airc-monitor.py#L1-L157) — helper `locked`, `read`, `save`, `main`.
4. <a id="source-4"></a> [scripts/test_airc_monitor.py](../../scripts/test_airc_monitor.py#L1-L78) — private-file and retained-handle regressions; `Makefile:11–14` includes the suite in `make test`.
