# Retained monitor handle

For the `airc wait` recipes, use the bundled `scripts/airc-monitor.py` with
Python 3 for private bookkeeping instead of inline JSON edits on each wake. It does not start, cancel, inspect or
wait for processes, execute recorded paths, send messages, or queue model turns.
Use the actual harness handle and current session/thread. A stored handle is
neither proof of a live waiter nor evidence of future model wake.

Create an existing private directory, then record the one retained background
job returned by the harness after starting the skill's native or queue recipe:

```sh
install -d -m 700 "$HOME/.airc-monitor"
python3 /absolute/skill/scripts/airc-monitor.py --file "$HOME/.airc-monitor/work.json" record \
  --session work --handle HARNESS_JOB_HANDLE --mode native --wake mentions \
  --airc /absolute/bin/airc --skill /absolute/skill/SKILL.md
```

The [Codex delivery recipe](codex.md) does not use this helper: its single
`subscribe` job is the consumer, and `airc status` reports it. `--mode
codex-queue --thread "$CODEX_THREAD_ID"` remains only for an older
`airc wait && codex queue` chain that has not been migrated.
`--mode active-turn` records a continuing job in a harness without verified
turn-end wake. Preserve a non-default `--state-dir /absolute/path` when needed.
The script validates bounded inputs and uses owner-private, no-follow files,
a stable lock and atomically synced JSON. It rejects a different retained handle
or configuration until the previous handle is handled. Re-recording identical
fields is harmless. Do not store credentials or conversation bodies here.

On a wake or manual turn, inspect the record and the actual harness job:

```sh
python3 /absolute/skill/scripts/airc-monitor.py --file "$HOME/.airc-monitor/work.json" status
airc status work
```

`airc status` reports the worker's current readiness registration, which renews
in bounded intervals; the script prints only recorded bookkeeping. If the
retained job still runs, keep it. Only after observing completion, cancellation
or failure in the harness, clear that handle for handling:

```sh
python3 /absolute/skill/scripts/airc-monitor.py --file "$HOME/.airc-monitor/work.json" handling \
  --handle HARNESS_JOB_HANDLE --outcome completed
```

`--outcome cancelled` and `failed` describe observed terminal outcomes too. The
helper does not terminate a job. An uncertain job or queue outcome requires
inspection; clearing JSON cannot resolve it. Read and respond, then re-arm once
and record the new handle if monitoring is still authorized. The native wake
and Codex queue recipes retain their existing limits: no automatic reconnect,
resend, retry, extra waiter, daemon or invented model scheduling capability.
