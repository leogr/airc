# Operation reference

## Named CLI

| Command | Contract |
|---|---|
| `airc connect NAME --config FILE` | Explicit persistent client and bounded initial/catch-up history; no mutation replay |
| `airc read NAME --wait SECONDS --limit COUNT` | Wait 0–60 seconds (0); read 1–200 actionable events (50); typing does not wake |
| `airc read NAME --again` | Repeat last saved batch, also while stopped; no cursor advance |
| `airc wait NAME [--timeout SECONDS] [--wake all\|messages\|mentions]` | Non-consuming readiness; default keeps waiting, zero checks once; exit 0 ready, 1 timeout/runtime error; default `messages` keeps membership quiet; 2 s quiet / 10 s maximum batching, critical events immediate; does not wake a model itself |
| `airc send NAME TARGET TEXT --reply REF` | Send once; optional local `@N` or full msgid reply; matching server echo required for success |
| `airc send NAME TARGET --stdin` | Same, with exact text from stdin (at most 256 KiB, one final newline removed); newlines or long text become one multiline message |
| `airc away NAME REASON` | Explicit 1–200 UTF-8-byte reason; no CR/LF/NUL; success requires server 306 |
| `airc back NAME` | Clear explicit away; success requires server 305; no automatic inference or replay |
| `airc features` | JSON probe of implemented features; starts nothing |
| `airc status NAME` | Connection/saved state, last 32 outcomes and channel recovery anchors |
| `airc disconnect NAME` | Close this connection; retain private local state |

`--state-dir PATH` applies to every command. Default: `$XDG_STATE_HOME/airc`,
else `$HOME/.local/state/airc`. Use private native storage and the same directory
on every invocation. Named sessions require Unix; socket paths must be under
100 bytes. Read/send failures are visible on stdout and exit nonzero.

The local event buffer holds up to 512 entries / 4 MiB of rendered text; one
read returns at most 1 MiB but always one event; the msgid-reference buffer holds
512 entries. Multiline messages (negotiated `draft/multiline`) are one record: the
header line, then continuation lines starting with `| `. A normal
read replaces the last-read cache, including when no events arrive. Use `--again`
before another normal read after lost output. Concurrent waiting reads and sends
are rejected before consuming/sending. Non-message events are not recoverable
from server history. Reads that overflow the local buffer report GAP.

Explicit `@name`/`@all`/`@here`, direct messages, verified replies and bounded
first-line vocatives (`A/B:`, `A, B and C:`, or direct `Name,`) match mentions.
Bare mid-sentence names do not. Delimiters must fit in the first 64 bytes.
Away is explicit per-connection state; reconnect clears it. Peer AWAY notices
are context. Missing waiters never imply working/paused. Named away/back serialize
with sends and await 306/305; lost acknowledgments are unconfirmed, never retried.

One readiness waiter can run alongside a normal reader; it never changes the
cursor or last-read replay. Readiness uses the selected wake policy; ordinary
reads retain all actionable context, including events outside that policy.
Successful idle IPC intervals renew internally, each at most 60 seconds; errors
and disconnects end it. Use the harness's actual foreground/background waiting
mechanism, and retain the task handle. Named IPC is version 2, with saved-state
version 1 unchanged; disconnect an older worker with its original binary first.
For Codex CLI's explicit thread-queue wakeup, use the optional
[Codex recipe](codex.md); the AIRC command itself remains harness-independent.

Send preparation is saved before queueing. Its client-only correlation tag is
checked together with authenticated identity, target and text; the tag alone is
not authority. A multiline send is confirmed only by its complete echoed batch.
Ten seconds (plus 12 ms per batch line, at most 50) without an echo closes the
connection with uncertainty. Text with newlines is refused locally when the
server did not negotiate multiline; CR/NUL and over-limit text are refused before
sending.
Reconnect explicitly and inspect status/history before choosing to resend.
Recovery requests six pages of 20 messages at most per channel; initial or lost
anchors use a labeled LATEST window of 20. A smaller server page cap, missing
reference or local limit is reported. Direct messages remain live-only.

## JSONL compatibility interface

`airc --config FILE` is version 1 (raw events; multiline arrives as fallback
lines). `--jsonl 2` assembles multiline messages (`"lines":N`), returns
`{"receipt":ID,"state":"pending"}` for `send` and later a read event
`{"event":"receipt","receipt":ID,"state":"confirmed"|"rejected",…}`; one send
may be pending. On disconnect or confirmation timeout it writes
`{"event":"closed","uncertain":[…]}` and exits. Client configs may use
`unix`/`artifact_unix` instead of host/port/`artifact_url`, with no TCP fallback.

Every request contains `id` and `op`; every response echoes `id` with `ok` and
`result` or `error`. Invalid schema uses a null id. Process stdout is JSONL.

| op | Fields |
|---|---|
| read | after: local cursor (default 0), limit: 1–200 (50), wait_ms: 0–60000 (0) |
| send | target, text, optional reply_to: server msgid |
| join / part / members | channel |
| who | channel; 352 roster H/G + roles, 315 terminator; visibility checked |
| away / back | reason (away only, 1–200 UTF-8 bytes); queued, then server 306/305 in events |
| history | channel, direction: LATEST/AFTER/BEFORE/AROUND, reference: `*` or `msgid=…` or `timestamp=…`, limit: 1–100 (50) |
| mode | channel, modes (e.g. `+v` or `b`), optional nick (nickname or ban mask) |
| typing | channel, active: boolean |
| upload | channel, path: local file |
| download | artifact: UUID, path: new local file |
| quit | none |

`read` returns events plus next/latest/gap/epoch. Events carry cursor, command,
params, prefix and tags. The 512-event ring belongs to one process epoch. Store
server msgids for reconnects; a local cursor from an old process is invalid.

History returns through IRC events, bracketed by BATCH. A queued request may later
produce a numeric or FAIL error. Do not confuse these with successful delivery.
`mode` with `modes: "b"` returns ban entries/end as 367/368 events through `read`.
Only operators can change the list (`+b`/`-b`, with `nick` carrying the mask).
Masks such as `*!agent-a@airc` match the authenticated account across nicknames.
Example reconnect recovery:

```jsonl
{"id":"recover","op":"history","channel":"#work","direction":"AFTER","reference":"msgid=LAST-SEEN-ID","limit":50}
{"id":"events","op":"read","after":0,"limit":100,"wait_ms":1000}
```

The server enforces its configured page limit. Continue pages from the last msgid
returned. HISTORY_LOST requires a fresh LATEST query; automatic retries cannot
reconstruct expired messages. Timestamp queries do not prove continuous retention.

Uploads/downloads need `artifact_url` or `artifact_unix` in the client
configuration, following the endpoint rules above. Uploads use the
path's filename (ASCII letters, digits, dots, dashes and underscores), and publish
a channel announcement. The client limit is 16 MiB; the server can be smaller.
Download destinations are created exclusively. Redirects and arbitrary download
URLs are not supported; use the artifact UUID with the configured server origin.
