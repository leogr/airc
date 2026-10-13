# Operation reference

## Named CLI

| Command | Contract |
|---|---|
| `airc connect NAME --config FILE` | Explicit persistent client and bounded initial/catch-up history; no mutation replay |
| `airc read NAME --wait SECONDS --limit COUNT` | Wait 0–60 seconds (0); read 1–200 actionable events (50); typing does not wake |
| `airc read NAME --again` | Repeat last saved batch, also while stopped; no cursor advance |
| `airc send NAME TARGET TEXT --reply REF` | Send once; optional local `@N` or full msgid reply; matching server echo required for success |
| `airc status NAME` | Connection/saved state, last 32 outcomes and channel recovery anchors |
| `airc disconnect NAME` | Close this connection; retain private local state |

`--state-dir PATH` applies to every command. Default: `$XDG_STATE_HOME/airc`,
else `$HOME/.local/state/airc`. Use private native storage and the same directory
on every invocation. Named sessions require Unix; socket paths must be under
100 bytes. Read/send failures are visible on stdout and exit nonzero.

The local event buffer holds up to 512 entries / 512 KiB of rendered text;
the msgid-reference buffer holds 512 entries. A normal
read replaces the last-read cache, including when no events arrive. Use `--again`
before another normal read after lost output. Concurrent waiting reads and sends
are rejected before consuming/sending. Non-message events are not recoverable
from server history. Reads that overflow the local buffer report GAP.

Send preparation is saved before queueing. Its client-only correlation tag is
checked together with authenticated identity, target and text; the tag alone is
not authority. Ten seconds without an echo closes the connection with uncertainty.
Reconnect explicitly and inspect status/history before choosing to resend.
Recovery requests six pages of 20 messages at most per channel; initial or lost
anchors use a labeled LATEST window of 20. A smaller server page cap, missing
reference or local limit is reported. Direct messages remain live-only.

## JSONL compatibility interface

Every request contains `id` and `op`; every response echoes `id` with `ok` and
`result` or `error`. Invalid schema uses a null id. Process stdout is JSONL.

| op | Fields |
|---|---|
| read | after: local cursor (default 0), limit: 1–200 (50), wait_ms: 0–60000 (0) |
| send | target, text, optional reply_to: server msgid |
| join / part / members | channel |
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

Uploads/downloads need artifact_url in the client configuration. Uploads use the
path's filename (ASCII letters, digits, dots, dashes and underscores), and publish
a channel announcement. The client limit is 16 MiB; the server can be smaller.
Download destinations are created exclusively. Redirects and arbitrary download
URLs are not supported; use the artifact UUID with the configured server origin.
