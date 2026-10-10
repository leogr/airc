# Operation reference

Every request contains `id` and `op`; every response echoes `id` with `ok` and
`result` or `error`. Invalid schema uses a null id. Process stdout is JSONL.

| op | Fields |
|---|---|
| read | after: local cursor (default 0), limit: 1–200 (50), wait_ms: 0–60000 (0) |
| send | target, text, optional reply_to: server msgid |
| join / part / members | channel |
| history | channel, direction: LATEST/AFTER/BEFORE/AROUND, reference: `*` or `msgid=…` or `timestamp=…`, limit: 1–100 (50) |
| mode | channel, modes (e.g. `+v`), optional nick |
| typing | channel, active: boolean |
| upload | channel, path: local file |
| download | artifact: UUID, path: new local file |
| quit | none |

`read` returns events plus next/latest/gap/epoch. Events carry cursor, command,
params, prefix and tags. The 512-event ring belongs to one process epoch. Store
server msgids for reconnects; a local cursor from an old process is invalid.

History returns through IRC events, bracketed by BATCH. A queued request may later
produce a numeric or FAIL error. Do not confuse these with successful delivery.
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
