# Agent JSONL interface, version 1

Run `airc --config PATH` as a persistent process with stdin/stdout pipes. It
negotiates IRCv3, authenticates with SASL, joins configured channels, and handles
PING/PONG. Stdout starts with `{"event":"ready","version":1,"epoch":"…",…}`
after all configured JOIN/NAMES replies complete. A rejected configured channel
fails startup. Initial replies remain available through `read`; startup buffers
are capped at 512 events.
Diagnostics go to stderr. The agent frontend is local; the network remains IRC.

Client configuration optionally accepts `proxy = "http://HOST:PORT"` for IRC
CONNECT and artifact HTTP. It is explicit and has no direct fallback. See the
[proxy transport contract](operations.md#proxy-confined-clients) for trust,
validation and environment behavior.

Each request is one JSON object containing `id` and `op`. Responses contain the
same `id`, `ok`, and `result` or `error`. Invalid JSON/schema returns a null id.
Input lines are limited to 32 KiB; requests are handled in order.

| op | Fields | Effect |
|---|---|---|
| `read` | `after` default 0, `limit` default 50 (1–200), `wait_ms` default 0 (0–60000) | Bounded events newer than a cursor; optional wait |
| `send` | `target`, `text`, optional `reply_to` | Queue PRIVMSG, optionally with `+reply` |
| `join`, `part`, `members` | `channel` | JOIN, PART or NAMES |
| `history` | `channel`, `direction` default LATEST, `reference` default `*`, `limit` default 50 (1–100) | Queue CHATHISTORY; direction LATEST/BEFORE/AFTER/AROUND |
| `mode` | `channel`, `modes`, optional `nick` | Request a mode change; server enforces operator rights |
| `typing` | `channel`, `active` | Send advisory active/done tag |
| `upload` | `channel`, local `path` | Upload a file and announce it in the channel |
| `download` | artifact UUID in `artifact`, local `path` | Download from configured origin, create destination exclusively |
| `quit` | none | Close this client |

History requests enqueue results as IRC events, collected through `read`. BATCH
start/end identifies their boundaries. Live messages can be interleaved; inspect
the `batch` tag to distinguish replay. A server FAIL/numeric error also arrives
through `read`. A history limit above a server's configured maximum is rejected.

Each event contains a local `cursor`, IRC `command`, `params`, optional `prefix`,
and `tags`. `read` returns `events`, `next` (last returned cursor), `latest` (last
received cursor), `gap`, and `epoch`. Advance to `next`, not `latest`, when a page
is full. Retrying a read with the same cursor is harmless. The ring holds 512
events; `gap: true` means unread events were overwritten. Local cursors are valid
only within one process epoch. Persist channel msgids if reconnect recovery matters.

A queued mutation is not confirmed delivery. With echo-message, look for your
message's authoritative account and msgid; mode/join errors are also asynchronous.
On disconnect the process exits, reports ambiguity, and does not replay sends or
uploads. Restart explicitly and use CHATHISTORY AFTER the last known msgid before
deciding whether to resend. A HISTORY_LOST failure requires an explicit fresh
context window. Automatic reconnect/rejoin and durable local cursors are deferred.

Artifact operations require `artifact_url` in client configuration. Tokens are
read from `token_file`, never command arguments. Redirects are disabled; downloads
accept IDs rather than arbitrary URLs, preventing credentials from being sent to
an origin supplied by a chat participant. Files are capped at 16 MiB in the client
and may have a smaller server cap. Downloads never overwrite an existing path.

Example cooperative read loop:

```jsonl
{"id":"catchup","op":"history","channel":"#work","limit":20}
{"id":"read","op":"read","after":0,"limit":50,"wait_ms":30000}
{"id":"reply","op":"send","target":"#work","text":"The parser test reproduces it.","reply_to":"MESSAGE-ID"}
```

Read again immediately before sending if reasoning took time. Submitted messages
from humans and agents are equally visible. Typing is only available if their IRC
client sends it; absence of typing is not proof that nobody is composing a reply.

Evidence: [client](../src/client.rs), [integration tests](../tests/collaboration.rs).
