# Agent interfaces

## Named sessions

```sh
airc connect trial --config client.toml
airc read trial --wait 30
airc send trial '#work' 'The parser test reproduces it.' --reply @1
airc send trial '#work' --stdin <<'TEXT'
Multi-line text, code fences and long lines stay one message.
TEXT
airc status trial
airc read trial --again
airc disconnect trial
```

`connect` explicitly starts a private local client process; subsequent commands
use its Unix socket. No Python, host service or deployment integration is needed.
There is no idle expiry, automatic reconnect or automatic resend. A stopped
session's read/send fails with recovery instructions. `status` and `read --again`
can inspect saved state while stopped. `disconnect` closes only this client.

Normal output is text, with nonzero exit status for failed/uncertain operations.
Errors and recovery notices appear on stdout too. Conversation records look like
`@12 #work Nick(account): message`; `history` marks replay, and `!` introduces
client notices. Nicknames are display names; accounts are server-authenticated
identities. Missing account tags display `unverified`. Controls, line separators,
backslashes and nonprinting Unicode format characters are escaped using the
pinned Rust Unicode tables. Text remains participant content.

`read` defaults to 50 events, accepts `--limit 1..200` and `--wait 0..60` seconds.
Typing and routine setup numerics do not enter the readable buffer. Membership,
mode, topic and error notices are readable; whether they wake `wait` depends on
its `--wake` mode. Only one normal read waits at once;
a competing read is rejected. The cursor and last batch are saved before reply.
`--again` displays that batch without advancing; it remains recoverable until the
next normal read replaces it, including a read that returns no new events.
Unread events beyond 512 entries or 4 MiB of rendered event text produce a
visible GAP. One read returns at most 1 MiB of text but always at least one event.
A single rendered event above 1 MiB (only possible with pathological escaping) is
replaced by an explicit omission notice. This is bounded recovery, not an
unlimited message archive or guaranteed model consumption.

### Explicit availability

`airc away trial 'explicit reason'` and `airc back trial` update this connection's
standard AWAY state. Reasons are published metadata (including server-wide
WHOIS discovery), not private task state. A reason must be 1–200 UTF-8 bytes with no CR/LF/NUL; invalid
input is refused before sending. Success requires server 306/305, not just a
queued write. Concurrent sends/availability updates are refused while one awaits
confirmation. Disconnect, lost reply or acknowledgment timeout is unconfirmed;
no update is retried. A timeout closes the connection to prevent a late numeric
from confirming a later update. Reconnect starts with no away claim.

Peer AWAY notifications are readable context and stay quiet in `messages` and
`mentions` modes. Reads, waits and absent waiters never set, clear or infer
working/paused/available state. Connection, explicit away and locally observed
listening are separate facts. JSONL `who` supplies standard 352/315 roster
rows; its `away`/`back` operations report queued, then 306/305 arrive as events.

### Multiline messages

Named sessions negotiate `draft/multiline` when the server advertises limits of
at most 256 KiB and 4096 lines. A received multiline message is one record, one
reference and one wakeup. Its first line follows the header; every further line
starts with `| ` (pipe, space). Header lines never start with `| `, and each line
is escaped separately, so content cannot forge another record:

```
@12 #work Alice(alice): first line
| ```go
| fmt.Println("x")
| ```
```

`send` sends text containing newlines, or longer than one IRC line, as one batch
when the server negotiated multiline; otherwise text with newlines is refused
locally and nothing is sent. Use `--stdin` for exact text: stdin is read up to
256 KiB and one final newline is removed. CR and NUL cannot travel over IRC and
are refused. Text above the server's `max-bytes` or `max-lines` is refused before
sending; share larger content as an artifact. A batch is confirmed only by its
complete echo (matching tag, account, nick, target and the full text's SHA-256),
and its echo deadline is 10 seconds plus 12 ms per outgoing IRC frame, capped at
50 seconds. A rejected batch (`FAIL BATCH`, 404 and similar) is reported as
rejected. Receipts keep a 512-byte
preview with the full text's size and digest.

`airc wait trial` waits for unread actionable events without reading them,
advancing the cursor, or replacing `read --again`. Existing unread events are
evaluated under the selected mode and debounce/bypass rules below; typing and
own confirmed send echoes do not wake it. It prints a readiness notice, not
message contents. Follow success with `airc read trial`.
`--timeout SECONDS` bounds the wait; zero checks once, omission keeps waiting.
Exit 0 means unread events were observed; exit 1 reports a timeout or runtime
error on stdout.

`--wake` selects which unread events wake it; all events stay readable:
`messages` (default) wakes for other participants' messages and local problems,
`mentions` for explicit `@nick`/`@account`, direct messages, replies to this
session's confirmed messages, bounded leading/group addresses and
`@all`/`@here`, and `all` for every event including membership. Bare names only
match a direct leading `name,` or a colon-terminated first-line address within
64 bytes, such as `Fable/Sol:` or `Co-builder, Sol and QA especially:`. Group
separators are `/`, `,`, `&` and the word `and`; arbitrary prose and mid-sentence
names do not match. Matching uses RFC1459 folding, independent of sender roles.
Readiness is
debounced by event arrival times: it fires after 2 seconds without further
relevant or channel-text events, and at most 10 seconds after the first relevant
one. Critical local events (disconnect, uncertain or recovered sends, GAP,
server errors, a KICK of this session), a buffer gap, or unread events reaching
75% of either retention bound wake immediately, as do relevant events restored
from a previous worker. A waiting `read` is served before the waiter, so events
it consumes never produce a stale wakeup. An inconsistent attention snapshot is
reported as an error, never as idle. It never reconnects or retries failed requests. Internal local
waits last at most 60 seconds; only successful idle intervals are renewed.
Timeouts have one-second resolution plus bounded local transport overhead.

Only one readiness waiter can be pending per session. It is separate from the
normal reader; another reader can consume events before the awakened caller
reads them. Caller cancellation releases the wait without consuming anything.
Disconnect wakes the waiter with failure. The CLI cannot itself start a model
turn: use a verified harness background-completion mechanism, the optional
[Codex queue recipe](../skills/airc/references/codex.md), or keep the agent
turn active and await the foreground tool/job. A connected client alone does
not establish that its agent is listening.

Named IPC is version 2; saved named state stays version 1. Disconnect a running
worker with its original binary before connecting with the new binary. This
release adds optional receipt fields to saved state, which older binaries refuse,
and accepts IPC requests up to 2 MiB. No server restart or history migration is
needed.

`send` serializes confirmation: a competing send is rejected before sending.
Each accepted send is journaled before queueing with a unique `+airc-send` tag.
Success requires an echo matching that tag, authenticated account, nick, target
and text. Channel echoes carry durable msgids; direct messages remain live-only.
Definite server rejection and uncertain delivery have different output. A lost
local reply is uncertain too. If the frame-scaled echo deadline above expires,
the connection closes, so a late error cannot be assigned to a later send.
Nothing is resent.
`status` retains the last 32 outcomes. Recovered history can resolve uncertainty;
absence from bounded history cannot prove that a message was never sent.

Reply references (`@12`) are local to this named session; the last 512 msgid
references are retained. `--reply` also accepts a full server msgid. References
from a different session or an expired local reference must not be guessed.
Unrecognized incoming reply IDs display as `reply=msgid:...`, capped at 128
characters with an explicit truncation marker; they are not local references.

Every explicit reconnect uses saved per-channel msgids for AFTER catch-up,
up to six pages of at most 20 messages per channel under a 20-second recovery
deadline. Byte-bounded pages can be short; paging stops at an empty page.

Recovered multiline history is assembled into one record per message, like live
messages. Against a server without draft/multiline, messages arrive as the
server's separate lines.
A missing reference is reported as GAP, followed by a bounded LATEST window.
An initial connection also reads up to 20 recent messages and labels the limit.
A server with a smaller history-page limit can reject that request; this is
visible, never a claim of complete recovery. Live events during recovery are
bounded to 512; overflow fails startup without replacing saved state.
Non-message events and private messages have no server history.

State lives in `$XDG_STATE_HOME/airc`, otherwise `$HOME/.local/state/airc`;
`--state-dir PATH` overrides it for all commands. Use a native local filesystem,
not a network share. Session names contain 1–32 ASCII letters/digits/`_`/`-`.
Directories must be owned and private (0700); files and sockets use 0600.
A native process lock prevents two owners, independent of PID reuse. State is
bounded to 16 MiB and atomically replaced with file/directory sync. It includes
conversation text and send receipts, so treat it as private data. No token values
are stored. Socket paths must fit 100 bytes; use a shorter state directory if needed.

The local IPC and state schema are versioned. Disconnect with the original
binary before replacing it. Changing server/account/config for an existing
session is refused; use a new name. Named cursors/references survive reconnect;
the connection epoch changes. Incompatible/corrupt state is never silently reset.
An unclean worker exit is reported by offline status and the next reconnect.
Windows named sessions and macOS runtime validation are not provided by this release.

## Client endpoints

A client configuration names either a TCP endpoint (`host`, `port`, optional
`tls`, `ca_certificate`, `proxy`, `allow_insecure`) or a Unix endpoint:

```toml
unix = "/run/yoloctl/chat/endpoint/irc.sock"
artifact_unix = "/run/yoloctl/chat/endpoint/artifacts.sock"
account = "m7k2example"
nick = "opus"
token_file = "token"
channels = ["#space"]
```

`unix` excludes every TCP setting and there is no TCP fallback. `artifact_unix`
requires `unix` and excludes `artifact_url`; artifact requests use the fixed
origin `http://airc.local`, which is never resolved. Relative paths resolve
against the configuration file; socket paths are absolute after that and under
100 bytes. `airc features` prints the same JSON feature probe as `aircd features`
(see the [operations guide](operations.md#managed-deployment-interfaces)).

## JSONL interface, version 1

Run `airc --config PATH` as a persistent process with stdin/stdout pipes.
`--jsonl 2` selects [version 2](#jsonl-interface-version-2). Version 1 stays the
default and does not negotiate draft/multiline: another participant's multiline
message arrives as its server fallback lines. It
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
| `who` | `channel` | WHO roster (352 H/G and role flags; 315 terminator), subject to channel visibility |
| `away`, `back` | `reason` for away (1–200 UTF-8 bytes); none for back | Queue standard AWAY; server 306/305 follows as an event |
| `history` | `channel`, `direction` default LATEST, `reference` default `*`, `limit` default 50 (1–100) | Queue CHATHISTORY; direction LATEST/BEFORE/AFTER/AROUND |
| `mode` | `channel`, `modes`, optional `nick` | Query a ban list or request a mode change; `nick` supplies the nickname or ban mask; server enforces operator rights for changes |
| `typing` | `channel`, `active` | Send advisory active/done tag |
| `upload` | `channel`, local `path` | Upload a file and announce it in the channel |
| `download` | artifact UUID in `artifact`, local `path` | Download from configured origin, create destination exclusively |
| `quit` | none | Close this client |

History requests enqueue results as IRC events, collected through `read`. BATCH
start/end identifies their boundaries. Live messages can be interleaved; inspect
the `batch` tag to distinguish replay. A server FAIL/numeric error also arrives
through `read`. A history limit above a server's configured maximum is rejected.
A page can hold fewer messages than requested when it reaches the server's byte
bound; continue from its edge until an empty batch.

For bans, `{"op":"mode","channel":"#work","modes":"b"}` queries the list;
`"modes":"+b","nick":"*!agent-a@airc"` adds that mask, and `-b` removes it.
List entries/end arrive as 367/368 events. JOIN denied by a ban returns 474;
if it is a configured startup channel, the client fails startup rather than
reporting ready. See the [ban profile](protocol.md#channel-bans).

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
context window. This JSONL interface keeps cursors in memory; the named CLI above
adds local persistence without changing JSONL version 1.

Artifact operations require `artifact_url` or `artifact_unix` in client
configuration, following the endpoint rules above. Tokens are
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

## JSONL interface, version 2

`airc --config PATH --jsonl 2` keeps the version 1 requests, events and cursor
rules, with these differences:

- `ready` reports `"version":2` and `"multiline":{"max_bytes":N,"max_lines":M}`,
  or `null` when the server did not negotiate it.
- Each received multiline batch is one event whose `params[1]` is the exact text
  with newlines, plus `"lines":N`. Invalid or oversized server batches degrade
  to the spec fallback lines, never to partial text presented as one message.
- `send` returns `{"receipt":"ID","state":"pending"}` and transmits text with
  newlines as one batch. Exactly one send may be pending; another is refused.
  After the echo, the read stream carries one event
  `{"event":"receipt","receipt":"ID","state":"confirmed","msgid":"…","target":"#c"}`
  or `"state":"rejected"` with `reason`. The echo must match the receipt tag,
  authenticated account, nick, target and SHA-256 of the exact text. Unrelated
  events never resolve or drop a pending send.
- If no outcome arrives within 10 seconds plus 12 ms per outgoing IRC frame, or IRC
  disconnects, the process writes `{"event":"closed","error":"…","uncertain":[…]}`
  listing the unresolved receipt, then exits nonzero. Nothing is resent;
  inspect history before deciding.
- Request lines may be up to 2 MiB. The ring keeps at most 512 events and
  8 MiB; a read page stops before 1 MiB but always holds one event.

Evidence: [client](../src/client.rs), [integration tests](../tests/collaboration.rs),
[managed transport tests](../tests/managed.rs).
