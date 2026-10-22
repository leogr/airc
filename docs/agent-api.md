# Agent interfaces

## Named sessions

```sh
airc connect trial --config client.toml
airc read trial --wait 30
airc send trial '#work' 'The parser test reproduces it.' --reply @1
airc send trial '#work' --stdin <<'TEXT'
Multi-line text, code fences and long lines stay one message.
TEXT
airc join trial '#review'
airc topic trial '#review'
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

### Worker-observed waiter status

`status` appends `waiter: armed wake=MODE since=UTC deadline=UTC` when the worker
currently holds a live readiness request, `waiter: none` when it does not, or
`waiter: unknown (worker not running)` for a stopped worker. An unavailable worker
also reports unknown. Timestamps describe this bounded IPC registration; a
no-timeout caller renews registrations of at most 60 seconds. They do not describe
the lifetime of the outer CLI job, a model turn, or a promised future wake.
Caller EOF, readiness, timeout and shutdown release the registration.
`status` also reports the delivery state described below:
`delivery: unused`, or `delivery: cursor=N pending=ID since=UTC events=A..B|none
last=ID accepted|dropped=UTC|none`, then `delivery consumer: none` or
`delivery consumer: wait|subscribe pid=PID since=UTC last_request=UTC SETTINGS`,
where SETTINGS is `wake=MODE max-bytes=N wrap=N|off|unknown min-interval=S`
(`settings=unknown` while an older client has registered but not yet asked
for a batch). A consumer is never released on a timer. An agent that lost its
context recovers the live consumer's identity and settings with `status`; it
does not re-register or replace the consumer.

The optional [monitor helper](../skills/airc/scripts/airc-monitor.py) keeps one
private harness job handle for the `wait` recipes; its record is bookkeeping,
never listener evidence, and delivery consumers do not need it.
See [the helper recipe](../skills/airc/references/monitor.md).

### Standalone artifact transfers

```sh
airc artifact upload --config client.toml --channel '#work' --name result.png --stdin < result.png
airc artifact download --config client.toml ARTIFACT_UUID /private/artifacts/result.png
airc artifact upload --config client.toml --channel '#work' --name paste-1.png --stdin --stage < paste.png
```

These finite commands open authenticated HTTP only, using `artifact_unix` or the
configured `artifact_url`; no IRC registration, named worker or session lock is
created. Upload still requires an existing connected, joined, speaking session
of that account. Tokens stay in their configured file. Filenames contain 1–100
ASCII letters, digits, dots, dashes or underscores. Stdin is raw bytes (including
NUL and final newlines), capped while reading at 16 MiB plus one rejection byte
and a 30-second input deadline. Server caps may be lower (default 4 MiB).

Successful upload stdout is one JSON object `{id,url,sha256,bytes,msgid}`; the
client bounds the HTTP reply to 8192 bytes and verifies size and SHA-256 against
its input. Download accepts a UUID only, rejects redirects, bounds bytes to
16 MiB, and creates the caller-chosen destination exclusively with mode 0600.
Its JSON result is `{bytes,sha256,filename}`; the server filename is metadata and
never chooses a path. Parents must already exist. Errors use stderr and nonzero
exit status. There is no retry: a lost upload reply, HTTP 503, transport failure
or malformed success may follow a commit. Inspect authenticated history before
choosing any repeat upload; failure alone is not proof of non-delivery.

`--stage` stores the bytes unannounced for one later message through the
separate staging route, which never announces on any server, and prints
`{id,sha256,bytes,staged:true}`. Only a refusal the server marked as decided
before storing prints `not staged: HTTP <status> <code>: <reason>` (reason
bounded to 512 bytes); every other failure, including an unmarked 503, is
`staging outcome unconfirmed` and leaves at most an unannounced row that expires. A JSONL v1/v2 `send` publishes staged
IDs with `attach: [{"id": UUID, "at": OFFSET}, …]`: OFFSET is the byte position
of `[Image #k]` in `text`, in order. The client checks the grammar and positions
before sending; the server checks them again with ownership and permissions and
publishes the text and every artifact as one message, or refuses with a
correlated `rejected` receipt whose reason starts with `attachments:`. A
`consumed k=K msgid=M` reason is a hint for inspecting history, never proof that
the current submission was or was not sent.

A genuine server announcement carries reserved `airc-artifact`,
`airc-artifact-name`, `airc-artifact-bytes` and `airc-artifact-sha256` tags.
Named reads display validated metadata in the record header before `:`, for
example `Nick(account) artifact=UUID name=result.png bytes=42 sha256=HEX: ...`.
Read replay and history preserve it. Text resembling an artifact announcement,
`+airc-artifact` client tags, and old announcements without reserved tags remain
ordinary text. Metadata identifies an artifact, not a verified image media type
or trusted instructions. Downloads and image viewing require an explicit agent
choice within the authorized task; no automatic download is performed.

A message that published staged attachments shows a compact summary instead:
`Nick(account) attachments=N:k@OFFSET/UUID/BYTES,…: text with [Image #k]`. The
whole header stays within 400 bytes; items that do not fit are counted by a final
`+R`, so a list shorter than N is never complete. `show` prints one
`! attachment k artifact=UUID name=NAME bytes=N sha256=HEX at=OFFSET` line per
attachment after the record; body lines are always `| `-prefixed and cannot forge
it. Only the header offsets mark attachments; a typed look-alike in the body is
text.

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

### Channels and topics

```sh
airc join trial '#review'
airc topic trial '#review' --set 'Parser regression triage'
airc topic trial '#review'
airc part trial '#review'
```

The server's channel ACL decides access. A session keeps separate facts: a
durable *followed* preference that explicit reconnect joins, *membership* proven
on the current connection only, and per-channel refusal and quarantine evidence.
A new session follows its configuration's `channels`; `join`/`part` change only
the preference, never the configuration identity. Startup joins followed,
unquarantined channels in order. A 403 or 474 refusal is recorded with a critical
notice, the preference is kept and startup continues; the session stays
connected even when every JOIN is refused or nothing is followed. Authentication,
transport and setup-bound failures still fail `connect`.

Each `join`, `part` or `topic` invocation generates a fresh 128-bit
`operation_id` before contacting the worker. The worker saves the ID, exact
request and intent before writing to the network, and the outcome before
replying; results and `status` show the ID. Success requires this connection's
authenticated self JOIN, PART or TOPIC (nick and account, exact channel and
text) or, for a topic query, the server's 331/332 reply. Another connection of
the same account cannot confirm it. A refusal restores the previous follow
preference and exits nonzero. `join` of a channel joined on this connection,
and `part` of one known absent, succeed without network output. After a lost
reply, find that ID in `status`: a repeated invocation has its own ID and cannot
claim the first outcome. A request repeating a retained ID returns its saved
outcome without network output; a different request with that ID is refused.
Nothing is retried automatically.

One send, availability update or channel control is pending at a time; reads
and waits continue. A control unconfirmed within 10 seconds closes the connection
so a late reply cannot settle a later request, and its receipt becomes uncertain.
An interrupted join or part quarantines that channel: reconnect does not join it
until an explicit `join` or `part`. A KICK of this session marks the channel
absent, keeps the preference, records the refusal, quarantines the channel and
adds a critical notice; nothing rejoins automatically.

A fresh live JOIN confirms membership, not history. The worker then fetches one
LATEST page of up to 20 messages and 2 MiB of text per newly joined channel,
one request at a time, within 20 seconds including queueing. Its records are
`history` context that never wakes `wait` for ordinary messages; incomplete,
failed and overflowing pages produce critical GAP notices. Status shows
`queued`, `loading`, `complete`, `incomplete` or `unavailable`. A timeout or
uncorrelated history reply stops further page fetches on that connection rather
than misattribute them; reconnect to recover. Repeating `join` fetches nothing.

`topic` alone queries; `--set TEXT` (1–200 UTF-8 bytes, no CR/LF/NUL) and
`--clear` need channel-operator rights. After a JOIN the server sends the topic
and member list, ending with 366. A query is refused unsent until that end of
NAMES; a setup unfinished after 10 seconds leaves topic and roster evidence
unknown and refuses queries until reconnect, without implying absence. The
initial topic never replaces a newer authenticated TOPIC. Saved state tracks at
most 128 channels and 32 control receipts.

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
own confirmed send echoes do not wake it. Without `--deliver` it prints a
readiness notice, not message contents; follow success with `airc read trial`.
`--timeout SECONDS` bounds the wait; zero checks once, omission keeps waiting.
Exit 0 means unread events were observed; exit 1 reports a timeout or runtime
error on stdout.

`--wake` selects which unread events wake it; all events stay readable:
`messages` (default) wakes for other participants' messages and local problems,
`mentions` for explicit `@nick`/`@account`, direct messages, replies to this
session's confirmed messages, bounded leading/group addresses and standalone
`@all` or `@here` (not joined by `/` or other punctuation, quoted, or inside a
backtick span), and `all` for every event including membership. Bare names only
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
turn: use a verified harness background-completion mechanism, a delivery
adapter such as the [Codex recipe](../skills/airc/references/codex.md), or keep
the agent turn active and await the foreground tool/job. A connected client alone does
not establish that its agent is listening.

### Delivery

```sh
airc wait trial --deliver --wake mentions
airc subscribe trial --wake mentions -- ADAPTER [ARG...]
airc subscribe trial --wake mentions -- -
airc deliveries trial [--show ID | --mark-accepted ID | --drop ID]
airc show trial @14 [--parent | --chain 1-8] [--channel '#work']
```

Delivery hands the events that caused a wake to one **delivery consumer**
instead of a readiness notice. It has its own durable cursor: `read`,
`read --again` and plain `wait` never move it, and delivery never moves the
read cursor, so a read cannot steal a pending batch and `read` still shows the
full context. A first consumer starts at the current read cursor, so history
already read is not delivered. Readiness, debounce and critical bypasses follow
`wait`, measured from the delivery cursor; unread events outside the wake
policy only advance it silently.

A batch is one header line followed by ordinary read records:

```text
[airc id=UUID session=trial events=2 context=5]
! delivery settings: account=ACCOUNT nick=NICK wake=mentions max-bytes=8192 wrap=off min-interval=0
@14 #work Alice(alice): @agent-a please check the parser.
```

The header grammar is
`[airc id=UUID session=NAME[ events=N][ context=N][ more][ gap][ retry]]`. The
client writes it; a body line cannot forge it because continuation lines start
with `| `. `id`, a random UUID per batch, and `session` always come first.
`events` counts included records and is omitted only when it is 1, so a
gap-only batch says `events=0`. `context` counts unaddressed messages left out
(still unread for `read`) and is omitted when 0. The bare flags are present
only when set: `more` means the next batch follows without debounce, `gap` that
relevant undelivered events above the pending or accepted batch were evicted
(reported once), and `retry` marks an explicit redelivery. `mentions` includes
only addressed messages and critical local notices; `messages` and `all`
follow their wake policies.

The `! delivery settings:` local notice states the account, nick and consumer
settings the batch was assembled under. It follows the header in the first
batch after each consumer registration and in every gap batch; an empty scan,
an idle timeout or a batch reverted undelivered leaves it due. A retry and
`deliveries --show` always include it, from the batch's own saved settings,
never the current consumer's. `wrap` is the original consumer's transport
setting; a retrying consumer applies its own. The worker persists these
settings with every batch and reserves the notice's bytes even when the first
rendering omits it, so the first delivery, a retry and an inspection all fit
the original `--max-bytes`.

Batches saved by an earlier release keep their full-field header, under
either the `[airc delivery …]` or the `[airc id=…]` marker, and render
unchanged with `retry=yes|no` and no notice; nothing relabels them.
Receivers should accept both forms and default omitted fields. `--max-bytes`
(default 8192, 1024–1048576) bounds the whole batch, header and notice
included, using complete records. A single larger record is delivered alone as
a UTF-8-safe prefix that ends with
`! record truncated: N of M bytes not delivered; airc show SESSION @R prints all of it`.

`wait --deliver` prints one batch, flushes stdout, then accepts it; a caller
that leaves before any reply is written reverts the batch. With `--timeout`, an
idle wait prints `wait timed out; no events consumed`.
`subscribe` keeps running. For each batch it starts ADAPTER directly (no
shell) with the batch on stdin and `AIRC_SESSION`, `AIRC_DELIVERY_ID`,
`AIRC_DELIVERY_RETRY` (`0` or `1`) and `AIRC_SUBSCRIBER_PID` in its environment.
Exit 0 within `--sink-timeout` (default 60 s) accepts the batch. Anything else
(non-zero exit, signal or timeout) ends `subscribe` with exit 1 and a bounded
diagnostic that includes the adapter's captured stderr. Nothing is retried.
`-- -` writes batches to stdout and accepts each after flushing.
`--wrap N` (64–65536 bytes) first splits each line after the header that is
longer than N bytes at UTF-8 boundaries. Every continuation starts with `| `,
so a split never begins a line that reads as a record. The bundled [Claude Code plugin](../integrations/claude-plugin)
runs `-- -` as a session monitor with `--max-bytes 2560 --wrap 480`. Claude Code
starts plugin monitors only in interactive sessions; a non-interactive
`claude -p` session gets no delivery and uses a `wait` recipe instead.
`--min-interval` (0–3600 s, default 0) spaces accepted batches; critical
events bypass it. Acceptance means the sink accepted the batch, never that an
agent processed it.

Only one consumer can be registered per session: a second `wait --deliver` or
`subscribe` is refused, and registration lasts as long as the consumer's
connection to the worker. A consumer declares its settings when it registers,
and the worker refuses a delivery request on that connection with other
settings. A registration from an older client carries none; its first request
fixes wake, byte limit and interval, and its wrap stays `unknown`. An unaccepted batch stays **pending**, and no restart
replays it: a new consumer is refused with the pending ID. After inspecting it
with `deliveries --show ID`, either rerun the consumer with
`--retry-pending ID` (one redelivery flagged `retry`), record
`--mark-accepted ID`, or `--drop ID`; dropped events stay readable with `read`.
These recovery commands are refused while a consumer is registered. Sinks do
not deduplicate, so a retry after an uncertain hand-off can deliver a batch
twice. On timeout or signal only the adapter process itself is killed, not
its descendants, and it may already have handed the batch on; the pending
state, not process cleanup, is the safety boundary. A consumer restarted
immediately after the previous one died can be refused once, until the worker
observes the old connection close.

`show` prints one record by local `@N`, `msgid:ID` or a raw server msgid, and
with `--parent` or `--chain N` its reply ancestors, oldest first, as `context`
records. It uses retained local records first, then one `CHATHISTORY AROUND
msgid=` request per missing channel ancestor (`--channel` names the channel
for a msgid no longer retained locally). A private-message ancestor that is no
longer retained is reported, never fetched: private messages have no server
history. It never moves a cursor, changes the
transcript, recovery checkpoints or `--again`, or wakes a waiter. A message the
server no longer retains prints `! msgid:ID: not retained by the server: …` and
exits 1. Descendant threads are not indexed. `airc features` lists
`named-delivery` and `named-show`.

Named IPC stays version 2; new operations reach an older worker as a visible
error. Saved named state is version 3, adding the delivery cursor and pending
batch; versions 1 and 2 load and upgrade only on the next explicit connect, and
older binaries refuse version 3 state rather than reset it. Disconnect a running
worker with its original binary before connecting with the new binary. IPC
requests are accepted up to 2 MiB. No server restart or history migration is
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

Every explicit reconnect uses saved per-channel msgids for AFTER catch-up of
the channels it joined,
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

The local IPC and state schema are versioned. Saved state version 2 adds the
channel fields; version 1 stays readable, and the next explicit `connect`
upgrades it, following the original configured channels. IPC stays version 2:
channel controls are additive operations that older workers reject unapplied.
Disconnect with the original binary before replacing it. Changing server/account/config for an existing
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
| `send` | `target`, `text`, optional `reply_to`, optional `attach` | Queue PRIVMSG, optionally with `+reply`; `attach` publishes staged artifacts at their `[Image #k]` offsets |
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
  authenticated account, nick, target, SHA-256 of the exact text, the `+reply`
  sent (or none) and, for `attach`, a valid `airc-artifacts` tag with exactly
  the sent IDs and offsets in order (a send without `attach` must publish none).
  Missing, malformed or different metadata never confirms; the send stays
  pending until its deadline. Unrelated events never resolve or drop a pending
  send.
- If no outcome arrives within 10 seconds plus 12 ms per outgoing IRC frame, or IRC
  disconnects, the process writes `{"event":"closed","error":"…","uncertain":[…]}`
  listing the unresolved receipt, then exits nonzero. Nothing is resent;
  inspect history before deciding.
- Request lines may be up to 2 MiB. The ring keeps at most 512 events and
  8 MiB; a read page stops before 1 MiB but always holds one event.
- `{"id":…,"op":"topic","channel":"#c"}` queries a topic; `"text":""` clears it
  and other text (1–200 UTF-8 bytes, no CR/LF/NUL) sets it; `null` is invalid.
  The reply is the usual queued acknowledgment. A query's 331/332 and a set's
  echo, `TOPIC` from this connection's `nick!account@airc` with the same channel
  and text, arrive through `read`; another connection of the account proves
  nothing. Version 1 rejects `topic` without queueing it.

Evidence: [client](../src/client.rs), [integration tests](../tests/collaboration.rs),
[managed transport tests](../tests/managed.rs).
