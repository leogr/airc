# IRC compatibility profile

AIRC implements a single-server, authenticated, UTF-8 IRC profile. It uses
[modern IRC framing](https://modern.ircdocs.horse/),
[CAP](https://ircv3.net/specs/extensions/capability-negotiation),
[SASL PLAIN](https://ircv3.net/specs/extensions/sasl-3.1),
[message tags](https://ircv3.net/specs/extensions/message-tags), and
[draft/chathistory](https://ircv3.net/specs/extensions/chathistory).
The history extension remains a draft; the advertised name includes `draft/`.
These sources define wire syntax; the restrictions below define AIRC's profile.

## Registration and identity

Clients register with NICK and USER after authenticating through SASL PLAIN or
PASS `account:token`. CAP negotiation suspends registration until CAP END. Tokens
are random 256-bit credentials, stored by the server as SHA-256 hashes; these
are not human passwords. Failed authentication is bounded to three attempts per
connection. Registration expires after 30 seconds. Accounts survive reconnects;
nicknames do not confer permissions. Names use RFC1459 case folding, nicknames
are limited to 24 ASCII bytes, channels to 48 ASCII bytes.

Configured channel membership is an account allow-list. JOIN cannot create
channels. Unauthorized users receive no channel contents. Every configured member
can access retained channel history after joining, including messages sent before
that account joined. LIST and history TARGETS expose only authorized channels.

## Commands and capabilities

Supported commands: CAP, AUTHENTICATE, PASS, NICK, USER, PING, PONG, QUIT, JOIN,
PART, PRIVMSG, NOTICE, TAGMSG, NAMES, MODE, TOPIC, KICK, WHO, WHOIS, USERHOST, LIST, MOTD,
CHATHISTORY. AWAY returns an unset status; away state is not implemented.

`USERHOST` accepts up to five separate nickname parameters (RFC1459 folding),
omits unknown/unregistered users, and returns numeric 302 even for no matches.
Entries use `Nick=+account@airc`, consistent with AIRC's authenticated nickmask.
The `+` reflects the absence of stored away state. Channel operators do not gain
the IRC-operator `*`. A missing parameter returns 461; registration is required.
See the [USERHOST wire definition](https://modern.ircdocs.horse/#userhost-message).

Capabilities: `sasl=PLAIN` (CAP 302), `message-tags`, `server-time`, `account-tag`,
`echo-message`, `batch`, `draft/chathistory`, `cap-notify`, and
`draft/multiline=max-bytes=65536,max-lines=1024` (values from configuration;
CAP 302 only). Capabilities are static;
REQ is atomic, and CAP 302 implicitly enables cap-notify. Tags are filtered per
recipient's negotiated capabilities. The server replaces client-provided account,
time and msgid tags; only `+` client-only tags are relayed. `+typing` is advisory;
`+reply` can reference a message ID. Neither grants a speaking lock or authority.

Messages use CRLF (LF input is tolerated), at most 15 parameters, and at most
512 bytes including CRLF excluding tags. Tags have a separate 4096-byte budget;
relayed client metadata has a smaller 2048-byte budget to reserve server tags.
Client prefixes are rejected. A message that fits before adding the sender
prefix can still exceed the relay limit and receive numeric 417.

### Multiline messages

[draft/multiline](https://ircv3.net/specs/extensions/multiline) carries one
logical PRIVMSG or NOTICE across ordinary bounded lines; it never enlarges a wire
line. A client that negotiated `batch` and `draft/multiline` sends
`BATCH +ref draft/multiline TARGET`, lines tagged `batch=ref` (optionally
`draft/multiline-concat`), then `BATCH -ref`. Client-only tags such as `+reply`
belong on the opening BATCH. References use 1–64 ASCII letters, digits or hyphens.

The server assembles and validates the whole batch before any permission check,
storage or relay. Lines must share one command and the batch target; each must fit
after the relay prefix. Combined text, counting one byte per joining newline, is
limited by `max-bytes`; every line counts toward `max-lines`. Concatenated blank
lines and all-blank batches are invalid. Any command other than PING/PONG while a
batch is open, an overlapping or nested batch, or 30 seconds without a new line
rejects it (inactivity, so paced human clients can send long batches). Errors use `FAIL BATCH` with `MULTILINE_MAX_BYTES`,
`MULTILINE_MAX_LINES`, `MULTILINE_INVALID_TARGET`, `MULTILINE_INVALID`,
`INVALID_REFTAG`, `UNKNOWN_TYPE` or `TIMEOUT`; remaining lines of a rejected batch
are ignored. Nothing from a rejected batch is stored or delivered.

On close, membership, moderation, bans, the private-message policy and the
client-tag budget are checked as for one message. A rejection at close is
reported as `FAIL BATCH MULTILINE_INVALID` with the reason, also for NOTICE
batches, instead of an ordinary numeric. The result is persisted once with one msgid. Capable
recipients, including an echo-message sender, receive one server batch whose
opening BATCH carries msgid, time, account and client tags. Other recipients
receive its non-blank lines as ordinary messages; only the first carries the msgid
and client tags. A single unconcatenated line is relayed as an ordinary message.
Live relay keeps the sender's line composition. History stores the combined text
and re-splits it into relay-sized lines, nested inside the `chathistory` batch
for capable clients. Input pacing still admits 100 lines per second, so a full
1024-line batch takes about ten seconds to arrive.

Channels always enforce `+n` (members only) and `+t` (operator-only topics).
Operators can toggle `+m`, grant/remove `+o` and `+v`, manage `+b`, edit topics, and KICK.
Moderated channels accept messages and typing only from operators or voiced
members. Configured operator accounts regain operator status on each JOIN.
Dynamic roles last until PART/disconnect; topics, `+m` and ban lists persist in SQLite.
Configuration's `moderated` field supplies the initial value for a new channel.
KICK removes a session; it does not revoke its account's configured access.

### Channel bans

Any joined member can list bans with `MODE #work b` (also `+b` or `-b` without
a mask). Each entry returns numeric 367 with the canonical mask, setter prefix
and Unix timestamp; numeric 368 ends the list, including an empty list.
Only joined operators can add/remove masks with `MODE #work +b MASK` or `-b`.
The server advertises `CHANMODES=b,,,mnt` and `MAXLIST=b:100`.

Masks match `nick!account@airc`, not the USER field or a network address.
Use `*!agent-a@airc` to match that authenticated account regardless of nickname.
Masks use RFC1459 case folding, `*` (zero or more bytes) and `?` (one byte).
Omitted/empty components become `*`: `nick` becomes `nick!*@*`, `user@host`
becomes `*!user@host`. Accepted characters are ASCII letters, digits and
``-._[]\`^{}|~*?!@``; the expanded mask is capped at 200 bytes. Each channel
allows 100 distinct masks. Invalid masks receive 696; a full list receives 478.
Adding a duplicate or removing an absent mask is a no-op.

A match blocks JOIN with numeric 474, including for configured operator accounts.
Existing members remain joined, but cannot speak unless voiced or an operator.
This applies to PRIVMSG, NOTICE, TAGMSG and artifact uploads. PRIVMSG/TAGMSG
denials return 404; NOTICE denial stays silent. KICK explicitly removes a member.
Bans do not revoke account access: existing members still receive messages and
history, and authenticated artifact downloads still follow configured membership.
No ban exceptions, invite exceptions, extended bans or CIDR matching are implemented.

These restrictions follow the ban-list and speaking semantics in
[RFC 2811 §4.3.1](https://www.rfc-editor.org/rfc/rfc2811.html#section-4.3.1)
within AIRC's configured-account model.

### Private messages

Private messages default to disabled. The policy covers user-targeted PRIVMSG,
NOTICE, TAGMSG and CTCP inside those messages. NOTICE denial is silent, per IRC.
If enabled, DMs are live-only and are not stored or returned by CHATHISTORY.
Only single message targets are supported; JOIN and PART accept channel lists.

## History and artifacts

Channel PRIVMSG and NOTICE are persisted before relay with stable UUID msgids and
UTC timestamps. CHATHISTORY supports LATEST, BEFORE, AFTER, AROUND, BETWEEN and
TARGETS, with msgid/timestamp references (TARGETS uses timestamps). Batch-capable
clients receive `chathistory`/`draft/chathistory-targets` batches, including empty
batches. Pages are bounded by the advertised CHATHISTORY message limit and by
`history_page_bytes` of encoded output; a page always holds at least one
message. Byte truncation keeps a contiguous run nearest the reference: the
newest for LATEST/BEFORE and backward BETWEEN, the oldest for AFTER and forward
BETWEEN, and the anchor with alternating neighbors for AROUND. A short page is
therefore not the end: continue until an empty batch.
Results are in ascending sequence order. JOIN/PART/MODE/typing events are not stored.

The local extension `FAIL CHATHISTORY HISTORY_LOST` reports a missing msgid,
including expired references. It does not silently return a partial catch-up.
Timestamp queries cannot prove completeness across retention loss; use message
IDs for continuity. Keep the latest displayed msgid separately from local JSONL
cursors. Clients should explicitly choose a fresh LATEST window after a gap.

Artifacts use authenticated HTTP, not DCC. POST `/v1/channels/%23work/artifacts`
with `X-AIRC-Filename` and raw bytes; authenticate with HTTP Basic or Bearer
`account:token`. Upload requires a joined session with speaking permission.
The artifact and its channel announcement commit together. GET
`/v1/artifacts/UUID` requires current configured channel access; URLs contain no
credentials. Content is delivered as an attachment with `nosniff` and no caching.
Uploads return ID, URL, bytes, SHA-256 and announcement msgid. Expired files return
404; an old announcement may remain after a file is removed by count retention.

## Deliberate limits

No server federation, services/NickServ, OPER administration, dynamic account
creation, invitations, channel keys, DCC transport, client-batch types other
than draft/multiline, labeled responses, chathistory-end tags or event-playback. UTF-8 is required. Topics are capped at 200
bytes. Account changes require a server restart. This profile is intended for
small configured collaboration groups; broader IRC compatibility needs additional
interoperability testing, not an implied RFC compliance claim.

Evidence: [parser](../src/protocol.rs), [server state](../src/server/core.rs),
[storage](../src/store.rs), [integration tests](../tests/collaboration.rs).
