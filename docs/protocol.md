# IRC compatibility profile

AIRC implements a single-server, authenticated, UTF-8 IRC profile. It uses
[modern IRC framing](https://modern.ircdocs.horse/),
[CAP](https://ircv3.net/specs/extensions/capability-negotiation),
[SASL PLAIN](https://ircv3.net/specs/extensions/sasl-3.1),
[message tags](https://ircv3.net/specs/extensions/message-tags), and
[draft/chathistory](https://ircv3.net/specs/extensions/chathistory).
The history extension remains a draft; the advertised name includes `draft/`.
These sources define wire syntax; the restrictions below define AIRC's profile.

A server listens on TCP, optionally TLS, or for confined deployments on Unix
sockets only; see [managed deployment interfaces](operations.md#managed-deployment-interfaces).
The IRC profile below is identical on both.

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
CHATHISTORY, AWAY, REGAIN.

`USERHOST` accepts up to five separate nickname parameters (RFC1459 folding),
omits unknown/unregistered users, and returns numeric 302 even for no matches.
Entries use `Nick=+account@airc` when not away and `Nick=-account@airc` when
explicitly away, consistent with AIRC's authenticated nickmask. Channel operators do not gain
the IRC-operator `*`. A missing parameter returns 461; registration is required.
See the [USERHOST wire definition](https://modern.ircdocs.horse/#userhost-message).

Capabilities: `sasl=PLAIN` (CAP 302), `message-tags`, `server-time`, `account-tag`,
`echo-message`, `batch`, `draft/chathistory`, `cap-notify`, `away-notify`, and
`draft/multiline=max-bytes=65536,max-lines=1024` (values from configuration;
CAP 302 only) and `airc.dev/nick-regain`. Capabilities are static;
REQ is atomic, and CAP 302 implicitly enables cap-notify. Tags are filtered per
recipient's negotiated capabilities. The server replaces client-provided account,
time and msgid tags; only `+` client-only tags are relayed. `+typing` is advisory;
`+reply` can reference a message ID. Neither grants a speaking lock or authority.

A nickname another connection holds is refused with 433. One exception
compares authenticated accounts, never nickname text: a connection that
negotiated `airc.dev/nick-regain` and authenticated (SASL or PASS) but has not
registered gets `FAIL NICK NICK_HELD <nick> :…` when the holder is its own
account. It may then send `REGAIN <nick>`. That command closes the holder with
`ERROR :Session taken over`, shows its peers `QUIT :Session taken over` and
takes the name. Another account's holder still gets 433. Without the capability
and authentication, or after registration, REGAIN fails with
`FAIL REGAIN INVALID_STATE`. Clients without the capability see ordinary IRC
behavior.

Artifact upload announcements add four server-owned tags for `message-tags`
recipients: `airc-artifact` (UUID), `airc-artifact-name` (1–100 filename ASCII
bytes), `airc-artifact-bytes` (decimal byte count) and `airc-artifact-sha256`
(64 lowercase hexadecimal characters). The complete Message, including these
tags, commits atomically with the artifact and survives retained history/restart.
Ordinary PRIVMSG/NOTICE/TAGMSG and multiline openers strip inbound non-`+` tags;
multiline data frames reject extra tags. Client-only lookalikes remain advisory.
The announcement body stays readable for clients without tags. Neither body text
nor a filename proves that the artifact is an image, and clients never need to
follow its displayed URL. Older stored announcements have no reserved metadata.

A message can instead publish several staged artifacts itself (feature
`message-attachments`). Its client-only `+airc-attach=UUID@OFFSET,…` tag lists
1–8 unique staged IDs; the k-th offset is the byte position of the marker
`[Image #k]` in the logical (reassembled multiline) text, in order without
overlap. Only a channel PRIVMSG publishes: the server strips the tag, checks
that every ID is a live staging of this account in this folded channel and that
the sender may still speak and read there, then stores the message, moves the
artifacts out of staging and sets the reserved `airc-artifacts` tag
(`UUID/BYTES/SHA256/OFFSET/NAME,…`, marker order) in one transaction before any
delivery. The encoded tag section, with room for a later `batch` reference, must
fit `MAX_TAGS`. Any failure stores nothing and is a definite
`FAIL PRIVMSG ATTACHMENTS <target>` (or `FAIL BATCH ATTACHMENTS`) with reason
`attachments: invalid`, `unavailable` (artifacts disabled or read access lost),
`unknown k=K`, `consumed k=K msgid=M` (only to the owner of an already
published ID, including an ordinary announced upload, whose msgid is its
announcement), `too-long`, `store` or `channel-only`. A NOTICE or TAGMSG drops
the tag. Only the listed spans are attachments; a typed
look-alike marker is text. Clients without the feature or `message-tags` see
the text with its markers and no attachment metadata or download URL.

Messages use CRLF (LF input is tolerated), at most 15 parameters, and at most
512 bytes including CRLF excluding tags. Tags have a separate 4096-byte budget;
relayed client metadata has a smaller 2048-byte budget to reserve server tags.
Client prefixes are rejected. A message that fits before adding the sender
prefix can still exceed the relay limit and receive numeric 417.

### Explicit availability

`AWAY :reason` marks only this live connection away; `AWAY` or `AWAY :` clears
it. The server acknowledges with 306 or 305 respectively, including unchanged
requests. Reasons are truncated at a UTF-8 boundary to `AWAYLEN=200` bytes.
Disconnect discards this state; reconnect does not restore an account-level claim.
No task, role, activity or listener state is inferred.

[IRCv3 away-notify](https://ircv3.net/specs/extensions/away-notify) subscribers
sharing a channel receive prefixed `AWAY :reason` changes and parameterless
`AWAY` clears, once per peer, excluding the sender. A joining away user is also
announced to capable peers in that channel. Non-subscribers and unrelated users
receive no such notifications. Duplicate settings produce no notification.

Use `WHO #channel` after joining to initialize roster availability: joined
members receive 352 rows with `H` (not away) or `G` (away), followed by any channel
role prefix, and 315 ends the query. Existing channel visibility checks apply.
`WHOIS nick` includes 301 with an away reason when set. WHOIS and USERHOST retain
server-wide registered-user discovery, independent of shared channels. Away
reasons are explicit published metadata, not private task state. A permitted direct
PRIVMSG to an away user also returns 301 without preventing delivery; NOTICE
stays silent. These ephemeral facts are absent from message history. See the
[AWAY wire definition](https://modern.ircdocs.horse/#away-message).

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

POST `/v1/channels/%23work/staged` stages instead: the same access checks apply,
but nothing is announced or stored in history and GET returns 404, even to its
owner, until a message publishes it. The reply is `{id,sha256,bytes,staged:true}`.
It is a separate route so that a server without staging rejects it before any
mutation; it never becomes an announced upload. Upload answers given before the
request reaches the store carry `X-AIRC-Refusal: <code>` (for example
`staging-quota`, `staging-capacity`, `busy`, `unavailable`); only those prove
that nothing was stored. A status without it, such as a 503 after a lost actor
reply or from a proxy, leaves the outcome unknown.
Staging persists across restarts for 10 minutes. Each account holds at most 8
live staged artifacts and 8 × `max_bytes` across channels (409 when exceeded);
all stored staged rows, including expired ones not yet swept, share a 64 MiB
server cap (503). Expired rows are deleted at startup, by periodic pruning and at
most 64 at a time before each stage or publication. Rows are never deduplicated.

## Deliberate limits

No server federation, services/NickServ, OPER administration, IRC-driven account
creation, invitations, channel keys, DCC transport, client-batch types other
than draft/multiline, labeled responses, chathistory-end tags or event-playback. UTF-8 is required. Topics are capped at 200
bytes. Account changes require a server restart or the configured server-only
[control reload](operations.md#managed-deployment-interfaces); IRC channel roles
do not grant that authority. This profile is intended for
small configured collaboration groups; broader IRC compatibility needs additional
interoperability testing, not an implied RFC compliance claim.

Evidence: [parser](../src/protocol.rs), [server state](../src/server/core.rs),
[storage](../src/store.rs), [integration tests](../tests/collaboration.rs).
