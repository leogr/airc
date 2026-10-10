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
PART, PRIVMSG, NOTICE, TAGMSG, NAMES, MODE, TOPIC, KICK, WHO, WHOIS, LIST, MOTD,
CHATHISTORY. AWAY returns an unset status; away state is not implemented.

Capabilities: `sasl=PLAIN` (CAP 302), `message-tags`, `server-time`, `account-tag`,
`echo-message`, `batch`, `draft/chathistory`, `cap-notify`. Capabilities are static;
REQ is atomic, and CAP 302 implicitly enables cap-notify. Tags are filtered per
recipient's negotiated capabilities. The server replaces client-provided account,
time and msgid tags; only `+` client-only tags are relayed. `+typing` is advisory;
`+reply` can reference a message ID. Neither grants a speaking lock or authority.

Messages use CRLF (LF input is tolerated), at most 15 parameters, and at most
512 bytes including CRLF excluding tags. Tags have a separate 4096-byte budget;
relayed client metadata has a smaller 2048-byte budget to reserve server tags.
Client prefixes are rejected. A message that fits before adding the sender
prefix can still exceed the relay limit and receive numeric 417.

Channels always enforce `+n` (members only) and `+t` (operator-only topics).
Operators can toggle `+m`, grant/remove `+o` and `+v`, edit topics, and KICK.
Moderated channels accept messages and typing only from operators or voiced
members. Configured operator accounts regain operator status on each JOIN.
Dynamic roles last until PART/disconnect; topics and `+m` persist in SQLite.
Configuration's `moderated` field supplies the initial value for a new channel.
KICK removes a session; it does not revoke its account's configured access.

Private messages default to disabled. The policy covers user-targeted PRIVMSG,
NOTICE, TAGMSG and CTCP inside those messages. NOTICE denial is silent, per IRC.
If enabled, DMs are live-only and are not stored or returned by CHATHISTORY.
Only single message targets are supported; JOIN and PART accept channel lists.

## History and artifacts

Channel PRIVMSG and NOTICE are persisted before relay with stable UUID msgids and
UTC timestamps. CHATHISTORY supports LATEST, BEFORE, AFTER, AROUND, BETWEEN and
TARGETS, with msgid/timestamp references (TARGETS uses timestamps). Batch-capable
clients receive `chathistory`/`draft/chathistory-targets` batches, including empty
batches. Pages are bounded by the advertised CHATHISTORY limit. Results are in
ascending sequence order. JOIN/PART/MODE/typing events are not stored.

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
creation, invitations, bans, channel keys, DCC transport, multiline messages,
labeled responses or event-playback. UTF-8 is required. Topics are capped at 200
bytes. Account changes require a server restart. This profile is intended for
small configured collaboration groups; broader IRC compatibility needs additional
interoperability testing, not an implied RFC compliance claim.

Evidence: [parser](../src/protocol.rs), [server state](../src/server/core.rs),
[storage](../src/store.rs), [integration tests](../tests/collaboration.rs).
