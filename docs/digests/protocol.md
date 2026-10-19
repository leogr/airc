# Protocol and permissions

Verification: `c1aaff4f72a8cea262c129b0c14f243c0758fd38` and accompanying changes; re-read cited symbols when behavior changes.

[FACT] The parser accepts UTF-8, bounds the non-tag IRC message to 512 bytes,
limits parameters to 15, decodes tag escapes, and rejects embedded CR/LF/NUL.
The bounded reader refuses oversized or truncated frames. RFC1459 folding
governs names; nick/channel validation defines the supported ASCII profile.
[1](#source-1)

[FACT] Registration requires NICK, USER and account authentication. CAP pauses
registration until END; REQ validates the whole list before changing capability
state. SASL PLAIN decodes bounded chunks, and PASS supports account/token login.
Authoritative identity is stored separately from nickname. [2](#source-2)

[FACT] JOIN checks configured account access and bans. Configured operator accounts gain
operator status on each successful join. NAMES reports operator/voice prefixes. MODE changes
moderation, bans and ephemeral roles; TOPIC requires operator authority. KICK removes
the target session without changing account configuration. [3](#source-3)

[FACT] Joined members query `b` without a mask using 367 entries and a 368 terminator;
operators add/remove masks. Canonical RFC1459 masks match `nick!account@airc`
with `*`/`?`; omitted components become wildcards. Masks are at most 200 ASCII
bytes and lists at most 100 entries, advertised as `MAXLIST=b:100`. Metadata
stores the setter prefix and Unix timestamp. Matching JOINs receive 474; existing
members remain joined but need voice/operator status to speak. Bans do not revoke
configured account reads. [3](#source-3) [9](#source-9) [10](#source-10)

[FACT] Chat checks membership/moderation/bans and the private-message switch before
relaying. Ordinary NOTICE denial is silent; TAGMSG requires negotiated tags. Client-only
tags are bounded and authoritative account/time metadata is replaced. Persisted
messages receive server msgids. Recipient capabilities filter outgoing tags.
[4](#source-4) [5](#source-5)

[FACT] CHATHISTORY supports all six documented subcommands, joins/ACL checks,
bounded limits, msgid/timestamp selection and batch replies. Missing msgids return
HISTORY_LOST, rather than silently establishing a continuous catch-up. TARGETS
filters configured channel access. Live-only direct messages and ephemeral state
events are not persisted. [6](#source-6) [4](#source-4)

[FACT] Integration tests cover account/tag spoofing, private-message and NOTICE
policy, moderation, unauthorized history, capability atomicity, retention gaps
and history continuity across restart. Ban tests cover queries, operator-only writes,
bounds, nickname changes, voice/operator exemptions, uploads and restart persistence.
Tests define these oracles; their presence alone does not establish a live run.
[7](#source-7) [8](#source-8) [10](#source-10)

[FACT] Registered users can query up to five USERHOST nicknames using RFC1459
folding. Numeric 302 returns `nick=+account@airc`, omitting unknown/unregistered
users; no matches still yield an empty reply. Missing parameters return 461.
The fixed `+` matches the unset AWAY profile; channel roles never imply IRC
operator status. Tests cover these boundaries and case folding.
[11](#source-11) [12](#source-12) [13](#source-13)

[FACT] CAP 302 advertises `draft/multiline` with configured max-bytes and
max-lines. A BATCH or batch-tagged line from a registered client enters a
per-client assembler; other commands except PING/PONG abort an open batch.
Openers require negotiated batch+multiline, a 1–64 byte reference, the
draft/multiline type and one valid target; client `+` tags are kept. Lines must
match the command and folded target, carry only batch/concat tags, fit after the
relay prefix and stay within line/byte limits. Failures send one `FAIL BATCH`
and remember up to four discarded references whose later lines are ignored.
Echoed client parameters are replaced by `*` unless they are safe middle
parameters. Close rejects all-blank or expired batches, else relays through the
shared permission/storage path in batch context: any rejection there (tag budget,
target, membership, moderation, ban, DM policy) sends `FAIL BATCH
MULTILINE_INVALID` with the reason, including for NOTICE. One unconcatenated line
becomes an ordinary message. Ticks expire batches with no accepted line for 30 seconds.
[14](#source-14) [15](#source-15) [16](#source-16)

[FACT] Delivery frames depend on each recipient: batch+multiline clients get an
opening BATCH carrying all message tags, lines tagged with the batch and concat
flag, and a closing BATCH; others get non-blank lines with msgid/client tags on
the first and account/time on all. The concat tag is kept only for multiline
clients. History re-splits stored multiline text into relay-sized parts, prefers
breaks after spaces, nests batches under the chathistory batch, and trims to a
contiguous encoded-byte window grown from the end nearest the reference, or from
the AROUND anchor outward (`page_window`). Selection reads a seq/id/time index and
decodes only selected rows. Tests cover delivery, echo,
fallback, nested history, restart, malformed/overlapping/interrupted/unauthorized
batches, permission at close, byte-bounded paging and Tick timeout.
[16](#source-16) [17](#source-17) [18](#source-18) [19](#source-19)

## Sources

1. <a id="source-1"></a> [src/protocol.rs](../../src/protocol.rs#L7-L219) — `Message`, `fold`, name validation and `read_line`.
2. <a id="source-2"></a> [src/server/core.rs](../../src/server/core.rs#L336-L662) — `command`, `register`, `cap`, `sasl`.
3. <a id="source-3"></a> [src/server/core.rs](../../src/server/core.rs#L664-L791) — `join` and membership/operator checks; `mode`, `topic`, `kick` at L1270-L1499.
4. <a id="source-4"></a> [src/server/core.rs](../../src/server/core.rs#L792-L914) — `chat` and shared `relay`.
5. <a id="source-5"></a> [src/server/core.rs](../../src/server/core.rs#L241-L267) — `send_frames` capability filtering and queue failure.
6. <a id="source-6"></a> [src/server/core.rs](../../src/server/core.rs#L1582-L2028) — history dispatch, target selection, references and pagination.
7. <a id="source-7"></a> [tests/collaboration.rs](../../tests/collaboration.rs#L243-L355) — moderation/DM/ACL and restart/history integration oracles.
8. <a id="source-8"></a> [tests/collaboration.rs](../../tests/collaboration.rs#L536-L568) — authentication failures, CAP atomicity and malformed-frame handling.
9. <a id="source-9"></a> [src/server/ban.rs](../../src/server/ban.rs#L1-L96) — bounded mask normalization, wildcard matching and boundary tests.
10. <a id="source-10"></a> [tests/collaboration.rs](../../tests/collaboration.rs#L760-L942) — ban listing, authorization, enforcement, exemptions, artifact access and persistence.

11. <a id="source-11"></a> [src/server/core.rs](../../src/server/core.rs#L1545-L1560) — `userhost`.
12. <a id="source-12"></a> [tests/collaboration.rs](../../tests/collaboration.rs#L212-L240) — USERHOST identity, limits, unknown users and registration.
13. <a id="source-13"></a> [Modern IRC client protocol](https://modern.ircdocs.horse/#userhost-message) — USERHOST and numeric 302 wire semantics; checked 2026-10-13.
14. <a id="source-14"></a> [src/server/core.rs](../../src/server/core.rs#L336-L348) — `command` batch dispatch and interruption; `cap` value at L517-L590.
15. <a id="source-15"></a> [src/server/core.rs](../../src/server/core.rs#L991-L1268) — `fail_batch`, `discard`, `abort_batch`, `timeout_batch`, `client_batch`, `batch_line`, `close_batch`.
16. <a id="source-16"></a> [src/server/core.rs](../../src/server/core.rs#L809-L989) — `relay`, batch-context `deny` and `chat_frames`; `valid_middle`/`split_lines` at L1816-L1858.
17. <a id="source-17"></a> [src/server/core.rs](../../src/server/core.rs#L1582-L1680) — `history` rendering; `Trim`/`page_window`/`select_history` from L1899 and the window test near L2165.
18. <a id="source-18"></a> [tests/collaboration.rs](../../tests/collaboration.rs#L1032-L1399) — multiline delivery, fallback, history, restart, rejection and paging tests.
19. <a id="source-19"></a> [IRCv3 multiline draft](https://github.com/ircv3/ircv3-specifications/blob/0efc8474e5997a00d452e227c3db0423c9081b20/extensions/multiline.md) — batch, fallback and FAIL semantics; client-batch framing in `extensions/client-batch.md` at the same revision.
