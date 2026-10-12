# Protocol and permissions

Verification: `c0bc1ed746f51ee2e5b3f415ca8f7f4d63c3408f` and accompanying changes; re-read cited symbols when behavior changes.

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
relaying. NOTICE denial is silent; TAGMSG requires negotiated tags. Client-only
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

## Sources

1. <a id="source-1"></a> [src/protocol.rs](../../src/protocol.rs#L7-L219) — `Message`, `fold`, name validation and `read_line`.
2. <a id="source-2"></a> [src/server/core.rs](../../src/server/core.rs#L265-L573) — `command`, `register`, `cap`, `sasl`.
3. <a id="source-3"></a> [src/server/core.rs](../../src/server/core.rs#L575-L1027) — `join`, membership/operator checks, `mode`, `topic`, `kick`.
4. <a id="source-4"></a> [src/server/core.rs](../../src/server/core.rs#L703-L796) — `chat`.
5. <a id="source-5"></a> [src/server/core.rs](../../src/server/core.rs#L190-L202) — `send` capability filtering and queue failure.
6. <a id="source-6"></a> [src/server/core.rs](../../src/server/core.rs#L1093-L1430) — history dispatch, target selection, references and pagination.
7. <a id="source-7"></a> [tests/collaboration.rs](../../tests/collaboration.rs#L212-L325) — moderation/DM/ACL and restart/history integration oracles.
8. <a id="source-8"></a> [tests/collaboration.rs](../../tests/collaboration.rs#L505-L537) — authentication failures, CAP atomicity and malformed-frame handling.
9. <a id="source-9"></a> [src/server/ban.rs](../../src/server/ban.rs#L1-L96) — bounded mask normalization, wildcard matching and boundary tests.
10. <a id="source-10"></a> [tests/collaboration.rs](../../tests/collaboration.rs#L729-L911) — ban listing, authorization, enforcement, exemptions, artifact access and persistence.
