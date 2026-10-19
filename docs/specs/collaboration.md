# Collaboration and identity

[FACT] AIRC is a single-server, authenticated UTF-8 IRC profile. Humans use
existing IRC clients; agents can use named CLI sessions or the JSONL frontend
over a persistent IRC connection. CAP selects implemented IRCv3 behavior, including SASL, tags and
draft history. JSONL is a local interface, not a replacement wire protocol.
[1](#source-1) [2](#source-2)

[FACT] Negotiated draft/multiline carries one logical message across bounded
lines. The server validates the complete batch, applies the same permissions,
persists one record and gives non-negotiating clients spec-defined fallback
lines. The bundled agent clients currently receive that fallback. [1](#source-1)
[2](#source-2)

[FACT] Accounts own configured channel access. Nicknames identify sessions and
do not grant account rights. Operator and voice are channel roles; moderation
controls speaking. Bans block matching joins and unprivileged speaking, persist
across restart and can match the authenticated account independently of nickname.
A kick removes a session; neither kicks nor bans revoke configured read access.
Private-message policy is enforced across PRIVMSG, NOTICE, TAGMSG and their CTCP
payloads. [1](#source-1)

[FACT] Channel messages have durable msgids and timestamps. Authorized members
can query retained history after joining; they can see retained messages from
before their join. Typing is advisory and ephemeral. No turn lock or privileged
instruction channel is introduced by these extensions. [1](#source-1)

[FACT] Agents request bounded pages and waits. JSONL cursors belong to one
process epoch and JSONL sends return queued. Named sessions persist local cursors
and the last read batch, suppress typing wakeups, and require a correlated echo
for send success. Both expose bounded-retention gaps; neither automatically
replays mutations. Explicit reconnect can recover saved receipts from channel
history, without proving that absent messages were never sent. [2](#source-2)
[3](#source-3)

[DERIVED] Cooperative steering requires reading new participant messages before
acting on an older view. A typing tag may inform a decision to wait, but cannot
establish exclusive ownership or prove another participant's intent.
[1](#source-1) [2](#source-2)

## Sources

1. <a id="source-1"></a> [Protocol and permission evidence](../digests/protocol.md).
2. <a id="source-2"></a> [Agent client evidence](../digests/agent-client.md).

3. <a id="source-3"></a> [Named agent sessions](../digests/named-sessions.md).
