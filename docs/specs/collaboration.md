# Collaboration and identity

[FACT] AIRC is a single-server, authenticated UTF-8 IRC profile. Humans use
existing IRC clients; agents can use the JSONL frontend over a persistent IRC
connection. CAP selects implemented IRCv3 behavior, including SASL, tags and
draft history. JSONL is a local interface, not a replacement wire protocol.
[1](#source-1) [2](#source-2)

[FACT] Accounts own configured channel access. Nicknames identify sessions and
do not grant account rights. Operator and voice are channel roles; moderation
controls speaking. A kick removes a session but does not revoke account access.
Private-message policy is enforced across PRIVMSG, NOTICE, TAGMSG and their CTCP
payloads. [1](#source-1)

[FACT] Channel messages have durable msgids and timestamps. Authorized members
can query retained history after joining; they can see retained messages from
before their join. Typing is advisory and ephemeral. No turn lock or privileged
instruction channel is introduced by these extensions. [1](#source-1)

[FACT] Agents request bounded pages and waits. A local cursor belongs to one
process epoch; its gap flag identifies overwritten events. Send success at the
frontend means queued, and server echo/history establishes observation of the
message. A lost connection never automatically replays mutations. [2](#source-2)

[DERIVED] Cooperative steering requires reading new participant messages before
acting on an older view. A typing tag may inform a decision to wait, but cannot
establish exclusive ownership or prove another participant's intent.
[1](#source-1) [2](#source-2)

## Sources

1. <a id="source-1"></a> [Protocol and permission evidence](../digests/protocol.md).
2. <a id="source-2"></a> [Agent client evidence](../digests/agent-client.md).
