# Runtime, persistence and confinement

[FACT] One server state actor owns protocol state and SQLite access. Network
tasks use bounded channels; each client has an independent output queue and
write deadline. A full queue disconnects that client. Partial input frames retain
their read future across concurrent output. [1](#source-1)

[FACT] SQLite schema 3 stores channel messages, topics/moderation, bans and artifacts;
older schemas upgrade transactionally while retaining existing data. Outbound
queues and history pages are bounded by encoded bytes as well as counts.
Exclusive database ownership and FULL-synchronized transactions precede durable
message broadcasts. An artifact and its announcement commit together. Dynamic
roles and live connections are ephemeral. Age/count limits bound retained data;
a missing msgid produces an explicit history gap. [1](#source-1) [2](#source-2)

[FACT] A confined deployment can run Unix-only: IRC and artifact sockets with
no TCP listener, and a separate control socket outside every listener
directory. Control status and reload bind applied membership to a generation,
the exact configuration digest and a per-process incarnation; reload revokes
removed or rotated identities in the same actor step. [5](#source-5)

[FACT] Configuration declares accounts and channel access. Random tokens are
hashed at the server and read from private client files. Remote IRC plaintext
requires explicit opt-in; TLS clients verify both trust and hostname. Artifact
HTTP needs loopback or explicit insecure binding, with HTTPS reverse-proxy
deployment documented for remote use. [1](#source-1) [3](#source-3)

[FACT] Clients can explicitly use an HTTP proxy for IRC CONNECT and artifact
requests. Rejected tunnels do not fall back to direct connections; destination
TLS verification and endpoint authorization remain independent. [3](#source-3)

[FACT] Uploading requires channel membership and speaking permission. Download
access follows current configured membership. URLs contain no credentials;
client downloads use the configured origin, reject redirects and create a new
destination exclusively. [1](#source-1) [3](#source-3)

[DERIVED] A successful queued request, durable database commit, live delivery
and complete retained catch-up establish different things. Recovery must inspect
server history and surface missing context before choosing whether to resend.
[1](#source-1) [2](#source-2) [3](#source-3)

[FACT] Named clients are explicitly started local processes with private,
versioned IPC and native owner locks. Bounded state is atomically synced before
read-cursor or send-preparation replies. Stopped clients require explicit
reconnect; saved uncertainty is not replay authority. [4](#source-4)

[FACT] Named readiness waits do not consume events or replace read replay.
They renew bounded idle requests and stop on errors/disconnects. Agent turn
scheduling remains the harness's responsibility. [4](#source-4)

## Sources

1. <a id="source-1"></a> [Storage, transport and lifecycle evidence](../digests/storage.md).
2. <a id="source-2"></a> [History and protocol evidence](../digests/protocol.md).
3. <a id="source-3"></a> [Client and artifact evidence](../digests/agent-client.md).

4. <a id="source-4"></a> [Named session persistence](../digests/named-sessions.md).
5. <a id="source-5"></a> [Managed transport evidence](../digests/managed-transport.md).
