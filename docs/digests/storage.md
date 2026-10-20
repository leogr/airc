# Storage, transport and server lifecycle

Verification: `59064c6188658bf60131db6afc33ab7bbf45357c` and accompanying changes; re-read cited symbols when behavior changes.

[FACT] Strict TOML configuration validates identifiers, account/channel references,
resource limits and remote plaintext opt-ins before listeners start. Relative
database/TLS paths resolve against the configuration directory. Authentication
compares a supplied random token's hash; channel access uses the authenticated
account. Initialization creates private files without overwriting existing paths.
[1](#source-1) [2](#source-2) [11](#source-11)

[FACT] The server owns one blocking state actor and SQLite connection. Socket
sessions use bounded input/output channels, retain partial read futures across
concurrent output, and apply write deadlines. Shutdown closes clients and joins
the actor. The actor queues pre-encoded frame groups, so a multiline batch is
written contiguously, and counts unwritten bytes per connection. A full queue or
exceeding `outbound_bytes` sets that client's cancellation signal. Validation
requires the byte budget to hold two worst-case multiline relays plus one history
page. [2](#source-2) [3](#source-3) [1](#source-1)

[FACT] SQLite refuses a newer schema, establishes exclusive locking and WAL with
FULL synchronization, and transactionally upgrades schema 1 to 2 by adding the
ban table; existing messages, channels and artifacts remain. Schema 3 adds no
table change: it marks that rows may hold multiline text, so older binaries
refuse the database. CHATHISTORY selects from a seq/id/time index of the
retained channel and decodes only the selected page's message JSON; the index
scan is still linear in retained rows. Message append assigns UUID
and timestamp, prunes channel counts/age, and commits before the caller broadcasts.
Artifact bytes and the associated message share that transaction. Reads filter
age retention; periodic pruning removes expired rows. Physical shrinking is not
performed. Channel moderation/topic and bans persist; session roles do not.
Ban writes commit before state changes and broadcast; startup rejects malformed
or oversized persisted lists. [4](#source-4) [5](#source-5) [12](#source-12) [13](#source-13) [14](#source-14)

[FACT] IRC supports server TLS and certificate-validated client TLS with an
optional additional CA. Artifact HTTP authenticates Basic/Bearer tokens, bounds
uploads and concurrent transfers, and delegates permissions to the actor. Upload
requires a joined speaking session, including ban/voice checks; download requires configured channel access.
Attachment responses set no-store and nosniff. [6](#source-6) [7](#source-7) [5](#source-5)

[FACT] Artifact announcements store server-owned `airc-artifact` (UUID),
`airc-artifact-name`, `airc-artifact-bytes` and `airc-artifact-sha256` tags in
that same transaction. They are delivered to message-tags clients and survive
history/restart without a schema change. Normal message input and multiline
openers strip non-client tags; tagged multiline data frames are rejected.
Client-only lookalikes never become reserved metadata. The body remains readable
for older clients. Artifact metadata does not verify media type. [5](#source-5)
[15](#source-15)

[FACT] Integration tests exercise unauthorized artifact access, upload limits,
moderation, restart persistence and TLS trust/hostname rejection. A state-actor
test verifies a full output queue cancels the slow client while a draining peer
continues receiving. [8](#source-8) [9](#source-9) [10](#source-10)

## Sources

1. <a id="source-1"></a> [src/config.rs](../../src/config.rs#L133-L332) — `load_with_digest` and `validate`; authentication/access at L334-L349, token generation and private writes at L365-L394.
2. <a id="source-2"></a> [src/server/mod.rs](../../src/server/mod.rs#L219-L369) — `start`/`start_inner`; `RunningServer` at L66-L95 and `session` at L389-L451.
3. <a id="source-3"></a> [src/server/core.rs](../../src/server/core.rs#L47-L539) — session roles, batch state, actor handling, timeouts, pruning and byte-counted output queues.
4. <a id="source-4"></a> [src/store.rs](../../src/store.rs#L37-L232) — schema ownership, append transactions, selection and pruning.
5. <a id="source-5"></a> [src/server/core.rs](../../src/server/core.rs#L2116-L2187) — `artifact` checks, persistence and broadcast.
6. <a id="source-6"></a> [src/transport.rs](../../src/transport.rs#L11-L107) — TLS acceptor and verified connection.
7. <a id="source-7"></a> [src/server/http.rs](../../src/server/http.rs#L25-L186) — routes, authentication, transfer limits and response headers.
8. <a id="source-8"></a> [tests/collaboration.rs](../../tests/collaboration.rs#L374-L457) — artifact ACL, moderation and size tests.
9. <a id="source-9"></a> [tests/collaboration.rs](../../tests/collaboration.rs#L658-L746) — persistent topic/moderation and TLS rejection tests.
10. <a id="source-10"></a> [src/server/core.rs](../../src/server/core.rs#L2416-L2473) — `slow_output_disconnects_only_that_client`.
11. <a id="source-11"></a> [src/bin/aircd.rs](../../src/bin/aircd.rs#L69-L145) — `init` private directory, credentials and configuration creation.
12. <a id="source-12"></a> [tests/collaboration.rs](../../tests/collaboration.rs#L979-L1058) — schema-1 migration retains history, topic/moderation and artifacts; newer schema refusal preserves data.
13. <a id="source-13"></a> [src/server/core.rs](../../src/server/core.rs#L84-L139) — persisted list validation.
14. <a id="source-14"></a> [src/server/core.rs](../../src/server/core.rs#L1589-L1739) — ban queries and persist-before-broadcast mutation.

15. <a id="source-15"></a> [src/server/core.rs](../../src/server/core.rs#L541-L558) — outbound capability filtering; `relay` at L1132–1147, `client_batch` at L1442–1443, `batch_line` at L1498–1502 strip/reject forged metadata; `tests/collaboration.rs:374–457` — atomic announcement fields and restart history, anti-forgery at L1629–1705.
