# Storage, transport and server lifecycle

Verification: `c0bc1ed746f51ee2e5b3f415ca8f7f4d63c3408f` and accompanying changes; re-read cited symbols when behavior changes.

[FACT] Strict TOML configuration validates identifiers, account/channel references,
resource limits and remote plaintext opt-ins before listeners start. Relative
database/TLS paths resolve against the configuration directory. Authentication
compares a supplied random token's hash; channel access uses the authenticated
account. Initialization creates private files without overwriting existing paths.
[1](#source-1) [2](#source-2) [11](#source-11)

[FACT] The server owns one blocking state actor and SQLite connection. Socket
sessions use bounded input/output channels, retain partial read futures across
concurrent output, and apply write deadlines. Shutdown closes clients and joins
the actor. A full client output queue sets that client's cancellation signal.
[2](#source-2) [3](#source-3)

[FACT] SQLite refuses a newer schema, establishes exclusive locking and WAL with
FULL synchronization, and transactionally upgrades schema 1 to 2 by adding the
ban table; existing messages, channels and artifacts remain. Older binaries
reject schema 2. Message append assigns UUID
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

[FACT] Integration tests exercise unauthorized artifact access, upload limits,
moderation, restart persistence and TLS trust/hostname rejection. A state-actor
test verifies a full output queue cancels the slow client while a draining peer
continues receiving. [8](#source-8) [9](#source-9) [10](#source-10)

## Sources

1. <a id="source-1"></a> [src/config.rs](../../src/config.rs#L12-L262) — strict config types, `Config::load`, `validate`, authentication and private writes.
2. <a id="source-2"></a> [src/server/mod.rs](../../src/server/mod.rs#L43-L206) — `RunningServer`, `start`, `session`.
3. <a id="source-3"></a> [src/server/core.rs](../../src/server/core.rs#L27-L202) — session roles, actor handling, timeouts, pruning and output queues.
4. <a id="source-4"></a> [src/store.rs](../../src/store.rs#L36-L203) — schema ownership, append transactions, selection and pruning.
5. <a id="source-5"></a> [src/server/core.rs](../../src/server/core.rs#L1245-L1308) — `artifact` checks, persistence and broadcast.
6. <a id="source-6"></a> [src/transport.rs](../../src/transport.rs#L11-L85) — TLS acceptor and verified connection.
7. <a id="source-7"></a> [src/server/http.rs](../../src/server/http.rs#L25-L175) — routes, authentication, transfer limits and response headers.
8. <a id="source-8"></a> [tests/collaboration.rs](../../tests/collaboration.rs#L326-L392) — artifact ACL, moderation and size tests.
9. <a id="source-9"></a> [tests/collaboration.rs](../../tests/collaboration.rs#L593-L681) — persistent topic/moderation and TLS rejection tests.
10. <a id="source-10"></a> [src/server/core.rs](../../src/server/core.rs#L1432-L1488) — `slow_output_disconnects_only_that_client`.
11. <a id="source-11"></a> [src/bin/aircd.rs](../../src/bin/aircd.rs#L59-L129) — `init` private directory, credentials and configuration creation.
12. <a id="source-12"></a> [tests/collaboration.rs](../../tests/collaboration.rs#L914-L988) — schema-1 migration retains history, topic/moderation and artifacts; newer schema refusal preserves data.
13. <a id="source-13"></a> [src/server/core.rs](../../src/server/core.rs#L65-L91) — persisted list validation.
14. <a id="source-14"></a> [src/server/core.rs](../../src/server/core.rs#L829-L948) — ban queries and persist-before-broadcast mutation.
