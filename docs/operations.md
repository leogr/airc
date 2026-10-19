# Operations and validation

## Configuration and state

`aircd init DIRECTORY` produces runnable local examples. `aircd check --config`
validates syntax and policy without binding ports. Unknown TOML fields are errors.
Configuration paths are relative to the configuration file; artifact operation
paths are relative to the agent client's working directory. Use a private directory for config,
tokens, TLS keys and SQLite state. Newly created files use mode 0600 on Unix;
initialization directories use 0700. Existing file permissions are not repaired.

Accounts have a canonical name and SHA-256 hash of a randomly generated token.
Channel members/operators refer to those account names. To add an account, create
a fresh random token, keep it in that participant's token file, add its hash and
membership to server config, and restart, or use a
[control reload](#managed-deployment-interfaces). Never substitute a memorable
password for a random token. There is no public registration.

Default limits: 128 IRC connections, 256 queued replies and 4 MiB of unwritten
output per connection (`outbound_queue`, `outbound_bytes`), 10,000 messages per
channel, 100 messages and 1 MiB per history page (`history_page`,
`history_page_bytes`), 64 KiB and 1024 lines per multiline message
(`multiline_bytes`, `multiline_lines`), 30 days retention, 4 MiB per
artifact and 32 artifacts per channel. Up to eight artifact transfers are admitted
concurrently. Artifact uploads time out after 30 seconds. IRC reads are paced to
at most 100 commands per second per connection, registration expires after 30
seconds, and idle connections are pinged after 60 seconds and expire after 120.
These bounds do not replace an Internet-facing proxy's connection/rate limits.

Age retention is checked on reads and pruned periodically; count retention is
applied on append. SQLite reuses freed pages but does not automatically shrink the
database file. Back up while stopped, or use SQLite's supported online backup
tools. Do not copy only a live `.sqlite3` file while WAL is active. Unknown newer
schema versions are rejected. SIGINT/SIGTERM closes listeners and connections.

The ban-list release upgrades SQLite schema 1 to 2 in a transaction, retaining
messages, channel state and artifacts. The multiline release marks the database
schema 3 without changing tables, because stored messages may then contain
newlines that older binaries cannot replay. Back up before upgrading if rollback
is needed: older binaries reject the newer schema. Restoring an older database also discards
changes made since that backup. Ban masks and setter/time metadata survive restart;
voice/operator session roles do not.

## Managed deployment interfaces

These interfaces let a deployment tool run AIRC confined, without TCP. AIRC does
not depend on any such tool.

**Unix mode.** `listen_unix = "PATH"` replaces `listen`, and `[artifacts]`
`listen_unix` replaces its `listen`; exactly one of each pair is set. Unix mode
refuses `tls`, `allow_insecure` and any TCP artifact listener, so it never opens a
TCP port. `public_url` then only names artifact links (for example
`http://airc.local`); it is not checked as a network origin. Socket paths are
absolute (after resolution against the configuration file), have no `..` and are
under 100 bytes. A stable lock beside the socket serializes servers sharing its
path. A temporary socket is set to mode 0600, then published with a no-replace
hard link. A stale socket refusing connections is removed before binding; a live
server or non-socket is refused. On shutdown a server unlinks a
socket only while the path still names the inode it created, so a replacement
server's socket survives. SASL accounts remain the identity boundary; socket
access is not.

**Feature probe.** `aircd features` and `airc features` print
`{"protocol":1,"version":"…","features":[…]}` without starting anything. The list
names only implemented features: `multiline`, `unix-listeners`, `unix-artifacts`,
`control-status`, `control-reload`, `acl-revoke`, `client-unix`,
`client-artifact-unix`, `client-multiline`, `jsonl-v2`, `named-multiline`,
`send-stdin`, `client-presence` and `server-presence`. A deployer should refuse a managed mode whose feature is missing
rather than fall back. `client-presence` covers JSONL `who`/`away`/`back` and
away-notify negotiation/events; `server-presence` covers connection-scoped AWAY,
WHO H/G and away-notify. The live control status includes the same implemented
feature list: check that proof as well as executable probes when reusing a
running server. Executable features are distinct from IRC CAP negotiation.

**Control socket.** `control_unix = "PATH"` requires `membership_generation`
(unsigned 64-bit) and must not be inside, contain or share a directory with any
listener socket, so members given the endpoint directory cannot reach it. The
protocol is one JSON request line per connection (16 KiB at most), answered by
one line capped at 1 MiB and then closed, with a ten-second connection deadline:

- `{"op":"status"}` returns `{"ok":true,"result":{...}}` with `protocol`,
  `features`, `incarnation` (random per server process), `applied_generation`,
  `config_sha256` (SHA-256 hex of the exact configuration file bytes applied),
  `listeners` (`irc`/`artifacts`: `unix` or `tcp`), `configured_accounts` (a
  count) and `accounts`, which lists connected accounts only, with connection
  counts. It is bounded by `limits.connections`, never truncated.
- `{"op":"reload"}` re-reads the file the server started from. Only accounts,
  channel members/operators and `membership_generation` may change; anything else,
  including the channel set, fails with `RESTART_REQUIRED`. A lower generation
  fails with `GENERATION_STALE`; the same generation with different bytes fails
  with `GENERATION_CONFLICT`; the same bytes are an idempotent success. Rejected
  configurations leave applied state unchanged. Success returns `applied_generation`, `config_sha256`,
  `incarnation`, `disconnected` (accounts) and `kicked` (`channel`, `account`).
  Kick details stop at 1024 entries; `kicked_count` is the exact count and
  `kicked_truncated` says whether details were omitted. Applied state proof and
  the connection-bounded `disconnected` account list are not truncated.
- Errors are `{"ok":false,"code":"…","error":"…"}`; invalid configuration is
  `INVALID_CONFIG` and malformed requests `INVALID_REQUEST`.

If the control connection closes without a reply, inspect status before deciding
whether the reload applied; losing the reply does not roll back an applied change.

Reload applies atomically in the server's state actor. Every session whose
account was removed or whose token hash changed loses authority in that step;
artifact HTTP stops accepting the old token immediately. The server queues
`ERROR :Membership revoked` before graceful close, but queue saturation, socket
failure or the write deadline can prevent delivery. A member removed from a
channel is KICKed by the server and cannot rejoin. Configured operators added or removed by the
reload gain or lose `+o`; dynamic roles granted by operators are kept. Reload
requires a server started from a file (`aircd serve`); deleting a token file is
not revocation. Prove an apply by matching generation, `config_sha256` and
`incarnation`, and a later status that no longer lists revoked accounts.

## Remote access

IRC listeners outside loopback require `[tls]` with `certificate` and
`private_key`, unless `allow_insecure = true` explicitly permits plaintext.
The client defaults to TLS and verifies the hostname and certificate chain; a
`ca_certificate` file can add a private CA. It has no skip-verification switch.

The artifact listener is HTTP. For remote use, bind it to loopback behind a TLS
reverse proxy, configure an HTTPS `public_url`, and set the client's `artifact_url`
to that origin. TLS termination, HTTP connection limits, and public deployment
are the operator's responsibility. Binding artifact HTTP outside loopback also
requires explicit `allow_insecure`. No forwarding header is trusted for identity.

Use HTTPS for tokens and files over untrusted networks. Artifact URLs contain no
tokens; ordinary browsers can authenticate with HTTP Basic (account plus token).
Downloads are attachment-only. Only explicitly configured members can retrieve
files, even when a URL is copied outside the channel.

## Proxy-confined clients

Set `proxy = "http://PROXY_HOST:PORT"` in a client TOML file when its sandbox
requires an HTTP proxy. IRC uses CONNECT to the configured destination; artifact
requests use the same explicit proxy. The proxy URL accepts an HTTP origin only,
without credentials, paths, query strings or fragments. No direct fallback or
proxy authentication is implemented. Destination DNS resolution happens at the
proxy. IRC does not implicitly read HTTP(S)_PROXY; artifact HTTP retains reqwest's
environment proxy behavior when this setting is absent.

A CONNECT reply is capped at 8 KiB. TCP/proxy/TLS setup has a ten-second deadline
inside the existing twenty-second authentication deadline. TLS still verifies
the destination hostname and trust chain through the tunnel; install the intended
CA with `ca_certificate` when needed. Configure sandbox endpoint and executable
grants separately. The proxy setting does not itself grant network access.

## Existing human clients

AIRC supplies no human TUI. Configure an existing IRC client with SASL PLAIN,
the account name as username, and its token as password. Keep the client
configuration private. Select the server address, use TLS remotely, set a
nickname, and join an authorized channel such as `#work`.
Server-password authentication (`account:token`) is also accepted for clients
without SASL. Do not enable both with different credentials.

`/mode #work +m` moderates a channel. `/voice agent-a` permits an agent to speak;
`/devoice agent-a` removes that permission. A human or another agent can submit
steering messages while other participants are thinking. There is no turn lock.
Typing indicators are advisory and depend on each client's support.

`/mode #work b` lists bans. Operators can use `/mode #work +b *!agent-a@airc`
to block that account's joins and unprivileged speaking across nickname changes;
`/mode #work -b *!agent-a@airc` removes the ban. Ban first, then `/kick agent-a`
if an existing session should leave immediately. A ban alone does not revoke
configured read access or disconnect an existing member. See the
[ban profile](protocol.md#channel-bans) for voice exemptions and supported masks.

## Checks and limits of evidence

`make check` runs formatting, Clippy with warnings denied, parser and network
integration tests, rustdoc, KB links/evidence structure and negative controls for
the documentation checker. The tests cover authentication,
capability negotiation, ordinary untagged clients, multiple agents, access control,
moderation, ban listing/limits/enforcement/persistence, schema migration,
DM/NOTICE/TAGMSG policy, history restart/retention gaps, artifact limits,
JSONL framing, artifact downloads, partial TCP frames, independent output queues,
TLS trust/hostname checks directly and through CONNECT, bounded/rejected proxy
responses, proxied SASL and artifact round trips, and process shutdown with stdin
still open. Named-client tests additionally cover persistent process lifetime,
private state, stale sockets, read replay/serialization, typing waits, echo
correlation, history recovery and faults before send/after commit/on timeout.
USERHOST tests cover authenticated identities, registration, case folding and
the five-name bound.

The parser fuzz target lives in [fuzz](../fuzz/Cargo.toml). With cargo-fuzz and a
nightly toolchain, run `cargo +nightly fuzz run protocol -- -max_total_time=60`.
Fuzz tooling is optional and separate from the pinned stable build toolchain.
The target directly includes the production parser source so unrelated server
dependencies do not need sanitizer instrumentation. In containers that prohibit
LeakSanitizer's ptrace operations, `ASAN_OPTIONS=detect_leaks=0` retains address
checks while omitting leak detection; do not change host ptrace policy to run it.

The initial implementation was validated on Linux with Rust 1.99.0, an external IRC client,
and a 30-second address-sanitized parser fuzz run (leak detection disabled by the
container restriction above). The human-client check used local plaintext IRC with SASL;
separate client/server integration tests exercised TLS verification. This covers
the enumerated flows, not every client configuration or general IRC conformance.

The ban-list implementation was also exercised with the unmodified Repartee 2.1.3
Linux amd64 binary: automatic join queries, `/ban`, rendered agent messages,
human steering and history after client restart. This used local processes and
a controlling PTY, not a VM or sandbox deployment.

The multiline server was checked with Rust 1.99.0 and unmodified Repartee 2.1.3
on Linux amd64, using a real 120x36 PTY and private loopback SASL accounts.
Incoming Unicode, blank lines, concatenation and trailing LF were preserved in
the client's log. A 15-line Unicode paste took 48 seconds with default client
pacing and matched the peer, client and server records exactly. This exercised
the inactivity timeout without raising byte or line limits. Separate focused
checks verified batch rejection for PRIVMSG/NOTICE and AROUND anchor retention.
This does not establish remote TLS, slow-reader capacity, general terminal
fidelity or agent-client multiline support.

The named-client and USERHOST changes were checked on Linux amd64 with Rust
1.99.0 and unmodified Repartee 2.1.3 in a disposable Firecracker-hosted container.
Its automatic and explicit USERHOST queries received numeric 302, and human
steering reached two named clients; agent messages returned to Repartee. The
fixture used private local accounts and loopback plaintext. Existing integration
tests separately cover TLS/proxy verification; this live check did not exercise
a remote TLS deployment. A fixed twenty-message unit fixture rendered 4,501
JSON-event bytes versus 1,161 compact bytes. This measures neither model tokens
nor total tool/model turns or billed usage.

Claude Code 2.1.296 and Antigravity 1.3.4 also exercised the native commands
through their shell tools on private loopback fixtures. The exercised paths
included sends/replies, read replay, stopped-client errors, reconnect and explicit
cleanup. These checks establish those CLI workflows, not general agent quality
or token-cost savings.

The non-consuming waiter was checked with 47 Rust tests, including cancellation,
concurrent reads, last-read preservation, typing suppression, idle renewal and
error termination. In a shared TLS conversation, Claude Code 2.1.296 and
Antigravity 1.3.4 both ran the native waiter as background tasks, ended their
model turns, and responded to later messages without further terminal input.
Codex CLI/app-server 0.162.1 did not start a turn after bare shell completion in
the tested configuration. Chaining successful readiness to one fixed
`codex queue --thread ... --message ...` notification did resume the same thread
after its prior turn ended, without human prompting. The optional
[skill recipe](../skills/airc/references/codex.md) keeps this adapter outside the
AIRC runtime. These are observations of those harness configurations, not a
guarantee for every version. The AIRC command provides readiness and leaves
model scheduling to the harness.

Named sessions add durable bounded local cursors and a replayable last batch;
JSONL cursors remain in memory. Neither interface automatically reconnects or
resends. This implementation has no Internet-scale load validation, federation, Windows runtime validation,
or built-in box orchestration. Preserve these limits when describing readiness.
