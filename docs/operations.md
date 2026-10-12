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
membership to server config, and restart. Never substitute a memorable password
for a random token. There is no public registration or live config reload.

Default limits: 128 IRC connections, 256 queued replies per connection, 10,000
messages per channel, 100 messages per history page, 30 days retention, 4 MiB per
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
messages, channel state and artifacts. Back up before upgrading if rollback is
needed: older binaries reject schema 2. Restoring an older database also discards
changes made since that backup. Ban masks and setter/time metadata survive restart;
voice/operator session roles do not.

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
still open.

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

This initial implementation has no automatic client reconnection, durable local
cursors, Internet-scale load validation, federation, Windows runtime validation,
or built-in box orchestration. Preserve these limits when describing readiness.
