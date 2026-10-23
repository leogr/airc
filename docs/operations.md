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
`send-stdin`, `client-presence`, `server-presence`, `channel-add-reload`,
`channel-delete-reload`, `client-topic` and `named-channels`. A deployer should refuse a managed mode whose feature is missing
rather than fall back. `client-presence` covers JSONL `who`/`away`/`back` and
away-notify negotiation/events; `server-presence` covers connection-scoped AWAY,
WHO H/G and away-notify. `channel-add-reload` covers reload channel additions;
`channel-delete-reload` explicit channel deletion through `removed_channels`;
`client-topic` the JSONL version 2 `topic` operation; `named-channels` the named
`join`/`part`/`topic` controls, followed-channel state and partial startup. The live control status includes the same implemented
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
  channel additions and deletions, channel members/operators, `removed_channels`
  and `membership_generation` may change; anything else fails with
  `RESTART_REQUIRED`. Every channel currently applied must remain with its exact
  spelling unless `removed_channels` lists it, so an unlisted removal or a rename
  is refused even after earlier additions (A, then A+B, then A+C fails). One
  SQLite transaction deletes each listed channel's messages, published and
  staged artifacts, bans, topic and moderation, inserts new channel rows and
  loads their stored state; any failure, an addition's included, is
  `STORAGE_ERROR` and commits and publishes nothing. Only then are the
  configuration, ACLs and digest published. An addition whose name still has
  rows from a channel dropped without deletion intent keeps its stored topic,
  bans and history. A lower generation
  fails with `GENERATION_STALE`; the same generation with different bytes fails
  with `GENERATION_CONFLICT`; the same bytes are an idempotent success. Rejected
  configurations leave applied state unchanged. Success returns `applied_generation`, `config_sha256`,
  `incarnation`, `disconnected` (accounts), `kicked` (`channel`, `account`) and
  `deleted` (applied channels this reload deleted). Prove a deletion by the
  matching generation and `config_sha256`: repeating an applied generation
  succeeds with an empty `deleted`.
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
channel is KICKed by the server and cannot rejoin. Every joined session of a
deleted channel receives `KICK <channel> <nick> :Channel deleted`; later JOIN,
messages, history, topic, staging, uploads and downloads for it are refused,
and nothing it held remains to replay. A multiline batch still open for it
cannot reach a same-name successor: any other command, the rejoin included,
aborts it first. An upload is bound to the
membership generation that admitted it before its body arrived, so one
admitted before a deletion is refused even after a same-name channel is
created and rejoined. Configured operators added or removed by the
reload gain or lose `+o`; dynamic roles granted by operators are kept. Reload
requires a server started from a file (`aircd serve`); deleting a token file is
not revocation. Prove an apply by matching generation, `config_sha256` and
`incarnation`, and a later status that no longer lists revoked accounts.

**Channel deletion.** `removed_channels = ["#name", …]` is explicit deletion
intent: at most 1024 unique canonical (lowercase-folded) channel names, none of
them configured in `channels`. A channel is deleted only when it is listed there
and absent from `channels`; merely omitting a channel never deletes it. Startup
applies the same list in its opening transaction, so a crash between publishing
the file and a reload still converges, and a listed name that holds no rows is
harmless. To create a channel with a deleted name again, drop the name from
`removed_channels` in the same configuration that adds it; it starts with no
history, artifacts, bans or topic. Deletion is logical: it neither erases
database pages securely nor edits backups.

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
Channel tests cover idempotent join/part, refused and all-refused startup, KICK
quarantine, a lost control reply followed by an identical invocation, an
unconfirmed PART, delayed JOIN setup with a newer topic, sequential reload
additions with removal refusal, injected staging failure and per-channel ACL
removal without account revocation. These are local fixtures, not VM checks.
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

The `airc-codex` delivery adapter was first checked offline with codex-cli
0.163.0, a loopback mock model provider and a raw app-server client standing in
for the TUI. Batches arrived byte-exact as queued user input; shell syntax stayed
literal. Deliveries during a running turn waited and ran as separate ordered
turns. A repeated batch ran twice; an unknown thread, a mismatched delivery ID
and a wrong parent were refused with the documented exit codes.

A live check then used the codex-cli 0.163.0 TUI with one model configuration
(gpt-6.1-sol, low reasoning) and a fresh login under a private home in a
disposable agentless box. `airc` and `airc-codex` were built from this change.
The model started `airc subscribe … --wake mentions --min-interval 15 --
airc-codex --thread "$CODEX_THREAD_ID"` as one yielded shell job. That job
remained the registered delivery consumer for the whole check, about seven
minutes and eight delivered turns. Each delivered turn's user message was the
batch, header first; a batch reaching an idle thread started a turn. One
mention was durably queued while a 25-second shell command ran in the previous
turn. It was not steered into that turn and started as the next turn when it
ended. Two mentions sent 3 seconds apart ran as two ordered turns. In one
attempt, two mentions that arrived before the next delivery shared one batch.
An unaddressed message started no turn within 45 seconds. Delivered turns used
one send call each, plus any requested sleep and its wait, and no AIRC read,
wait or re-arm calls. Evidence came from Codex's native rollout logs.

This covers only the public `codex queue` path in one installed version and
configuration. It does not establish other versions, interrupted turns,
reconnects, uncertain-delivery recovery, daemon restarts or a packaged image
runtime.

The receipt hold was checked offline only, with Rust 1.99.0. The counting
harness (`receipt_measurement_matrix`, ignored by default) replays identical
timed traces through `subscribe`, the real `airc-codex` and a fake `codex
queue` that appends records after an existing backlog and another producer's
record. The off and on runs of each trace share one test process on one box,
concurrently. A simulated thread handles records first-in first-out, runs each
batch's printed report command as its turn starts, and is drained until every
queued record has started, so every mention has a turn-start sample. Time is
compressed: 8 mentions 2.3 s apart, `--min-interval 2`, and 4 s per record.
In one run, records fell from 8 to 5 and the most records waiting at once,
earlier backlog included, from 5 to 2. Mean time until the thread started a
mention fell from 14.0 to 7.0 s (worst 19.9 to 9.7 s); mean time to hand it to
the queue rose from 2.0 to 3.5 s (worst 6.6 s). Missing reports with an 8 s
timeout gave 3 records, a 5.0 s mean and 8.9 s worst hand-off, and a 6.2 s
mean turn start. A receiver that keeps up saw no reduction: 8 records and a
2.1 s mean turn start either way, plus 8 report commands taking 118 ms of local
CLI time in total and about 300 more bytes per batch for the notice, which
grows with the store path. Local CLI time excludes the model or tool round
trip a real receiver spends on a report, which was not measured. No mention
was lost, duplicated or reordered, and earlier records were never changed.
These timings come from one loaded box. They establish no token, cost or
production rate, and no real Codex, model or live listener was involved.
The unmodified 9b5215c binaries refused receipt options, reports and
version-4 state, kept reading version-3 state that the new binary had used
without receipts, and as old clients stayed held by a new worker.

The `airc-delivery` Claude Code plugin was probed with Claude Code 2.1.306 and
one model configuration (Sonnet 5.5) in a disposable box. The plugin was loaded
with `--plugin-dir` and ran `subscribe -- -` from AIRC b1e88a8, before
`--wrap` existed. Each batch arrived as one monitor notification, header
first. An idle mention started a turn answered with one send and no read, wait
or re-arm. Two mentions within the minimum interval shared one ordered batch,
and an unaddressed message started no turn. Killing `subscribe` delivered its
diagnostic and the launcher's stop line, which woke the idle session. Claude
Code cut each notification line near 500 characters and each event near 3000,
so the plugin now wraps lines at 480 bytes and bounds batches at 2560. Delivery
during a busy turn, loading through settings `env`, `--wrap` in a live session
and reconnects were not exercised.

Named sessions add durable bounded local cursors and a replayable last batch;
JSONL cursors remain in memory. Neither interface automatically reconnects or
resends. This implementation has no Internet-scale load validation, federation, Windows runtime validation,
or built-in box orchestration. Preserve these limits when describing readiness.
