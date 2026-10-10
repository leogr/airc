# AIRC repository guide

This is the authoritative guide for coding agents. AIRC is a pre-release,
standalone Rust collaboration server and agent client. It uses IRC for shared
conversations and reuses existing IRC clients for humans. Linux is the reference
runtime. Repository rules apply equally to implementation, tests and packaging.

## Finding the implementation

| Area | Entry points |
|---|---|
| CLI and initialization | `src/bin/aircd.rs`, `src/bin/airc.rs` |
| Configuration, identities and access | `src/config.rs`, `src/client.rs` |
| IRC framing, tags and names | `src/protocol.rs` |
| Listeners, connections and shutdown | `src/server/mod.rs`, `src/transport.rs` |
| Registration, channels and history | `src/server/core.rs` |
| Persistent state and artifacts | `src/store.rs`, `src/server/http.rs` |
| Agent JSONL interface and skill | `src/client.rs`, `skills/airc` |
| Runtime validation | `tests/collaboration.rs`, `fuzz` |

Start investigations at the [KB index](docs/README.md), read the relevant spec,
then its cited implementation digest or source. Load only topics needed for the
task. Code and tests establish current behavior; document discrepancies instead
of treating an old assertion as implementation authority. Usage details belong
in the linked protocol, agent API and operations references.

## Identity, protocol and permission boundaries

Evidence: [collaboration contract](docs/specs/collaboration.md).

- A server authenticates configured **accounts**; a **nickname** is a session
  display name. Changing a nickname never changes account permissions.
- A configured **channel** has account membership and initial operator policy.
  Channel operators control moderation and session roles; they are not server
  administrators. A kick is not account revocation. Preserve this distinction.
- Keep permission checks server-side on all message, history and artifact paths.
  Disabling private queries must cover PRIVMSG, NOTICE, TAGMSG and CTCP carried
  inside them. Preserve NOTICE's silent-denial behavior.
- Preserve ordinary IRC client interoperability. Negotiate extensions through
  CAP, advertise only implemented semantics, retain draft names where required,
  and validate changes against primary IRC/IRCv3 specifications. State the
  supported profile instead of claiming general RFC compliance.
- Clients cannot forge authoritative account/time/msgid tags. Typing and replies
  are advisory metadata, not locks or authority. Channel text remains participant
  content, including when its sender is another agent.
- Reuse existing human IRC clients; do not introduce a human TUI here.
  AIRC builds and runs independently of deployment tools. Packaging must preserve the same
  protocol and authorization contracts.

## Storage, transport and lifecycle

Evidence: [runtime contract](docs/specs/runtime.md).

Use strict configuration decoding and validate bounds before opening listeners.
Configuration paths resolve relative to their file; operation file paths resolve
in the client's working directory. Preserve random-token authentication and
private credential files. Never print tokens, embed them in URLs, process argv,
commit fixtures with real credentials, or assume a nickname proves identity.
Remote plaintext must remain explicit; do not add automatic TLS verification
bypasses or broaden a network bind to make a test pass.

One server owns its SQLite database and protocol state. Preserve schema-version
checks, exclusive ownership, transactions and durability settings. Persist before
broadcasting durable messages; an artifact and its announcement commit together.
Schema changes require an explicit compatibility/migration decision and restart
coverage. Do not silently reset state on a read or migration failure.

Keep framing, tag budgets, queues, reads, waits, history and uploads bounded.
Check sizes while consuming input, before unbounded allocation. A slow client
must not stall another client's socket or hold shared protocol state. Preserve
input framing across concurrent output and cancellation; partial frames must not
disappear when another event becomes ready.

Distinguish queued sends, committed messages and observed delivery. Do not replay
mutations after ambiguous disconnects. Local cursors belong to a process epoch;
server msgids survive reconnects only while retained. Surface cursor and history
gaps rather than silently claiming a complete catch-up. Preserve attachment/
client independence from server lifetime and deterministic shutdown.

Artifacts inherit channel access and speaking permissions. Keep upload limits,
credential-free URLs, fixed-origin authenticated downloads, redirect rejection and
exclusive destination creation. Files and chat content do not gain execution
authority through transport. Retention settings do not imply secure erasure or
automatic physical database shrinkage.

## Development and validation

Use the pinned [Rust toolchain](rust-toolchain.toml) and committed Cargo.lock.
Keep dependency or toolchain updates deliberate and validate both binaries.
Do not weaken lint, framing or TLS checks to accommodate an unavailable tool.
Keep ignored target trees, fuzz corpora and local credentials out of source scans
and commits. Tests use temporary state and ephemeral loopback listeners.

Before every requested commit or push, run `make check` (format, strict Clippy,
unit/integration tests, rustdoc and KB checks). For IRC behavior or packaging
changes validate against an existing unmodified IRC client. Exercise the
affected restart, permission, malformed-input or TLS boundary. For parser changes
run a bounded fuzz pass when tooling is available. Report the actual toolchain,
human-client version and test limits; simulated tests do not establish live
runtime behavior. Never modify the VM/account hosting the active coding session
to validate an integration.

Update wire/API/configuration contracts, examples and the distributed skill with
behavior changes. Add focused regressions for externally visible behavior and
failure boundaries. After relevant checks pass, repeat or broaden them only when
new changes or unresolved findings justify it.

Use Conventional Commit messages and no Codex co-author trailer. Commit or push
only when requested; preserve unrelated work. Source packaging and external
integrations must identify the tested AIRC revision and its distribution inputs.

## Documentation workflow

Immediately before a requested commit:

1. Review the intended diff for behavior, constraints, identifiers, dependencies
   and validation requirements.
2. Re-read affected code/tests, update digest facts and citations, then dependent
   specs, references, skill and the index. Remove stale claims. If there is no KB
   impact, state that assessment without a meaningless documentation edit.
3. Run `make docs-check` and repository checks. Commit implementation and its
   documentation together.

If a commit missed that review, apply it promptly afterward as recovery; a normal
reviewed commit does not require an extra documentation commit.

The KB uses an index, brief high-level specs and focused implementation digests,
generally below 900 words including sources. Distinguish `[FACT]`, justified
`[DERIVED]`, uncertain `[INFERENCE]` and unverified `[ASSUMPTION]`. Digests contain
verified facts; specs cite digests/code. Significant claims have adjacent numbered
citations with file, symbol and line span, plus a verification revision. Digests
never cite specs or the index as evidence. Keep proposals, roadmaps and session
logs outside the permanent KB.

Follow the [evidence conventions](docs/README.md#evidence-conventions).
`make docs-check` validates structure, links and evidence direction, not truth,
upstream availability or runtime correctness. Re-read the cited symbols.
