# AIRC repository guide

AIRC is a standalone Rust IRC collaboration server and agent client. Start at
[README.md](README.md), then the relevant contract in [docs](docs/architecture.md).
Implementation and tests are authoritative; update contracts when behavior changes.

- Preserve ordinary IRC client compatibility. Negotiate extensions through CAP;
  do not advertise functionality the server does not implement. Label drafts.
- Authentication accounts own access. Nicks are display names. Channel operators
  are not server administrators. Apply permissions to all message paths, history
  and artifacts. Never log credentials or put them in process arguments.
- Keep framing, queues, reads, waits, history and uploads bounded. A slow client
  must not stall other clients. Avoid cancellation of partially consumed frames.
- Persist before broadcasting durable events. Do not automatically retry sends
  or uploads after an ambiguous disconnect. Surface cursor/retention gaps.
- Human clients are existing open source clients; do not create a human TUI.
- SQLite is single-server state. Keep schema compatibility explicit and test
  restarts. Configuration paths are relative to the configuration file.
- Run `make check` before any requested commit. Validate IRC behavior changes against an existing
  unmodified human client when available; report unavailable checks explicitly.
  Use temporary local listeners/state, never the active development environment.
- Keep documentation, examples, skill and behavior in agreement. Test externally
  visible behavior, failure boundaries and permissions, not implementation shape.
- Use Conventional Commit messages. Do not add a Codex co-author trailer. Commit
  or push only when requested. Preserve unrelated changes.

The supported profile and current limitations are in
[protocol](docs/protocol.md), [agent API](docs/agent-api.md) and
[operations](docs/operations.md). Do not describe this as a general-purpose IRC
network daemon or finished deployment integrations.
