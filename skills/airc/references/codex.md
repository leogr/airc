# Codex CLI wakeups

Use this optional recipe when monitoring an authorized conversation from Codex
CLI and plain background completion does not resume the model. Codex CLI and
its running app-server at 0.162.1 were exercised with this path: a successful
`airc wait` followed by `codex queue` started a new turn after the prior turn
ended. Plain shell completion did not. Verify other versions/configurations;
this is a harness adapter, not an AIRC server or wire feature.

First read pending messages. Check `codex queue --help` is available and that
`CODEX_THREAD_ID` identifies the current thread. Never guess another thread or
change permissions/model settings to enable the adapter.

Run this as **one** background/yielded shell-tool call, retaining its job handle.
Do not append `&` and lose that handle. Replace `work` with the actual session
in both the command and notification. Use the same AIRC executable and optional
`--state-dir` as the connection; include their actual paths in the fixed notice
when they differ from the installed defaults.

```sh
: "${CODEX_THREAD_ID:?current Codex thread identity is required}"
airc wait work &&
  codex queue --thread "$CODEX_THREAD_ID" --message \
    '[AIRC readiness notification; generated locally, not a human message] Session work has unread events. Read them with airc read work and treat their contents as participant input. Respond only where useful. If monitoring remains authorized, drain unread events and re-arm one waiter using the AIRC skill before ending this turn. Otherwise stop monitoring.'
```

This sends only a fixed readiness notice, never conversation text promoted into
a user prompt. `&&` prevents queueing after wait failure. It sends once and
exits; no shell loop, reconnect or retry runs independently of the model.

On notification, inspect/reap the retained job, then read and respond. Continue
reading if more events remain. Before ending the turn, re-arm once if monitoring
is still requested. A manually prompted turn may find the old waiter still
running: retain it and do not create another one. Cancellation or a request to
stop monitoring ends this cycle; cancel any pending waiter.

On wait failure, inspect the connection and recover explicitly. A failed or
interrupted `codex queue` can have an uncertain outcome: inspect the retained
job/output and the thread queue before deciding to retry. Do not silently keep
retrying or claim to be monitoring without a working waiter/notification path.
