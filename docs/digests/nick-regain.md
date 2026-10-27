# Same-account nick regain

Verification: `832ed2c20c53d55cb736dd50f70f9f162c916c29` and accompanying changes.

[FACT] A nickname that another connection holds is refused with 433. The
comparison folds nicknames, and the holder's account is consulted only for an
authenticated, unregistered connection that negotiated `airc.dev/nick-regain`.
When the holder is that same account, NICK gets `FAIL NICK NICK_HELD` and
`REGAIN` closes the holder ("Session taken over"), then takes the name. Another
account's holder stays 433. REGAIN without the capability or authentication, or
after registration, is `FAIL REGAIN INVALID_STATE`. [1](#source-1) [2](#source-2)

[FACT] `close` broadcasts `QUIT :Session taken over` to peers, sends `ERROR` to
the closed session and ignores its later input. Pending sends from that session
are therefore never relayed. [3](#source-3)

[FACT] The client sends NICK, or REGAIN when `regain = true`, only after SASL
succeeds and before CAP END. It requests the capability when offered, and a
`regain` client refuses a server without it. A registration `FAIL NICK|REGAIN`
fails as `IRC registration failed (CODE)`. `regain` defaults to false and is
not serialized when false. [4](#source-4)

[FACT] JSONL version 2 records the server's ERROR reason. Its closed event adds
`server_error`, and the process exits nonzero with that reason. Nothing
reconnects or resends. [5](#source-5)

[FACT] Tests cover the server rules with raw peers: an ordinary same-account
client still gets 433, another account cannot learn or take over the holder,
REGAIN state refusals, NICK_HELD that leaves the holder intact, and take-over
with ERROR/QUIT and a case-folded welcome. A JSONL v2 test covers the human-view
sequence: NICK_HELD, another account's 433, and the closed view's
`server_error` and nonzero exit. A send that is pending exactly when the view
is taken over is not exercised deterministically. [6](#source-6)

## Sources

1. <a id="source-1"></a> [Server nick rule](../../src/server/core.rs#L840-L890) — `nick` (NICK/REGAIN dispatch at L787–L791); `CAPS` at L20–L32.
2. <a id="source-2"></a> [Capability name](../../src/protocol.rs#L9-L12) — `NICK_REGAIN`; feature `nick-regain` in `src/lib.rs` `FEATURES`.
3. <a id="source-3"></a> [Session close](../../src/server/core.rs#L480-L503) — `close`.
4. <a id="source-4"></a> [Client registration](../../src/client.rs#L44-L50) — `ClientConfig::regain`; `open_connection` capability check, request, post-SASL NICK/REGAIN and FAIL mapping at L284–L374 (`NICK_REGAIN` imported at L4).
5. <a id="source-5"></a> [Closed event](../../src/client.rs#L688-L715) — `Ring::error`, `Ring::message`; `run_frontend_v2` closed event at L988–L1002.
6. <a id="source-6"></a> [Regain tests](../../tests/collaboration.rs#L605-L729) — `regain_peer`, `nick_regain_takes_over_only_the_same_accounts_session`, `regain_client_closes_the_taken_over_jsonl_view`.
