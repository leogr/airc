# Agent client and file operations

Verification: `59064c6188658bf60131db6afc33ab7bbf45357c` and accompanying changes; re-read cited symbols when behavior changes.

[FACT] Client config is strict, resolves credential/CA paths relative to its file,
and requires TLS for remote connections unless plaintext is explicit. Tokens are
read from a file. Connection startup negotiates available capabilities,
authenticates with SASL, and completes configured JOIN/NAMES before returning.
Startup events remain available in a bounded buffer. [1](#source-1)

[FACT] Optional `proxy` is a credential-free HTTP origin. IRC uses CONNECT with
bounded headers and no destination DNS lookup or direct fallback. The transport
deadline is ten seconds inside twenty-second registration; TLS verifies the
destination after tunneling. The same explicit proxy configures artifact HTTP.
Without it, IRC is direct and HTTP retains reqwest environment proxy behavior.
Tests cover refusal, framing, injection, tunneled TLS trust/hostname checks and
proxied SASL/artifact round trips. [8](#source-8) [9](#source-9) [4](#source-4)

[FACT] The JSONL frontend emits versioned ready metadata and reads bounded JSONL
requests in order. A 512-event ring uses local cursors and an epoch; read responses
include next/latest and a gap flag. Waits and page sizes are bounded. IRC results
and errors arrive through the event stream. Queuing a send does not establish
delivery. Disconnects fail explicitly without reconnect or mutation replay.
[2](#source-2)

[FACT] `connect_with` requests draft/multiline only when asked and the
advertised limits fit 256 KiB and 4096 lines; then a bounded assembler (eight
open batches) turns server batches, including nested history ones, into one
message carrying the opener's tags. Invalid or oversized batches degrade to
fallback lines, never partial text as one message. Outbound items are frame
groups written contiguously. JSONL `--jsonl 2` uses it: assembled events add
`lines`, `send` returns a pending receipt, one send may be pending, and a ring
receipt follows the correlated echo (tag, account, nick, target, text digest)
or rejection; unrelated events never resolve it. Timeout or disconnect writes a
closed event listing the uncertain receipt. Version 1 is unchanged. Requests are
2 MiB, the ring 512 events/8 MiB, pages 1 MiB with at least one event.
[11](#source-11) [12](#source-12) [13](#source-13)

[FACT] The binary's shutdown timeout handles Tokio stdin's uncancellable blocking
read after quit. Connection drop cancels its transport task. The process-lifecycle
test keeps stdin open and requires quit to terminate. [3](#source-3) [1](#source-1)
[7](#source-7)

[FACT] HTTP operations use the configured origin, disable redirects and use
account/token authentication. Upload bounds the file read to 16 MiB; download
accepts a UUID, caps received bytes and creates the destination exclusively.
Paths supplied to operations use the process working directory. The client
reports bytes, SHA-256 and the bounded server filename without embedding credentials in URLs.
[4](#source-4)

[FACT] Standalone `artifact upload|download` dispatches before named-session
storage and opens HTTP only. Raw stdin upload reads at most 16 MiB + 1 with a
30-second deadline; success stdout is one JSON object and errors use stderr.
Upload replies are capped at 8192 bytes and checked against the input's size/hash.
HTTP non-success, including redirects, is rejected; no retry follows an ambiguous
post-send failure. Named reads render complete validated reserved metadata in the
header, retaining it in read replay/history. Fake body text and client-only tags
cannot populate those fields. [15](#source-15) [16](#source-16)

[FACT] Integration tests exercise SASL, the JSONL frontend, injection rejection,
artifact upload/download and refusal to overwrite. A cursor test checks lost
events, page continuation and unknown-field rejection. [5](#source-5)
[6](#source-6)

[FACT] Named CLI sessions add private local persistence, compact reads and
correlated send confirmation on the same connection implementation. Their
separate lifecycle and limits are documented in the implementation digest.
[10](#source-10)

[FACT] Available away-notify is negotiated. JSONL away/back queue standard AWAY
and expose server acknowledgments as events; they do not claim confirmation.
The shared builder refuses empty, over-200-byte or CR/LF/NUL reasons. JSONL who
queues WHO for a valid channel; raw 352 H/G/role rows and 315 supply roster data
under existing server visibility checks. [14](#source-14)

## Sources

1. <a id="source-1"></a> [src/client.rs](../../src/client.rs#L23-L493) — `ClientConfig`, token reading, `connect_with`, registration/JOIN setup and connection drop.
2. <a id="source-2"></a> [src/client.rs](../../src/client.rs#L937-L1050) — version 1 `run_frontend`, bounded events and requests.
3. <a id="source-3"></a> [src/bin/airc.rs](../../src/bin/airc.rs#L156-L173) — `main` runtime shutdown after frontend termination.
4. <a id="source-4"></a> [src/client.rs](../../src/client.rs#L1157-L1327) — `http_client`, bounded `upload_bytes`, `artifact_response` and exclusive `download`.
5. <a id="source-5"></a> [tests/collaboration.rs](../../tests/collaboration.rs#L459-L568) — `frontend_scenario`, SASL/JSONL and artifact round trip.
6. <a id="source-6"></a> [src/client.rs](../../src/client.rs#L1332-L1457) — pending-receipt matching and `cursor_pages_report_gaps_and_resume_without_skipping`.
7. <a id="source-7"></a> [tests/collaboration.rs](../../tests/collaboration.rs#L748-L792) — `client_quit_exits_with_stdin_still_open`.
8. <a id="source-8"></a> [src/transport.rs](../../src/transport.rs#L26-L176) — `connect`, `proxy_url`, bounded CONNECT; transport regressions at L178–281.
9. <a id="source-9"></a> [tests/collaboration.rs](../../tests/collaboration.rs#L20-L71) — `Proxy`; `proxy_sasl_and_artifact_round_trip` at L464–568, `proxy_tls_requires_trust_and_checks_hostname` at L707–746.
10. <a id="source-10"></a> [Named session evidence](named-sessions.md) — lifecycle, confirmation and bounded recovery.
11. <a id="source-11"></a> [src/client.rs](../../src/client.rs#L169-L493) — `connect_with`, multiline negotiation, assembly and frame-group writer.
12. <a id="source-12"></a> [src/client.rs](../../src/client.rs#L599-L935) — `frontend_with`, ring, `rejects`, `observe`, `run_frontend_v2`; shared `src/multiline.rs` `Assembler` and its tests.
13. <a id="source-13"></a> [tests/managed.rs](../../tests/managed.rs#L603-L698) — JSONL v2 receipts and v1 fallback; pending-send regression in `src/client.rs:1332–1374`.
14. <a id="source-14"></a> [src/client.rs](../../src/client.rs#L1052-L1155) — `away_message`, `to_message`; `connect_with` negotiates away-notify, `presence_tests:1527–1559` validates bounds.
15. <a id="source-15"></a> [src/bin/airc.rs](../../src/bin/airc.rs#L97-L195) — `ArtifactCommand`, `artifact_command` and early HTTP dispatch; `tests/collaboration.rs:1707–1912` tests CLI no-collision, caps, private exclusive paths, redirects/503 and malformed/oversized replies; `tests/managed.rs:86–208` tests Unix-only CLI download.
16. <a id="source-16"></a> [src/agent_session/state.rs](../../src/agent_session/state.rs#L352-L401) — header rendering; `artifact_metadata:785–816` and replay regression at L852–894.
