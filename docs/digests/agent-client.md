# Agent client and file operations

Verification: `db271dabb0266cfa5e491ac056d6c064282d4a78` and accompanying changes; re-read cited symbols when behavior changes.

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
reports bytes and SHA-256 without embedding credentials in URLs.
[4](#source-4)

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

1. <a id="source-1"></a> [src/client.rs](../../src/client.rs#L20-L292) — configuration, token reading, connection startup and drop.
2. <a id="source-2"></a> [src/client.rs](../../src/client.rs#L818-L911) — version 1 `run_frontend`; request types and IRC operation conversion nearby.
3. <a id="source-3"></a> [src/bin/airc.rs](../../src/bin/airc.rs#L79-L96) — runtime shutdown after frontend termination.
4. <a id="source-4"></a> [src/client.rs](../../src/client.rs#L1026-L1098) — fixed-origin HTTP client, bounded upload and exclusive download.
5. <a id="source-5"></a> [tests/collaboration.rs](../../tests/collaboration.rs#L425-L534) — SASL/JSONL and artifact round trip.
6. <a id="source-6"></a> [src/client.rs](../../src/client.rs#L1101-L1229) — cursor-gap, pagination and pending-receipt tests.
7. <a id="source-7"></a> [tests/collaboration.rs](../../tests/collaboration.rs#L714-L758) — `client_quit_exits_with_stdin_still_open`.

8. <a id="source-8"></a> [src/transport.rs](../../src/transport.rs#L26) — `connect`, `proxy_url`, bounded CONNECT and transport regressions.
9. <a id="source-9"></a> [tests/collaboration.rs](../../tests/collaboration.rs#L20) — `Proxy`, `proxy_sasl_and_artifact_round_trip`, `proxy_tls_requires_trust_and_checks_hostname`.

10. <a id="source-10"></a> [Named session evidence](named-sessions.md) — lifecycle, confirmation and bounded recovery.
11. <a id="source-11"></a> [src/client.rs](../../src/client.rs#L169-L379) — `connect_with`, multiline negotiation, assembly and frame-group writer.
12. <a id="source-12"></a> [src/client.rs](../../src/client.rs#L480-L816) — `frontend_with`, ring, `rejects`, `observe`, `run_frontend_v2`; `src/multiline.rs` `Assembler` and its tests.
13. <a id="source-13"></a> [tests/managed.rs](../../tests/managed.rs#L569-L663) — JSONL v2 receipts and v1 fallback; pending-send unit regression in `src/client.rs` at L1105-L1147.

14. <a id="source-14"></a> [src/client.rs](../../src/client.rs#L931-L1011) — `away_message`, `to_message`; `connect_with` requests away-notify and `presence_tests` validates operation mapping/bounds.
