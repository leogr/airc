# Agent client and file operations

Verification: `8d76633ca9a712a72b017dbf20871cad064a9bd6`; re-read cited symbols when behavior changes.

[FACT] Client config is strict, resolves credential/CA paths relative to its file,
and requires TLS for remote connections unless plaintext is explicit. Tokens are
read from a file. Connection startup negotiates available capabilities,
authenticates with SASL, and completes configured JOIN/NAMES before returning.
Startup events remain available in a bounded buffer. [1](#source-1)

[FACT] The frontend emits versioned ready metadata and reads bounded JSONL
requests in order. A 512-event ring uses local cursors and an epoch; read responses
include next/latest and a gap flag. Waits and page sizes are bounded. IRC results
and errors arrive through the event stream. Queuing a send does not establish
delivery. Disconnects fail explicitly without reconnect or mutation replay.
[2](#source-2)

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

## Sources

1. <a id="source-1"></a> [src/client.rs](../../src/client.rs#L20-L276) — configuration, token reading, connection startup and drop.
2. <a id="source-2"></a> [src/client.rs](../../src/client.rs#L283-L564) — request types, `frontend`, `run_frontend`, ring and IRC operation conversion.
3. <a id="source-3"></a> [src/bin/airc.rs](../../src/bin/airc.rs#L12-L26) — runtime shutdown after frontend termination.
4. <a id="source-4"></a> [src/client.rs](../../src/client.rs#L566-L637) — fixed-origin HTTP client, bounded upload and exclusive download.
5. <a id="source-5"></a> [tests/collaboration.rs](../../tests/collaboration.rs#L338-L425) — SASL/JSONL and artifact round trip.
6. <a id="source-6"></a> [src/client.rs](../../src/client.rs#L640-L718) — cursor-gap and pagination contract test.
7. <a id="source-7"></a> [tests/collaboration.rs](../../tests/collaboration.rs#L594-L639) — `client_quit_exits_with_stdin_still_open`.
