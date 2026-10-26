# Shared message references

Verification: `9b5215ccc61ac235fa52f1f7211927d9d35d5f1f` and accompanying changes.

[FACT] A canonical (lowercase, hyphenated) UUID msgid has one shared reference:
`m:` plus the unpadded base64url encoding of its 16 bytes in display order, 24
ASCII characters. Decoding accepts exactly 22 characters from `A–Z a–z 0–9 - _`
whose decoded 16 bytes re-encode to the same text, so padding, whitespace, other
alphabets or lengths and nonzero unused bits fail; the result is the lowercase
hyphenated UUID, with no version or variant restriction. Other msgid forms get
no `m:` spelling. [1](#source-1) [5](#source-5)

[FACT] Record headers start with the message's reference and name a reply
target by `reply=` and its reference; neither needs retained local state. A
msgid without an `m:` form shows as `msgid:` and the ID only when that
spelling resolves to the identical bytes; otherwise its escaped display (spaces
as `\u{20}`) ends in `…[unusable]`, or is cut at 128 characters with
`…[truncated]`, and is never offered. A message without a msgid shows
`live-only`.
Confirmed-send outcomes, recovered-send notices, omission notices, delivery
truncation hints and `show` lookup notes use the same spelling. Message text,
delivery batch IDs, send tokens and operation IDs are unchanged. [1](#source-1)
[2](#source-2) [3](#source-3) [4](#source-4)

[FACT] `show` and `--reply` take an `m:` reference, `msgid:ID` or a raw msgid
(1–128 bytes, no whitespace or controls) and resolve it to one exact msgid. A
leading `@` is refused with guidance to use the `m:` reference; a malformed
`m:` form or a cut or escaped display fails locally and is never used as an
opaque msgid.
Both commands validate before IPC, so a refused reply prepares no receipt and
sends nothing. The `+reply` tag carries the full msgid. [1](#source-1)
[3](#source-3) [6](#source-6)

[FACT] `show` finds a retained record whose first word equals the msgid's
exact reference; otherwise it fetches one message by `CHATHISTORY AROUND` with
`--channel`, as before. It follows a parent only from a `reply=` reference that
round-trips and notes a cut, escaped or `unavailable` one. There is no prefix
search, sequence number or new server operation. [2](#source-2) [3](#source-3)

[FACT] Saved state stays version 3. Each retained msgid keeps its internal
number, used for deduplication and to map records an earlier release saved
under `@N`. Such records, recovered-send notices, omission notices and saved
receipts show the msgid's reference in `read`, fresh delivery batches, `show`
and `status`, or `unavailable` when the number is no longer retained; `show`
finds them through that number. A pending batch and `read --again` return their
saved bytes. Wire tags, history checkpoints and deduplication keep full msgids.
[2](#source-2) [4](#source-4)

[FACT] One JSON vector file fixes the codec for every client: valid pairs
covering byte order, both alphabet extremes and case, invalid spellings,
msgids with no `m:` form and opaque displays that are or are not shareable. Unit tests also refuse every unused-bit pattern of the
final character. Integration tests show two named clients printing the same
references live, in history, in confirmed sends and in a `show` chain, one
retained and one fetched from server history; and refuse `@N` arguments.
[5](#source-5) [6](#source-6) [7](#source-7)

## Sources

1. <a id="source-1"></a> [Codec](../../src/agent_session/reference.rs#L1-L247) — `compact`, `decode`, `resolve`, `present`, `exact`, `shareable` and their unit tests.
2. <a id="source-2"></a> [Presentation](../../src/agent_session/state.rs#L277-L551) — `remember`, `legacy`, `shown`, `header`, `retained_show`; `read` L614, `prepare_delivery` L715, `describe` L1086, `record_token`/`local_number` L1143–L1156.
3. <a id="source-3"></a> [Show walk](../../src/agent_session/mod.rs#L361-L443) — `advance`, `push`; `validate` at L1008 refuses show and reply arguments.
4. <a id="source-4"></a> [Delivery hint](../../src/agent_session/delivery.rs#L351-L357) — `record_reference` offers only shareable references.
5. <a id="source-5"></a> [Shared vectors](../../tests/data/message-references.json#L1-L53) — valid, invalid, uncompactable and opaque entries.
6. <a id="source-6"></a> [State and argument tests](../../src/agent_session/state.rs#L1435-L1580) — shared headers and receipts, saved-number records, byte-preserved pending batches; `mod.rs` refusal test at L1893.
7. <a id="source-7"></a> [Named-session tests](../../tests/named_sessions.rs#L187-L285) — send/reply refusal and confirmation; two-client test at L1908, show chain at L1983.
