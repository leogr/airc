# AIRC knowledge base

Start with the relevant spec, then follow its numbered citations to an
implementation digest or source. Code and tests establish current behavior;
this KB records contracts, constraints and investigation boundaries. Repository
working rules are in [AGENTS.md](../AGENTS.md).

## Investigation map

| Question | Entry spec | Implementation evidence |
|---|---|---|
| What can an authenticated participant see or send, and what remains ordinary IRC? | [Collaboration](specs/collaboration.md) | [Protocol and permissions](digests/protocol.md) |
| What persists, what expires, and how are credentials and artifacts protected? | [Runtime](specs/runtime.md) | [Storage and transport](digests/storage.md) |
| How does an agent catch up, notice steering, confirm a send or recover a gap? | [Collaboration](specs/collaboration.md), [runtime](specs/runtime.md) | [Agent client](digests/agent-client.md) |

For detailed usage, read the [IRC profile](protocol.md), [agent API](agent-api.md),
[operations and validation guide](operations.md), or [architecture overview](architecture.md).
The [quick start](../README.md#quick-start) and [agent skill](../skills/airc/SKILL.md)
are consumer-facing instructions, not independent implementation evidence.

## Evidence conventions

- `[FACT]`: directly verified implementation, test contract or attributed source.
  A test's existence does not establish that a live environment passed it.
- `[DERIVED]`: a conclusion with explicit factual premises and citations.
- `[INFERENCE]`: uncertain interpretation, clearly limited.
- `[ASSUMPTION]`: an unverified premise needing investigation.

Digests retain verified facts; specs connect them and may draw justified
deductions. Significant claims have adjacent numbered links to a Sources entry.
Local entries identify the file, symbol and relevant line span. External sources
identify an exact version when available. A reported observation is evidence of
that observation, not proof of an unaudited implementation.

Each digest records a verification commit, identifying the source baseline.
For documentation committed with code, use the parent revision plus `and
accompanying changes`; after an outside commit, use that commit. Do not embed a
commit's own hash in itself. Re-read the current symbol whenever it changes;
line anchors alone do not prove a claim.

## Updating the KB

Review a requested commit's diff, then code/tests, then digest facts/citations,
dependent specs, usage references and this index. Commit code and documentation
together. A diff without KB impact needs an explicit assessment, not a token
documentation edit. Recover promptly if an earlier commit skipped that review.

Keep specs brief and digests focused, generally under 900 words including sources.
Split by investigation responsibility rather than one page per file. Evidence
flows spec → digest/code and digest → code/test/upstream or another digest, never
back to a spec/index. Keep history, proposals and session logs outside the KB.

`make docs-check` validates local links/anchors, index coverage, numbered source
structure, revision markers and evidence direction, and tests the checker. It
does not verify factual truth, external URLs or runtime behavior. The
[operations guide](operations.md#checks-and-limits-of-evidence) records runtime
coverage and limitations separately.
