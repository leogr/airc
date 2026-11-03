# CI publication and original-release attachment

This is a source candidate. No remote mutation, release run or publication was
performed while authoring it. Local diagnosis and mock checks are separate from
native producer and publication qualification.

## Manual protected candidate publication

Install the reviewed common files as `ci/publish-release.py` and
`ci/publish-release.sh` in each upstream CI definition revision W. Run:

```sh
bash ci/publish-release.sh SOURCE_SHA NEW_RELEASE_TAG SAME_RUN_INPUT_DIR
```

Default mode is PLAN and reads local inputs only. Inputs are exactly the two
download-artifact directories `OpenShell-linux-{amd64,arm64}` or
`airc-linux-{amd64,arm64}`, containing the producer's `publication/` export at
each directory root. Every referenced raw evidence path must survive export.
The helper validates source archive/Git tree/lock, actual compiler/native host,
OCI bytes, two distinct successful container builds, reproduced archive bytes,
static ELF, version/help, and AIRC's actual upstream/behavior test records.
AIRC additionally exports and validates its effective local UNIX Docker socket,
raw context/daemon/native architecture and resource records, both Docker/output
backing filesystems, the fixed 20/16/14 GiB disk floors and 4/2 GiB memory
floors throughout the bounded run. Create/start records bind to that admitted
endpoint; the publishing worker never resolves the producer's host paths.
Checksums and successful-looking JSON booleans cannot replace those records.

Only the reviewed protected publish job sets `RELEASE_MODE=apply`, with
`RELEASE_REPOSITORY`, `BUILD_DEFINITION`, fixed `SOURCE_SUBJECT`, `RELEASE_TAG`,
and `GH_TOKEN`. `GITHUB_*` context must identify first-attempt owner-requested
`workflow_dispatch` on protected `refs/heads/release-ci`. The immutable W
workflow must have literal publish job environment `upstream-release` and the
actual review history must contain owner approval for it. This uses existing
GitHub environment authorization, not a new approval or recovery mechanism.
Required token permissions are `contents:write` and `actions:read`; unavailable
metadata/protection/approval is a blocking owner configuration prerequisite.

Apply joins the extracted inputs to independently fetched same-run artifact
ZIP bytes and server digests. It creates a fresh local attempt directory under
`RUNNER_TEMP`, consumes an O_EXCL/fsynced intent before every remote mutation,
creates a NEW tag then a NEW draft, uploads the exact asset set, downloads and
hashes every draft asset independently, and only then publishes a prerelease
with `make_latest=false`. No clobber, existing-tag/draft adoption, deletion,
generic retry or automatic recovery path exists. The protected workflow should
retain the publication attempt directory using an always-run artifact step.

An HTTP success creates only a transport receipt with `result_known=false`.
The exact operation ACK (new tag identity, new draft identity, uploaded asset
identity/size/digest or final published release identity/timestamp) must pass
before `.complete.json` records semantic `result_known=true`. A malformed or
mismatched ACK is UNCERTAIN even with HTTP201/200; its intent and available
transport receipt remain, with no suffix mutation or replay. A later failed
verification read is PARTIAL PUBLICATION: earlier exact ACKs remain known and
are listed, rather than being mislabeled no-effect or retroactively uncertain.

Once the final PATCH exact ACK and its operation `.complete.json` are durable,
failure of the later aggregate `publication-complete.json` write/fsync or stdout
report is `PUBLISHED_LOCAL_REPORTING_FAILURE`, with `result_known=true`, the
published release/tag/assets, the receipt directory and original exact ACK
receipt references. It stops and never repeats PATCH. A failure persisting the
mutation's own semantic ACK remains UNCERTAIN under the existing mutation
boundary. Local report failure does not erase or revise a known publication.

A failed/lost mutation response stops. Local client settlement or a later
absence/presence query never proves an earlier transaction's outcome. A rerun
attempt greater than1 is refused. Across runs an uncertain prior attempt must
be inspected by the owner before a fresh explicit protected dispatch; starting
another worker does not grant recovery authority or authorize replay. Existing
tags/releases/receipts stop the helper. Partial drafts/refs and all receipts
remain for owner review.

The published channel is **candidate/prerelease producer bytes**. Consumer,
custom-base, yoloctl G1–G8 and final embedded-pin qualification remain NOT RUN.
The helper does not enable repository immutable-release/tag settings; those
remain owner configuration, and no server-enforced immutability is claimed.
Only the exact two fixed repositories/source subjects are supported. Different
producer schemas, workflow shapes or missing evidence require review and fail
closed; they are never silently blessed.

API contracts: [workflow review history](https://docs.github.com/en/rest/actions/workflow-runs),
[release API](https://docs.github.com/en/rest/releases/releases),
and [release assets](https://docs.github.com/en/rest/releases/assets).


## Automatic AIRC attachment to the original published release

The AIRC workflow also handles `release: types: [published]` for stable releases and prereleases. The owner publishes the original release; both native architectures then build and run the unchanged checks. Only after both succeed does the `attach` job run `ci/attach-release.py SOURCE_SHA TAG SAME_RUN_INPUT_DIR SOURCE_CHECKOUT`. It uses `contents:write` and `actions:read`. The original owner-published event authorizes attachment to that release ID; this path does not require the separate manual new-release environment approval and cannot create or publish another release. First-attempt owner identity, original event/ref, actual run head S, both successful same-run jobs, immutable workflow W and actual script/recipe bytes must agree. Missing evidence is a refusal.

S is the actual tagged release commit (`github.sha`); W is `github.workflow_sha`. Separate clean checkouts and the original bounded event file bind them independently, including when they are equal. The source tree and lock are derived from S, and the entire archive is reconstructed against that tree. Existing validators still check every binary/member/mode, both reproduced builds, static ELF, native tests and full raw evidence. Same-run Actions ZIPs are independently fetched and rehashed. The two actual producer recipes must match W before any upload. No runtime option can supply a different release ID, upload URL, source policy or workflow identity.

Attachment repeatedly checks the original numeric release ID, tag, published state and tag target. It POSTs only the complete checked asset set (two native archives, source/proof/manifest/checksum assets) to that ID and downloads every uploaded asset to verify bytes. It never creates a tag/release, PATCHes release metadata, deletes, overwrites or retries a mutation. Unrelated owner assets are preserved. A complete pre-existing managed set is accepted only after all exact bytes and identities verify; this is a read-only reconciliation, never proof that an earlier mutation succeeded. A partial, conflicting, duplicate or incomplete managed set stops before uploads and requires owner receipt inspection; it is not resumed or clobbered.

Each upload uses the shared publisher's exclusive fsynced intent and exact acknowledgement receipt. Lost/malformed acknowledgement is UNCERTAIN with no replay. A failed read after an exact acknowledgement retains that known upload as PARTIAL. Aggregate local reporting failure leaves acknowledged outcomes known. The always-run artifact step retains the new `attachment-airc-RUN_ID` attempt directory. Concurrent owner edits can still interrupt an attempt after acknowledged uploads; repeated checks do not claim a cross-request server transaction. Review receipts rather than rerunning it blindly.

Attachment does not emit another `release/published` event. Manual new-release publication uses GITHUB_TOKEN, whose events GitHub suppresses from starting another workflow. Workflow-dispatch remains available with publish=false for native build diagnosis, and its existing protected publish=true path remains separate. Neither path automatically qualifies yoloctl consumer pins, image/custom-base ABI or runtime defaults.

GitHub contracts: [release event source/ref and published trigger](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows#release), [workflow commit context](https://docs.github.com/en/actions/reference/workflows-and-actions/contexts#github-context), and [GITHUB_TOKEN event suppression](https://docs.github.com/en/actions/concepts/security/github_token).
