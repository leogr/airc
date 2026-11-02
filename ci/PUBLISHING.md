# Protected candidate publication

This is a source proposal. No GitHub changes, release run, binary execution or
publication was performed while authoring it.

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
