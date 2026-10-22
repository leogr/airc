# Managed transport and membership control

Verification: `7da41509514601ec60d4b7a97ec3dedf5ad090db` and accompanying changes; re-read cited symbols when behavior changes.

[FACT] Server configuration takes exactly one of `listen` and `listen_unix`, and
for artifacts exactly one of `listen` and `listen_unix`. Unix IRC refuses TLS,
`allow_insecure` and a TCP artifact listener; Unix artifacts require Unix IRC
and skip the HTTPS origin check for `public_url`. Socket paths resolve against
the file and must be absolute, without `..`, and shorter than 100 bytes.
`control_unix` requires `membership_generation` and may not equal, contain or sit
inside any listener directory. `removed_channels` holds at most 1024 unique
canonical channel names absent from `channels`. [1](#source-1)

[FACT] A Unix listener first takes a stable lock file beside the socket (refusing
a non-regular lock path), so servers sharing the path are serialized. It refuses
an existing non-socket and a socket that accepts a connection, and unlinks only
a socket refusing connections. It binds a private temporary name, sets mode
0600, records that inode and publishes with a hard link, which never replaces an
existing path. Its owner record unlinks the path on drop only while
device/inode still match, and holds the lock for the listener's lifetime. The accept loop wraps Unix
streams without TLS; artifact HTTP serves the same Axum router on either
listener kind. [2](#source-2) [3](#source-3)

[FACT] `start_file` loads the configuration with the SHA-256 of exactly the bytes
parsed and records that digest and a baseline of non-reloadable fields. The
control task accepts at most four concurrent connections, reads one line of at
most 16 KiB with a ten-second deadline, re-reads the source file for reload and
passes the operation to the state actor. Replies are capped at 1 MiB.
[2](#source-2) [4](#source-4) [1](#source-1)

[FACT] Status reports protocol 1, the feature list, a per-process random
incarnation, applied generation, applied digest, listener kinds, the configured
account count and connected accounts with connection counts. Reload compares
the baseline (all fields except accounts, channels, `removed_channels` and
generation) and returns `RESTART_REQUIRED` on difference or when a currently
applied channel name is absent with its exact spelling and not listed in
`removed_channels`, so a rename or an unlisted removal fails after earlier
additions. Then `GENERATION_STALE` covers a lower generation and
`GENERATION_CONFLICT` the same generation with a different digest; the same
digest is idempotent. One store transaction deletes every row of each listed
channel, inserts new channels (`INSERT OR IGNORE` keeps a surviving row's
topic/moderation) and loads them with validated topics and bans; any failure
returns `STORAGE_ERROR`, commits nothing and changes no configuration, ACL,
digest or actor channel. Applying then drops deleted channels from the actor
and swaps accounts, channels, deletion intent and generation into the actor
and the shared handle read by artifact HTTP. Store opening purges the listed
names too, idempotently; unlisted absent channels keep their rows.
[5](#source-5) [10](#source-10)

[FACT] After applying, sessions whose account is absent or whose token hash
changed lose authority. The actor queues QUIT to peers and `ERROR :Membership
revoked` plus a close sentinel to the session; queue/socket failure or the write
deadline can prevent notification delivery.
Members no longer listed for a channel are KICKed by the server; configured
operator additions/removals change `+o`, leaving dynamic grants. Sessions in a
deleted channel get a server KICK `Channel deleted`; the reply's `deleted`
names those channels. An open batch for it is aborted by any other command,
the rejoin of a successor included. Uploads carry the
generation of the snapshot that authenticated and admitted them; the actor
refuses one whose channel became live later (`since`). The reply lists
disconnected accounts and up to 1024 kick details, with exact `kicked_count` and
`kicked_truncated`; applied state proof is never truncated. [5](#source-5) [2](#source-2)

[FACT] Client configuration may set `unix` (excluding host, port, proxy,
`ca_certificate` and `allow_insecure`) and `artifact_unix` (requiring `unix`,
excluding `artifact_url`). Unix connections have a ten-second timeout and no TCP
fallback; artifact HTTP uses reqwest's Unix socket support with origin
`http://airc.local`. `FEATURES` lists implemented features; both binaries print
it through `features`. `client-presence` covers JSONL WHO/away/back and
away-notify negotiation; `server-presence` covers explicit AWAY, WHO and
away-notify; `channel-add-reload`, `channel-delete-reload`, `client-topic` and
`named-channels` cover reload additions, explicit deletion, JSONL version 2
`topic` and named channel controls. Live
control status uses the same build's list. [6](#source-6) [7](#source-7) [8](#source-8)

[FACT] Tests run a Unix-only server with no TCP address, check 0600 sockets,
artifact upload/download/announcement over Unix and socket removal on shutdown;
stale/live/file/symlink handling; configuration exclusivity and control placement;
the feature probe from both binaries; and control status, idempotent/conflicting/
stale/restart-required reloads, KICK, account revocation within one second, old
token refusal over IRC and HTTP, and token rotation; a channel addition, a
one-channel ACL removal that KICKs without revoking the account (a rejoin gets
403) and a refused later replacement; `removed_channels` validation; a
deletion that persists across restart and allows a clean same-name channel;
and held uploads refused across delete/recreate/rejoin while another
channel's succeed. Unit tests cover lock refusal, ERROR after the kill sender
drops, exact kick counts with 1024 details for 1100 kicks, addition/removal/
rename with an injected staging failure, and deletion: rejected reloads purge
nothing, later requests add no rows, a stale batch never posts into a
successor, and startup purges only listed names.
[9](#source-9) [2](#source-2) [5](#source-5) [10](#source-10)

## Sources

1. <a id="source-1"></a> [src/config.rs](../../src/config.rs#L14-L48) — `MAX_REMOVED_CHANNELS`, `Config` fields; `load_with_digest` at L142-L171, `validate` (Unix/control and `removed_channels` rules) at L173-L356, `unix_path` at L376-L387.
2. <a id="source-2"></a> [src/server/mod.rs](../../src/server/mod.rs#L61-L217) — `ControlOp`, `Listener`, `OwnedSocket`, locked no-replace `bind_unix`; `start_file`/`start_inner` at L225-L369; `session` explicit-kill and writer sentinel at L389-L451; lock and graceful-close tests at L454-L511.
3. <a id="source-3"></a> [src/server/http.rs](../../src/server/http.rs#L23-L55) — generic `serve` over an Axum listener and per-request shared configuration; `accept` admission snapshot at L122-L218.
4. <a id="source-4"></a> [src/server/control.rs](../../src/server/control.rs#L1-L95) — bounded control listener, request handling and reply cap.
5. <a id="source-5"></a> [src/server/core.rs](../../src/server/core.rs#L95-L522) — `Channel` (`since`), `reloadable_baseline`, `status`, `reload`, `reloaded`, `close`; `admits` at L2389; reload tests at L2859-L3258.
6. <a id="source-6"></a> [src/client.rs](../../src/client.rs#L23-L132) — Unix endpoint fields, absolute-base loading and `validate`; `artifact_origin`/`http_client` at L1016-L1042.
7. <a id="source-7"></a> [src/transport.rs](../../src/transport.rs#L42-L59) — `connect_unix`.
8. <a id="source-8"></a> [src/lib.rs](../../src/lib.rs#L11-L48) — `FEATURES` and `feature_probe`; `features` commands in `src/bin/aircd.rs` and `src/bin/airc.rs`.
9. <a id="source-9"></a> [tests/managed.rs](../../tests/managed.rs#L90-L1254) — transport, sockets, configuration, probe, control, channel addition, ACL removal and deletion tests.
10. <a id="source-10"></a> [src/store.rs](../../src/store.rs#L64-L150) — `open`, `change_channels`; `purge` at L446-L455; startup test at L587-L633.
