# TLS verifier dependencies

Verification: `685d73efb290538aa64e2cb18e4ffb53b114965b` and accompanying changes;
re-read cited symbols when behavior changes.

[FACT] The committed lock selects rustls-webpki 0.103.16 and webpki-roots
1.0.10. [1](#source-1) [8](#source-8) The verifier and compiled roots are shared by rustls 0.23.45,
tokio-rustls 0.26.6 and reqwest 0.12.28 through hyper-rustls 0.27.10.
[1](#source-1) [9](#source-9) [10](#source-10) [11](#source-11) The
locked-graph regression inspects every package, including duplicate and
target-specific entries, and rejects affected or unreviewed verifier/root
versions. [2](#source-2) Building with `--locked` preserves the selected versions
and registry checksums. [12](#source-12)

[FACT] RUSTSEC-2026-0382 identifies rustls-webpki 0.103.16 as the stable fix
for paths containing compromised emSign intermediate public keys, even when
configured anchors allow the path. RUSTSEC-2026-0381 identifies webpki-roots
1.0.10 as removing emSign ECC Root CA - G3. Verifier distrust and default-root
removal are separate protections; a private CA file does not bypass verifier
key distrust. [3](#source-3) [4](#source-4)

[FACT] IRC TLS adds the configured CA certificates to compiled roots and
verifies the destination hostname after an optional CONNECT tunnel. The
artifact client independently uses reqwest TLS, adds the configured CA, and
disables redirects. Neither path offers a skip-verification option. Unix
transports are separate local endpoints. [5](#source-5) [6](#source-6)

[FACT] The offline regression supplies authenticated upstream public chains
and the corresponding G3 or AAA anchor at a fixed verification time, requiring
`CertDistrusted` for both cross-signed paths. It also checks G3 is absent from
compiled roots and an unrelated custom CA still verifies its valid hostname.
The HTTPS artifact fixture exercises the production download client through an
owned loopback CONNECT tunnel, accepting the trusted localhost certificate and
rejecting missing trust or a wrong hostname. Existing IRC tests cover direct
and proxied TLS. These are test contracts, not a claim of completed deployment
or a probe of the incident website. [7](#source-7) [13](#source-13)
[14](#source-14) [15](#source-15)

## Sources

1. <a id="source-1"></a> [Cargo.lock](../../Cargo.lock#L1370-L1412) — `rustls` and `rustls-webpki` versions, dependencies and checksums.
2. <a id="source-2"></a> [tests/tls_verifier.rs](../../tests/tls_verifier.rs#L36-L76) — `locked_tls_graph_contains_only_patched_verifiers_and_roots`.
3. <a id="source-3"></a> [RUSTSEC-2026-0382](https://rustsec.org/advisories/RUSTSEC-2026-0382.html) — affected paths and patched stable 0.103.16.
4. <a id="source-4"></a> [RUSTSEC-2026-0381](https://rustsec.org/advisories/RUSTSEC-2026-0381.html) — root removal at 1.0.10 and compatibility-wrapper limits.
5. <a id="source-5"></a> [src/transport.rs](../../src/transport.rs#L43-L107) — `connect_unix`, `connect_inner` and additive TLS roots.
6. <a id="source-6"></a> [src/client.rs](../../src/client.rs#L1257-L1503) — `http_client` at L1257–1273; `download` at L1470–1503.
7. <a id="source-7"></a> [tests/collaboration.rs](../../tests/collaboration.rs#L829-L875) — `tls_scenario`, direct and proxied trust/hostname tests.
8. <a id="source-8"></a> [Cargo.lock](../../Cargo.lock#L2073-L2080) — `webpki-roots` version, dependency and checksum.
9. <a id="source-9"></a> [Cargo.lock](../../Cargo.lock#L1757-L1765) — `tokio-rustls` version and dependencies.
10. <a id="source-10"></a> [Cargo.lock](../../Cargo.lock#L1265-L1301) — `reqwest` version and TLS dependencies.
11. <a id="source-11"></a> [Cargo.lock](../../Cargo.lock#L663-L677) — `hyper-rustls` version and TLS dependencies.
12. <a id="source-12"></a> [Makefile](../../Makefile#L6-L17) — `lint`, `test` and `docs-check` use `--locked`.
13. <a id="source-13"></a> [tests/tls_verifier.rs](../../tests/tls_verifier.rs#L11-L135) — `verification_time`, supplied-anchor distrust, unrelated-CA and compiled-root controls.
14. <a id="source-14"></a> [tests/tls_verifier.rs](../../tests/tls_verifier.rs#L137-L318) — typed certificate error helper and `https_artifact_requires_trust_and_checks_hostname`.
15. <a id="source-15"></a> [Fixture provenance](../../tests/data/tls-verifier/README.txt#L1-L33) — exact upstream revision, hashes, Apache-2.0 license and notice.
