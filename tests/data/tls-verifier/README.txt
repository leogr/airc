Public offline TLS distrust fixtures

Source: https://github.com/galadran/we-have-a-problem.com
Immutable revision: 05ebae9f2895478c514e25e221614cf5248750a4
Retrieved over authenticated HTTPS from raw.githubusercontent.com on 2026-10-30.
The two chains are copied byte-for-byte from chains/regular/ with their original
names. emsign-g3-root.pem and aaa-root.pem are the unmodified PEM fields of the
matching certificates in roots.json. Their DER SHA256 identities were also
checked against that upstream manifest. Git blob IDs were independently matched
against the HTTPS GitHub tree response. Only public certificates are retained;
there are no private keys. The upstream files are under Apache-2.0; the full
license is retained in LICENSE-APACHE-2.0, with the upstream NOTICE. Certificate
bytes are not modified.

Original paths:
chains/regular/dv-g3b-noct-emudhra.we-have-a-problem.com.pem
chains/regular/dv-g3b-aaa-noct-emudhra.we-have-a-problem.com.pem
roots.json (emSign ECC Root CA - G3; AAA Certificate Services)

Verification time: 2026-10-30T00:00:00Z (1793318400), within both leaves' validity.
Both tests explicitly add the corresponding root to the normal compiled store.
They must fail with CertDistrusted, not UnknownIssuer, expiry or name mismatch.
The G3B key is shared across its G3 and AAA cross-signs. The block is supplied by
rustls-webpki 0.103.16; see RUSTSEC-2026-0382 and rustls/webpki PR546, merged at
8ecdcc76f56b68fda67a6adf7976f5eed5006a4c. No test connects to the incident hosts.
An unrelated generated localhost certificate is the valid/custom-CA control;
it also retains negative hostname and missing-anchor checks.

SHA256 (PEM bytes as committed):
1ed6f8a660c9cb74052f0409815a06afaf5766341e866cf17c427e8f05a60521  aaa-root.pem
a360266958b7dfc04250553f6a5f55be769457f126bfd4de415bbaffc9cf62ad  dv-g3b-aaa-noct-emudhra.we-have-a-problem.com.pem
de597b4e346d6e84d264a6bc62f5cb5bf4a4d64ec5e587edc446016e4aa693c5  dv-g3b-noct-emudhra.we-have-a-problem.com.pem
6ec5f9223d14cf158280993df354563a22756c68c5e9aa3a60501b9527ed5427  emsign-g3-root.pem
