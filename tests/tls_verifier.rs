use rustls::client::danger::ServerCertVerifier;
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use std::{io::Cursor, sync::Arc, time::Duration};

const G3_CHAIN: &[u8] =
    include_bytes!("data/tls-verifier/dv-g3b-noct-emudhra.we-have-a-problem.com.pem");
const AAA_CHAIN: &[u8] =
    include_bytes!("data/tls-verifier/dv-g3b-aaa-noct-emudhra.we-have-a-problem.com.pem");
const G3_ROOT: &[u8] = include_bytes!("data/tls-verifier/emsign-g3-root.pem");
const AAA_ROOT: &[u8] = include_bytes!("data/tls-verifier/aaa-root.pem");

fn certificates(pem: &[u8]) -> Vec<CertificateDer<'static>> {
    rustls_pemfile::certs(&mut Cursor::new(pem))
        .collect::<Result<_, _>>()
        .unwrap()
}

fn verifier(extra_ca: &[u8]) -> Arc<rustls::client::WebPkiServerVerifier> {
    let mut roots = rustls::RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    for certificate in certificates(extra_ca) {
        roots.add(certificate).unwrap();
    }
    rustls::client::WebPkiServerVerifier::builder_with_provider(
        Arc::new(roots),
        Arc::new(rustls::crypto::ring::default_provider()),
    )
    .build()
    .unwrap()
}

fn verification_time() -> UnixTime {
    // 2026-10-30T00:00:00Z: within the public fixtures' validity periods.
    UnixTime::since_unix_epoch(Duration::from_secs(1_793_318_400))
}

#[test]
fn locked_tls_graph_contains_only_patched_verifiers_and_roots() {
    let lock: toml::Value = toml::from_str(include_str!("../Cargo.lock")).unwrap();
    let packages = lock["package"].as_array().unwrap();
    let mut verifiers = 0;
    let mut roots = 0;
    // Inspect every locked package, including duplicates and target-specific
    // consumers, rather than checking only AIRC's direct dependencies.
    for package in packages {
        let name = package["name"].as_str().unwrap();
        if !matches!(name, "rustls-webpki" | "webpki" | "webpki-roots") {
            continue;
        }
        let version = package["version"].as_str().unwrap();
        let parts: Vec<u64> = version.split('.').map(|p| p.parse().unwrap()).collect();
        assert_eq!(
            parts.len(),
            3,
            "unexpected TLS dependency version: {version}"
        );
        match name {
            "rustls-webpki" => {
                assert!(
                    parts[0] == 0 && parts[1] == 103 && parts[2] >= 16,
                    "unreviewed or affected verifier: {version}"
                );
                verifiers += 1;
            }
            "webpki-roots" => {
                assert!(
                    parts[0] == 1 && (parts[1] > 0 || parts[2] >= 10),
                    "unreviewed or affected roots: {version}"
                );
                roots += 1;
            }
            _ => panic!("legacy webpki verifier requires explicit review: {version}"),
        }
    }
    assert!(verifiers > 0 && roots > 0, "TLS packages missing from lock");
}

#[test]
fn supplied_anchors_cannot_reenable_distrusted_intermediate_keys() {
    for (chain, anchor, hostname) in [
        (
            G3_CHAIN,
            G3_ROOT,
            "dv-g3b-noct-emudhra.we-have-a-problem.com",
        ),
        (
            AAA_CHAIN,
            AAA_ROOT,
            "dv-g3b-aaa-noct-emudhra.we-have-a-problem.com",
        ),
    ] {
        let chain = certificates(chain);
        assert!(chain.len() >= 2, "fixture must contain leaf and issuer");
        let result = verifier(anchor).verify_server_cert(
            &chain[0],
            &chain[1..],
            &ServerName::try_from(hostname).unwrap(),
            &[],
            verification_time(),
        );
        let error = result.unwrap_err();
        assert!(
            format!("{error:?}").contains("CertDistrusted"),
            "{hostname} must fail for key distrust, not missing roots, time or hostname: {error:?}"
        );
    }
}

#[test]
fn unrelated_custom_ca_still_verifies_valid_chain_and_hostname() {
    let certificate = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let trusted = verifier(certificate.cert.pem().as_bytes());
    let verify = |verifier: &rustls::client::WebPkiServerVerifier, name: &str| {
        verifier.verify_server_cert(
            certificate.cert.der(),
            &[],
            &ServerName::try_from(name.to_owned()).unwrap(),
            &[],
            verification_time(),
        )
    };
    verify(&trusted, "localhost").unwrap();
    assert!(verify(&trusted, "wrong.example").is_err());
    assert!(verify(&verifier(b""), "localhost").is_err());
}

#[test]
fn compiled_roots_exclude_emsign_g3() {
    let mut old_root = rustls::RootCertStore::empty();
    old_root.add(certificates(G3_ROOT).remove(0)).unwrap();
    assert!(
        webpki_roots::TLS_SERVER_ROOTS.iter().all(|root| {
            root.subject_public_key_info != old_root.roots[0].subject_public_key_info
        })
    );
}

fn certificate_error<'a>(
    error: &'a (dyn std::error::Error + 'static),
) -> Option<&'a rustls::CertificateError> {
    let tls = error.downcast_ref::<rustls::Error>().or_else(|| {
        error
            .downcast_ref::<std::io::Error>()?
            .get_ref()?
            .downcast_ref::<rustls::Error>()
    });
    if let Some(rustls::Error::InvalidCertificate(certificate)) = tls {
        return Some(certificate);
    }
    error.source().and_then(certificate_error)
}

#[tokio::test]
async fn https_artifact_requires_trust_and_checks_hostname() {
    use airc::{client, config::write_private};
    use anyhow::{Context, ensure};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    for (host, trusted, expected) in [
        ("localhost", false, false),
        ("localhost", true, true),
        ("127.0.0.1", true, false),
    ] {
        let case = format!("host={host} trust={trusted}");
        eprintln!("HTTPS {case} phase=setup");
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        let dir = tempfile::tempdir().unwrap();
        let certificate = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let cert_path = dir.path().join("cert.pem");
        let key_path = dir.path().join("key.pem");
        let token_path = dir.path().join("token");
        write_private(&cert_path, certificate.cert.pem().as_bytes()).unwrap();
        write_private(
            &key_path,
            certificate.signing_key.serialize_pem().as_bytes(),
        )
        .unwrap();
        // Satisfy the real ClientConfig::token contract before testing TLS.
        write_private(&token_path, b"0123456789abcdef0123456789abcdef").unwrap();
        let acceptor = airc::transport::acceptor(&airc::config::Tls {
            certificate: cert_path.clone(),
            private_key: key_path,
        })
        .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let proxy = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy_address = proxy.local_addr().unwrap();
        // JoinSet aborts owned tasks on drop (including a panic). Explicit
        // abort-and-join below preserves the original client error on failure.
        let mut tasks = tokio::task::JoinSet::new();
        tasks.spawn(async move {
            let (tcp, _) = listener.accept().await.context("server accept")?;
            let Ok(mut tls) = acceptor.accept(tcp).await else {
                // This outcome proves a connection actually reached TLS.
                return Ok::<_, anyhow::Error>(("server", false));
            };
            let mut header = Vec::new();
            while !header.ends_with(b"\r\n\r\n") {
                ensure!(header.len() < 8192, "server header limit");
                header.push(tls.read_u8().await.context("server request header")?);
            }
            ensure!(header.starts_with(b"GET /v1/artifacts/"), "artifact GET missing");
            tls.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 7\r\nContent-Disposition: attachment; filename=\"fixture\"\r\nConnection: close\r\n\r\nfixture")
                .await.context("server response")?;
            let _ = tls.shutdown().await;
            Ok(("server", true))
        });
        // An explicit loopback CONNECT proxy avoids ambient proxy settings;
        // the only forwarding destination is this test's owned listener.
        tasks.spawn(async move {
            let (mut downstream, _) = proxy.accept().await.context("proxy accept")?;
            let mut header = Vec::new();
            while !header.ends_with(b"\r\n\r\n") {
                ensure!(header.len() < 8192, "proxy header limit");
                header.push(downstream.read_u8().await.context("proxy CONNECT header")?);
            }
            ensure!(
                String::from_utf8(header)?
                    .starts_with(&format!("CONNECT {host}:{} HTTP/1.1\r\n", address.port())),
                "unexpected CONNECT authority"
            );
            let mut upstream = tokio::net::TcpStream::connect(address)
                .await
                .context("proxy upstream")?;
            downstream
                .write_all(b"HTTP/1.1 200 Connection established\r\n\r\n")
                .await
                .context("proxy response")?;
            // A rejected TLS handshake may close either side first.
            let _ = tokio::io::copy_bidirectional(&mut downstream, &mut upstream).await;
            Ok(("proxy", true))
        });
        let config = client::ClientConfig {
            host: "localhost".into(),
            port: address.port(),
            tls: true,
            allow_insecure: false,
            ca_certificate: trusted.then_some(cert_path),
            proxy: Some(format!("http://{proxy_address}")),
            account: "fixture".into(),
            token_file: token_path,
            nick: "fixture".into(),
            channels: vec![],
            artifact_url: Some(format!("https://{host}:{}", address.port())),
            unix: None,
            artifact_unix: None,
            regain: false,
        };
        let destination = dir.path().join("download");
        let mut phase = "download";
        eprintln!("HTTPS {case} phase={phase}");
        // Reserve one second of the unchanged ten-second total for cleanup.
        let outcome = tokio::time::timeout_at(deadline - Duration::from_secs(1), async {
            let result = client::download(
                &config,
                "12345678-1234-4234-8234-123456789012",
                &destination,
            )
            .await;
            phase = "certificate/result validation";
            eprintln!("HTTPS {case} phase={phase}");
            if expected {
                result.context("trusted artifact download")?;
                ensure!(
                    std::fs::read(&destination)? == b"fixture",
                    "wrong artifact bytes"
                );
            } else {
                let error = result.err().context("invalid certificate accepted")?;
                let certificate = certificate_error(error.as_ref());
                let intended = if trusted {
                    matches!(
                        certificate,
                        Some(
                            rustls::CertificateError::NotValidForName
                                | rustls::CertificateError::NotValidForNameContext { .. }
                        )
                    )
                } else {
                    matches!(certificate, Some(rustls::CertificateError::UnknownIssuer))
                };
                ensure!(intended, "wrong rejection reason: {error:?}");
                ensure!(!destination.exists(), "failed TLS created a destination");
            }
            phase = "fixture completion";
            eprintln!("HTTPS {case} phase={phase}");
            let mut completed = 0;
            while let Some(joined) = tasks.join_next().await {
                let (role, served) = joined.context("fixture task panicked")??;
                ensure!(
                    served == (role == "proxy" || expected),
                    "unexpected {role} TLS outcome"
                );
                completed += 1;
            }
            ensure!(completed == 2, "missing fixture task outcome");
            Ok::<_, anyhow::Error>(())
        })
        .await;
        // An early client/setup failure must not wait on unrelated accept().
        tasks.abort_all();
        let cleaned = tokio::time::timeout_at(deadline, async {
            while tasks.join_next().await.is_some() {}
        })
        .await;
        assert!(
            cleaned.is_ok(),
            "HTTPS {case} phase=cleanup exceeded total ten-second bound"
        );
        match outcome {
            Ok(result) => {
                result.unwrap_or_else(|error| panic!("HTTPS {case} phase={phase}: {error:?}"))
            }
            Err(error) => panic!("HTTPS {case} phase={phase} deadline: {error}"),
        }
    }
}
