use crate::config::Tls;
use anyhow::{Context, Result, ensure};
use std::{io::BufReader, net::IpAddr, sync::Arc, time::Duration};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio_rustls::{TlsAcceptor, TlsConnector};

pub trait Io: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Io for T {}
pub type Stream = Box<dyn Io>;

pub fn acceptor(config: &Tls) -> Result<TlsAcceptor> {
    let certs = rustls_pemfile::certs(&mut BufReader::new(std::fs::File::open(
        &config.certificate,
    )?))
    .collect::<std::io::Result<Vec<_>>>()?;
    let key = rustls_pemfile::private_key(&mut BufReader::new(std::fs::File::open(
        &config.private_key,
    )?))?
    .context("no private key in TLS file")?;
    let config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)?;
    Ok(TlsAcceptor::from(Arc::new(config)))
}

pub async fn connect(
    host: &str,
    port: u16,
    tls: bool,
    ca: Option<&std::path::Path>,
    proxy: Option<&str>,
) -> Result<Stream> {
    tokio::time::timeout(
        Duration::from_secs(10),
        connect_inner(host, port, tls, ca, proxy),
    )
    .await
    .context("transport connection timed out")?
}

/// Connect to a local Unix endpoint. There is deliberately no TCP fallback.
pub async fn connect_unix(path: &std::path::Path) -> Result<Stream> {
    #[cfg(unix)]
    {
        let stream = tokio::time::timeout(
            Duration::from_secs(10),
            tokio::net::UnixStream::connect(path),
        )
        .await
        .context("Unix connection timed out")?
        .with_context(|| format!("connect {}", path.display()))?;
        Ok(Box::new(stream))
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        anyhow::bail!("Unix endpoints require a Unix host")
    }
}

async fn connect_inner(
    host: &str,
    port: u16,
    tls: bool,
    ca: Option<&std::path::Path>,
    proxy: Option<&str>,
) -> Result<Stream> {
    let tcp = if let Some(proxy) = proxy {
        let proxy = proxy_url(proxy)?;
        let proxy_host = proxy.host_str().context("proxy host missing")?;
        let proxy_host = proxy_host.trim_start_matches('[').trim_end_matches(']');
        let mut tcp =
            tokio::net::TcpStream::connect((proxy_host, proxy.port_or_known_default().unwrap()))
                .await?;
        let authority = authority(host, port)?;
        tcp.write_all(
            format!("CONNECT {authority} HTTP/1.1\r\nHost: {authority}\r\n\r\n").as_bytes(),
        )
        .await?;
        read_connect_reply(&mut tcp).await?;
        tcp
    } else {
        tokio::net::TcpStream::connect((host, port)).await?
    };
    tcp.set_nodelay(true)?;
    if !tls {
        return Ok(Box::new(tcp));
    }
    let mut roots = rustls::RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    if let Some(path) = ca {
        for cert in rustls_pemfile::certs(&mut BufReader::new(std::fs::File::open(path)?)) {
            roots.add(cert?)?;
        }
    }
    let config = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let name = rustls::pki_types::ServerName::try_from(host.to_owned())?;
    Ok(Box::new(
        TlsConnector::from(Arc::new(config))
            .connect(name, tcp)
            .await?,
    ))
}

// A proxy is a transport endpoint, never a credential-bearing URL or redirect.
// Keep it explicit: IRC does not silently inherit HTTP(S)_PROXY or fall back.
pub fn proxy_url(value: &str) -> Result<reqwest::Url> {
    let url = reqwest::Url::parse(value).context("invalid proxy URL")?;
    ensure!(
        url.scheme() == "http"
            && url.host_str().is_some()
            && url.username().is_empty()
            && url.password().is_none()
            && url.path() == "/"
            && url.query().is_none()
            && url.fragment().is_none()
            && url.port_or_known_default().is_some_and(|port| port != 0),
        "proxy must be an http origin without credentials, path, query or fragment"
    );
    Ok(url)
}

fn authority(host: &str, port: u16) -> Result<String> {
    if let Ok(ip) = host.parse::<IpAddr>() {
        return Ok(std::net::SocketAddr::new(ip, port).to_string());
    }
    ensure!(
        !host.is_empty()
            && host.len() <= 253
            && host
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || b".-_".contains(&c)),
        "invalid IRC host for proxy tunnel"
    );
    Ok(format!("{host}:{port}"))
}

async fn read_connect_reply(stream: &mut (impl AsyncRead + Unpin)) -> Result<()> {
    let mut header = Vec::new();
    // Read exactly the header: a server may send its first IRC bytes in the
    // same TCP segment as the proxy response. Those bytes belong to the caller.
    loop {
        ensure!(
            header.len() < 8192,
            "proxy response header exceeds 8192 bytes"
        );
        header.push(
            stream
                .read_u8()
                .await
                .context("incomplete proxy response")?,
        );
        if header.ends_with(b"\r\n\r\n") {
            break;
        }
    }
    let header = std::str::from_utf8(&header).context("invalid proxy response")?;
    let status: Vec<_> = header
        .lines()
        .next()
        .unwrap_or_default()
        .split_whitespace()
        .collect();
    ensure!(
        status.len() >= 2 && matches!(status[0], "HTTP/1.0" | "HTTP/1.1") && status[1] == "200",
        "proxy refused CONNECT or sent an invalid status"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proxy_and_authority_validation() {
        for url in [
            "http://127.0.0.1:3128",
            "http://proxy.example",
            "http://[::1]:8080",
        ] {
            assert!(proxy_url(url).is_ok(), "{url}");
        }
        for url in [
            "https://proxy",
            "http://user:secret@proxy",
            "http://proxy/path",
            "http://proxy/?q=x",
            "http://proxy/#x",
            "http://proxy:0",
            "socks5://proxy",
        ] {
            assert!(proxy_url(url).is_err(), "{url}");
        }
        assert_eq!(authority("::1", 6697).unwrap(), "[::1]:6697");
        assert_eq!(
            authority("chat.example", 6697).unwrap(),
            "chat.example:6697"
        );
        for host in [
            "",
            "chat\r\nInjected: yes",
            "chat/path",
            "user@chat",
            "chat:80",
        ] {
            assert!(authority(host, 6697).is_err());
        }
    }

    #[tokio::test]
    async fn connect_header_is_bounded_and_preserves_tunnel_bytes() {
        let mut reply =
            &b"HTTP/1.1 200 Connection Established\r\n\r\n:server NOTICE * :ready\r\n"[..];
        read_connect_reply(&mut reply).await.unwrap();
        assert_eq!(reply, b":server NOTICE * :ready\r\n");
        for bytes in [
            b"HTTP/1.1 403 Denied\r\n\r\n".as_slice(),
            b"HTTP/1.1 302 Redirect\r\n\r\n",
            b"HTTP/1.1 200 incomplete",
            b"garbage\r\n\r\n",
        ] {
            let mut bytes = bytes;
            assert!(read_connect_reply(&mut bytes).await.is_err());
        }
        assert!(read_connect_reply(&mut &[b'x'; 8193][..]).await.is_err());
    }

    #[tokio::test]
    async fn connect_uses_proxy_without_resolving_destination() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            while !request.ends_with(b"\r\n\r\n") {
                request.push(stream.read_u8().await.unwrap());
                assert!(request.len() < 512);
            }
            assert_eq!(request, b"CONNECT unresolvable.invalid:6667 HTTP/1.1\r\nHost: unresolvable.invalid:6667\r\n\r\n");
            for part in [b"HTTP/1.1 200 OK\r\n".as_slice(), b"\r\nPING :fixture\r\n"] {
                stream.write_all(part).await.unwrap();
            }
        });
        let mut stream = connect("unresolvable.invalid", 6667, false, None, Some(&proxy))
            .await
            .unwrap();
        let mut message = String::new();
        stream.read_to_string(&mut message).await.unwrap();
        assert_eq!(message, "PING :fixture\r\n");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn refused_proxy_does_not_fall_back_to_direct_connection() {
        let destination = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        destination.set_nonblocking(true).unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy = format!("http://{}", listener.local_addr().unwrap());
        let refusal = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            while !request.ends_with(b"\r\n\r\n") {
                request.push(stream.read_u8().await.unwrap());
                assert!(request.len() < 512);
            }
            stream
                .write_all(b"HTTP/1.1 403 Denied\r\n\r\n")
                .await
                .unwrap();
            assert_eq!(stream.read(&mut [0; 1]).await.unwrap(), 0);
        });
        assert!(
            connect(
                "127.0.0.1",
                destination.local_addr().unwrap().port(),
                false,
                None,
                Some(&proxy)
            )
            .await
            .is_err()
        );
        assert_eq!(
            destination.accept().unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
        refusal.await.unwrap();
    }
}
