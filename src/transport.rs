use crate::config::Tls;
use anyhow::{Context, Result};
use std::{io::BufReader, sync::Arc};
use tokio::io::{AsyncRead, AsyncWrite};
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
) -> Result<Stream> {
    let tcp = tokio::net::TcpStream::connect((host, port)).await?;
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
