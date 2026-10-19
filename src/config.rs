use crate::protocol::{fold, valid_channel, valid_nick};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet,
    net::SocketAddr,
    path::{Path, PathBuf},
};
use subtle::ConstantTimeEq;

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub server_name: String,
    pub listen: SocketAddr,
    pub database: PathBuf,
    #[serde(default)]
    pub allow_insecure: bool,
    #[serde(default)]
    pub allow_private_messages: bool,
    pub tls: Option<Tls>,
    #[serde(default)]
    pub limits: Limits,
    pub accounts: Vec<Account>,
    pub channels: Vec<Channel>,
    pub artifacts: Option<Artifacts>,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Tls {
    pub certificate: PathBuf,
    pub private_key: PathBuf,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Limits {
    pub connections: usize,
    pub outbound_queue: usize,
    pub history_per_channel: usize,
    pub history_page: usize,
    pub history_days: u32,
    /// Combined text bytes of one draft/multiline message.
    pub multiline_bytes: usize,
    /// PRIVMSG/NOTICE lines in one draft/multiline message.
    pub multiline_lines: usize,
    /// Encoded bytes queued for one connection before it is disconnected.
    pub outbound_bytes: usize,
    /// Encoded bytes of one CHATHISTORY page; a page always holds one message.
    pub history_page_bytes: usize,
}

/// Worst-case encoded size of one relayed multiline message: content, a
/// bounded per-line prefix/tag overhead, plus opening and closing BATCH lines.
pub fn multiline_wire_bytes(limits: &Limits) -> usize {
    limits.multiline_bytes + limits.multiline_lines * 256 + 2 * 4608
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            connections: 128,
            outbound_queue: 256,
            history_per_channel: 10_000,
            history_page: 100,
            history_days: 30,
            multiline_bytes: 64 * 1024,
            multiline_lines: 1024,
            outbound_bytes: 4 * 1024 * 1024,
            history_page_bytes: 1024 * 1024,
        }
    }
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Account {
    pub name: String,
    pub token_sha256: String,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Channel {
    pub name: String,
    pub members: Vec<String>,
    #[serde(default)]
    pub operators: Vec<String>,
    #[serde(default)]
    pub moderated: bool,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Artifacts {
    pub listen: SocketAddr,
    pub public_url: String,
    #[serde(default = "artifact_bytes")]
    pub max_bytes: usize,
    #[serde(default = "artifact_count")]
    pub per_channel: usize,
}
fn artifact_bytes() -> usize {
    4 * 1024 * 1024
}
fn artifact_count() -> usize {
    32
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let mut config: Self =
            toml::from_str(&std::fs::read_to_string(path).context("read server configuration")?)
                .context("parse server configuration")?;
        let parent = path.parent().unwrap_or(Path::new("."));
        config.database = parent.join(&config.database);
        if let Some(tls) = &mut config.tls {
            tls.certificate = parent.join(&tls.certificate);
            tls.private_key = parent.join(&tls.private_key);
        }
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<()> {
        ensure!(
            !self.server_name.is_empty()
                && self.server_name.len() <= 63
                && self
                    .server_name
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b".-".contains(&b)),
            "invalid server_name"
        );
        ensure!(
            self.listen.ip().is_loopback() || self.tls.is_some() || self.allow_insecure,
            "non-loopback IRC requires TLS or explicit allow_insecure"
        );
        ensure!(
            (1..=4096).contains(&self.limits.connections),
            "invalid connection limit"
        );
        ensure!(
            (8..=4096).contains(&self.limits.outbound_queue),
            "invalid outbound queue limit"
        );
        ensure!(
            (1..=500).contains(&self.limits.history_page)
                && self.limits.outbound_queue >= self.limits.history_page + 8,
            "history_page must fit outbound_queue with 8 slots spare"
        );
        ensure!(
            (1..=100_000).contains(&self.limits.history_per_channel)
                && (1..=3650).contains(&self.limits.history_days),
            "invalid history retention"
        );
        ensure!(
            (1024..=1024 * 1024).contains(&self.limits.multiline_bytes)
                && (2..=4096).contains(&self.limits.multiline_lines),
            "multiline_bytes must be 1 KiB-1 MiB and multiline_lines 2-4096"
        );
        ensure!(
            (64 * 1024..=64 * 1024 * 1024).contains(&self.limits.history_page_bytes)
                && self.limits.outbound_bytes <= 256 * 1024 * 1024
                && self.limits.outbound_bytes
                    >= 2 * multiline_wire_bytes(&self.limits) + self.limits.history_page_bytes,
            "outbound_bytes must hold two maximal multiline messages plus history_page_bytes"
        );
        let mut accounts = BTreeSet::new();
        for account in &self.accounts {
            ensure!(
                valid_nick(&account.name)
                    && account.name == fold(&account.name)
                    && accounts.insert(account.name.clone()),
                "account names must be unique, canonical IRC names"
            );
            ensure!(
                account.token_sha256.len() == 64
                    && account
                        .token_sha256
                        .bytes()
                        .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()),
                "invalid token hash for {}",
                account.name
            );
        }
        ensure!(!accounts.is_empty(), "at least one account required");
        let mut channels = BTreeSet::new();
        for channel in &self.channels {
            ensure!(
                valid_channel(&channel.name) && channels.insert(fold(&channel.name)),
                "invalid or duplicate channel"
            );
            ensure!(
                !channel.members.is_empty() && channel.members.iter().all(|a| accounts.contains(a)),
                "unknown or empty channel membership"
            );
            ensure!(
                channel
                    .operators
                    .iter()
                    .all(|a| channel.members.contains(a)),
                "operators must be channel members"
            );
        }
        ensure!(
            !channels.is_empty() && channels.len() <= 128,
            "configure 1 to 128 channels"
        );
        if let Some(a) = &self.artifacts {
            ensure!(
                a.listen.ip().is_loopback() || self.allow_insecure,
                "artifact HTTP requires loopback or explicit allow_insecure; use a TLS reverse proxy remotely"
            );
            let url = reqwest::Url::parse(&a.public_url).context("invalid artifact public_url")?;
            ensure!(
                ["http", "https"].contains(&url.scheme())
                    && url.host_str().is_some()
                    && url.username().is_empty()
                    && url.password().is_none()
                    && url.query().is_none()
                    && url.fragment().is_none()
                    && url.path() == "/"
                    && a.public_url.len() < 160
                    && !a.public_url.contains(char::is_whitespace),
                "public_url must be an HTTP(S) origin without credentials"
            );
            ensure!(
                url.scheme() == "https"
                    || self.allow_insecure
                    || url.host_str().is_some_and(|h| h == "localhost"
                        || h.parse::<std::net::IpAddr>()
                            .is_ok_and(|ip| ip.is_loopback())),
                "remote artifact URLs require HTTPS"
            );
            ensure!(
                (1..=16 * 1024 * 1024).contains(&a.max_bytes)
                    && (1..=1024).contains(&a.per_channel),
                "invalid artifact limits"
            );
        }
        Ok(())
    }

    pub fn authenticate(&self, name: &str, token: &str) -> Option<String> {
        let hash = token_hash(token);
        self.accounts
            .iter()
            .find(|a| {
                a.name == fold(name) && bool::from(a.token_sha256.as_bytes().ct_eq(hash.as_bytes()))
            })
            .map(|a| a.name.clone())
    }

    pub fn may_read(&self, account: &str, channel: &str) -> bool {
        self.channels
            .iter()
            .any(|c| fold(&c.name) == fold(channel) && c.members.iter().any(|a| a == account))
    }
}

pub fn token_hash(token: &str) -> String {
    format!("{:x}", Sha256::digest(token.as_bytes()))
}
pub fn token_hash_bytes(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

pub fn new_token() -> String {
    use rand::RngCore;
    let mut bytes = [0u8; 32];
    rand::rng().fill_bytes(&mut bytes);
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Credentials/configuration are created without overwriting anything, mode 0600.
pub fn write_private(path: &Path, bytes: &[u8]) -> Result<()> {
    use std::io::Write;
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}
