//! Persistent IRC connection with a bounded, cursor-addressed JSONL frontend.
use crate::{
    multiline::{self, Assembler, Limits},
    protocol::{MAX_LINE, Message, fold, read_line, valid_channel, valid_nick},
    transport,
};
use anyhow::{Context, Result, bail, ensure};
use base64::{Engine, engine::general_purpose::STANDARD};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{BTreeSet, VecDeque},
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::{
    io::{AsyncBufRead, AsyncWrite, AsyncWriteExt, BufReader},
    sync::mpsc,
};

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ClientConfig {
    #[serde(default)]
    pub host: String,
    #[serde(default)]
    pub port: u16,
    #[serde(default = "yes")]
    pub tls: bool,
    #[serde(default)]
    pub allow_insecure: bool,
    pub ca_certificate: Option<PathBuf>,
    pub proxy: Option<String>,
    pub account: String,
    pub token_file: PathBuf,
    pub nick: String,
    #[serde(default)]
    pub channels: Vec<String>,
    pub artifact_url: Option<String>,
    /// Unix IRC endpoint; excludes host, port, proxy and TLS settings.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unix: Option<PathBuf>,
    /// Unix artifact endpoint; excludes `artifact_url`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_unix: Option<PathBuf>,
}
fn yes() -> bool {
    true
}

impl ClientConfig {
    pub fn load(path: &Path) -> Result<Self> {
        let mut config: Self = toml::from_str(&std::fs::read_to_string(path)?)?;
        let path = std::path::absolute(path)?;
        let parent = path.parent().unwrap_or(Path::new("/"));
        config.token_file = parent.join(&config.token_file);
        config.ca_certificate = config.ca_certificate.map(|p| parent.join(p));
        config.unix = config.unix.map(|p| parent.join(p));
        config.artifact_unix = config.artifact_unix.map(|p| parent.join(p));
        config.validate()?;
        Ok(config)
    }
    pub fn validate(&self) -> Result<()> {
        if let Some(proxy) = &self.proxy {
            transport::proxy_url(proxy)?;
        }
        ensure!(
            valid_nick(&self.nick) && valid_nick(&self.account),
            "invalid nick or account"
        );
        ensure!(
            self.channels.len() <= 128 && self.channels.iter().all(|c| valid_channel(c)),
            "invalid channels"
        );
        if let Some(path) = &self.unix {
            // Unix mode has no network fallback: every TCP setting is refused.
            ensure!(
                path.is_absolute()
                    && path.as_os_str().len() < 100
                    && self.host.is_empty()
                    && self.port == 0
                    && self.proxy.is_none()
                    && self.ca_certificate.is_none()
                    && !self.allow_insecure,
                "unix excludes host, port, proxy, ca_certificate and allow_insecure"
            );
        } else {
            ensure!(
                !self.host.is_empty() && self.port != 0,
                "configure host and port, or unix"
            );
            ensure!(
                self.tls
                    || self.allow_insecure
                    || self
                        .host
                        .parse::<std::net::IpAddr>()
                        .is_ok_and(|ip| ip.is_loopback()),
                "remote connections require TLS or explicit allow_insecure"
            );
        }
        if let Some(path) = &self.artifact_unix {
            ensure!(
                path.is_absolute()
                    && path.as_os_str().len() < 100
                    && self.unix.is_some()
                    && self.artifact_url.is_none(),
                "artifact_unix requires unix and excludes artifact_url"
            );
        }
        if let Some(url) = &self.artifact_url {
            let url = reqwest::Url::parse(url)?;
            ensure!(
                url.username().is_empty()
                    && url.password().is_none()
                    && url.path() == "/"
                    && url.query().is_none()
                    && url.fragment().is_none(),
                "artifact_url must be an origin"
            );
            ensure!(
                url.scheme() == "https"
                    || (url.scheme() == "http"
                        && (self.allow_insecure
                            || url.host_str().is_some_and(|h| h
                                .parse::<std::net::IpAddr>()
                                .is_ok_and(|ip| ip.is_loopback())))),
                "artifact URL requires HTTPS or explicit allow_insecure"
            );
        }
        Ok(())
    }
    fn token(&self) -> Result<String> {
        let token = std::fs::read_to_string(&self.token_file).context("read token file")?;
        let token = token.trim().to_owned();
        ensure!(
            token.len() >= 32 && token.len() <= 256 && !token.contains(['\r', '\n', '\0', ':']),
            "invalid token file"
        );
        Ok(token)
    }
}

pub struct Connection {
    /// Each item is written contiguously, so one multiline batch is never
    /// interleaved with keepalive replies.
    pub outbound: mpsc::Sender<Vec<Message>>,
    /// Inbound events; negotiated multiline batches arrive as one message.
    pub inbound: mpsc::Receiver<Message>,
    pub capabilities: BTreeSet<String>,
    /// Server multiline limits when draft/multiline was negotiated.
    pub multiline: Option<Limits>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Connection {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Connect without draft/multiline: other participants' batches arrive as the
/// server's fallback lines (JSONL version 1 behavior).
pub async fn connect(config: &ClientConfig) -> Result<Connection> {
    connect_with(config, false).await
}

/// Connect, negotiating draft/multiline when `multiline` is requested and the
/// server advertises limits within this client's bounds.
pub async fn connect_with(config: &ClientConfig, multiline: bool) -> Result<Connection> {
    config.validate()?;
    let token = config.token()?;
    tokio::time::timeout(
        Duration::from_secs(20),
        open_connection(config, &token, multiline, &config.channels, false),
    )
    .await
    .context("connection/authentication timed out")?
    .map(|startup| startup.connection)
}

pub(crate) struct JoinRefusal {
    pub channel: String,
    pub code: String,
    pub reason: String,
}

pub(crate) struct NamedStartup {
    pub connection: Connection,
    pub server_name: String,
    pub joined: Vec<String>,
    pub refusals: Vec<JoinRefusal>,
}

/// Named sessions may retain an authenticated connection when individual
/// followed channels refuse JOIN. Generic JSONL startup remains strict.
#[cfg(unix)]
pub(crate) async fn connect_named(
    config: &ClientConfig,
    channels: &[String],
) -> Result<NamedStartup> {
    config.validate()?;
    ensure!(
        channels.len() <= 128 && channels.iter().all(|c| valid_channel(c)),
        "invalid followed channels"
    );
    let unique = channels.iter().map(|c| fold(c)).collect::<BTreeSet<_>>();
    ensure!(unique.len() == channels.len(), "duplicate followed channel");
    let token = config.token()?;
    tokio::time::timeout(
        Duration::from_secs(20),
        open_connection(config, &token, true, channels, true),
    )
    .await
    .context("connection/authentication timed out")?
}

/// AIRC's authoritative connection prefix, not a display nick or client tag.
pub(crate) fn own_prefix(message: &Message, nick: &str, account: &str) -> bool {
    message
        .prefix
        .as_deref()
        .and_then(|prefix| prefix.split_once('!'))
        .is_some_and(|(sender, userhost)| {
            fold(sender) == fold(nick) && userhost == format!("{account}@airc")
        })
}

pub(crate) fn server_numeric(message: &Message, server: &str, nick: &str) -> bool {
    message.prefix.as_deref() == Some(server)
        && message
            .params
            .first()
            .is_some_and(|recipient| fold(recipient) == fold(nick))
}

async fn open_connection(
    config: &ClientConfig,
    token: &str,
    want_multiline: bool,
    channels: &[String],
    partial: bool,
) -> Result<NamedStartup> {
    let stream = match &config.unix {
        Some(path) => transport::connect_unix(path).await?,
        None => {
            transport::connect(
                &config.host,
                config.port,
                config.tls,
                config.ca_certificate.as_deref(),
                config.proxy.as_deref(),
            )
            .await?
        }
    };
    let (reader, mut writer) = tokio::io::split(stream);
    let mut reader = BufReader::new(reader);
    let mut capabilities = BTreeSet::new();
    let mut offered_multiline = None;
    write_message(&mut writer, Message::new("CAP", ["LS", "302"])).await?;
    write_message(&mut writer, Message::new("NICK", [&config.nick])).await?;
    write_message(
        &mut writer,
        Message::new("USER", [&config.account, "0", "*", "AIRC agent"]),
    )
    .await?;
    let server_name = loop {
        let line = read_line(&mut reader, MAX_LINE)
            .await?
            .context("connection closed during authentication")?;
        let m = Message::parse(&line)?;
        match m.command.as_str() {
            "CAP" if m.params.get(1).is_some_and(|s| s == "LS") => {
                let offered = m.params.last().context("invalid CAP response")?;
                ensure!(
                    offered
                        .split_whitespace()
                        .any(|s| s == "sasl" || s.starts_with("sasl=")),
                    "server lacks SASL"
                );
                offered_multiline = offered
                    .split_whitespace()
                    .find_map(|s| s.strip_prefix(&format!("{}=", multiline::CAP)))
                    .and_then(Limits::parse)
                    .filter(|_| want_multiline);
                let requested = [
                    "sasl",
                    "message-tags",
                    "server-time",
                    "batch",
                    "echo-message",
                    "account-tag",
                    "away-notify",
                    "draft/chathistory",
                    multiline::CAP,
                ]
                .into_iter()
                .filter(|c| {
                    (*c != multiline::CAP || offered_multiline.is_some())
                        && offered
                            .split_whitespace()
                            .any(|s| s.split('=').next() == Some(c))
                })
                .collect::<Vec<_>>()
                .join(" ");
                write_message(&mut writer, Message::new("CAP", ["REQ", &requested])).await?;
            }
            "CAP" if m.params.get(1).is_some_and(|s| s == "ACK") => {
                capabilities.extend(
                    m.params
                        .last()
                        .into_iter()
                        .flat_map(|s| s.split_whitespace())
                        .map(str::to_owned),
                );
                write_message(&mut writer, Message::new("AUTHENTICATE", ["PLAIN"])).await?;
            }
            "CAP" if m.params.get(1).is_some_and(|s| s == "NAK") => {
                bail!("capability negotiation rejected")
            }
            "AUTHENTICATE" if m.params.first().is_some_and(|s| s == "+") => {
                let encoded = STANDARD.encode(format!("\0{}\0{token}", config.account));
                for chunk in encoded.as_bytes().chunks(400) {
                    write_message(
                        &mut writer,
                        Message::new("AUTHENTICATE", [std::str::from_utf8(chunk)?]),
                    )
                    .await?;
                }
                if encoded.len() % 400 == 0 {
                    write_message(&mut writer, Message::new("AUTHENTICATE", ["+"])).await?;
                }
            }
            "903" => write_message(&mut writer, Message::new("CAP", ["END"])).await?,
            "001" => {
                // Named sessions correlate later numerics with this server name.
                let server = m.prefix.clone().unwrap_or_default();
                ensure!(
                    !partial
                        || (!server.is_empty()
                            && !server.contains(['!', '@'])
                            && server_numeric(&m, &server, &config.nick)),
                    "invalid registration identity"
                );
                break server;
            }
            "PING" => write_message(&mut writer, Message::new("PONG", m.params)).await?,
            "ERROR" | "432" | "433" | "464" | "904" | "905" | "906" => {
                bail!("IRC registration failed ({})", m.command)
            }
            _ => {}
        }
    };
    let negotiated = offered_multiline
        .filter(|_| capabilities.contains(multiline::CAP) && capabilities.contains("batch"));
    let mut assembler = negotiated.map(Assembler::new);
    let mut startup = Vec::new();
    let mut joined_channels = Vec::new();
    let mut refusals = Vec::new();
    for channel in channels {
        // Named startup bounds each automatic JOIN setup; generic startup keeps
        // only the aggregate connection timeout.
        let until = tokio::time::Instant::now() + Duration::from_secs(10);
        let mut self_joined = false;
        write_message(&mut writer, Message::new("JOIN", [channel])).await?;
        loop {
            let read = read_line(&mut reader, MAX_LINE);
            let line = if partial {
                tokio::time::timeout_at(until, read)
                    .await
                    .context("channel JOIN setup timed out")??
            } else {
                read.await?
            }
            .context("connection closed while joining")?;
            let message = Message::parse(&line)?;
            if message.command == "PING" {
                write_message(&mut writer, Message::new("PONG", message.params)).await?;
                continue;
            }
            let target = message
                .params
                .get(1)
                .is_some_and(|name| fold(name) == fold(channel));
            // Named setup also proves the numeric's server origin and recipient.
            let numeric =
                target && (!partial || server_numeric(&message, &server_name, &config.nick));
            let refused = partial && numeric && matches!(message.command.as_str(), "403" | "474");
            ensure!(
                (refused || message.command == "422" || !message.command.starts_with(['4', '5']))
                    && message.command != "ERROR",
                "channel join failed ({})",
                message.command
            );
            if refused {
                // The session records the refusal; the raw numeric is not replayed.
                refusals.push(JoinRefusal {
                    channel: channel.clone(),
                    code: message.command.clone(),
                    reason: message.params.get(2).cloned().unwrap_or_default(),
                });
                break;
            }
            self_joined |= partial
                && message.command == "JOIN"
                && own_prefix(&message, &config.nick, &config.account)
                && message
                    .params
                    .first()
                    .is_some_and(|name| fold(name) == fold(channel));
            let joined = message.command == "366" && numeric;
            ensure!(
                !partial || !joined || self_joined,
                "JOIN setup ended without an authentic self JOIN"
            );
            match &mut assembler {
                Some(assembler) => startup.extend(assembler.push(message)),
                None => startup.push(message),
            }
            ensure!(startup.len() <= 512, "startup event buffer exceeded");
            if joined {
                if partial {
                    joined_channels.push(channel.clone());
                }
                break;
            }
        }
    }
    let (outbound, mut outgoing) = mpsc::channel::<Vec<Message>>(32);
    let (incoming, inbound) = mpsc::channel(512);
    for message in startup {
        incoming.try_send(message)?;
    }
    let (pong, mut pongs) = mpsc::channel::<Message>(8);
    let task = tokio::spawn(async move {
        let input = async {
            while let Ok(Some(line)) = read_line(&mut reader, MAX_LINE).await {
                let Ok(m) = Message::parse(&line) else {
                    break;
                };
                if m.command == "PING" {
                    if pong.send(Message::new("PONG", m.params)).await.is_err() {
                        break;
                    }
                    continue;
                }
                let events = match &mut assembler {
                    Some(assembler) => assembler.push(m),
                    None => vec![m],
                };
                for event in events {
                    if incoming.send(event).await.is_err() {
                        return;
                    }
                }
            }
        };
        let output = async {
            loop {
                let frames = tokio::select! {
                    message = pongs.recv() => message.map(|m| vec![m]),
                    frames = outgoing.recv() => frames,
                };
                let Some(frames) = frames else {
                    break;
                };
                let written = tokio::time::timeout(Duration::from_secs(30), async {
                    for message in frames {
                        write_message(&mut writer, message).await?;
                    }
                    Ok::<_, anyhow::Error>(())
                })
                .await;
                if !matches!(written, Ok(Ok(()))) {
                    break;
                }
            }
        };
        tokio::select! { _ = input => {}, _ = output => {} }
    });
    Ok(NamedStartup {
        connection: Connection {
            outbound,
            inbound,
            capabilities,
            multiline: negotiated,
            task,
        },
        server_name,
        joined: joined_channels,
        refusals,
    })
}

async fn write_message(writer: &mut (impl AsyncWrite + Unpin), message: Message) -> Result<()> {
    writer.write_all(message.encode()?.as_bytes()).await?;
    Ok(())
}

#[derive(Debug, Deserialize)]
struct Request {
    id: Value,
    #[serde(flatten)]
    op: Operation,
}
#[derive(Debug, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
enum Operation {
    Read {
        #[serde(default)]
        after: u64,
        #[serde(default = "page")]
        limit: usize,
        #[serde(default)]
        wait_ms: u64,
    },
    Send {
        target: String,
        text: String,
        reply_to: Option<String>,
    },
    Join {
        channel: String,
    },
    Part {
        channel: String,
    },
    Topic {
        channel: String,
        #[serde(default, deserialize_with = "optional_text")]
        text: Option<String>,
    },
    Members {
        channel: String,
    },
    Who {
        channel: String,
    },
    Away {
        reason: String,
    },
    Back,
    History {
        channel: String,
        #[serde(default = "latest")]
        direction: String,
        #[serde(default = "star")]
        reference: String,
        #[serde(default = "page")]
        limit: usize,
    },
    Mode {
        channel: String,
        modes: String,
        nick: Option<String>,
    },
    Typing {
        channel: String,
        active: bool,
    },
    Upload {
        channel: String,
        path: PathBuf,
    },
    Download {
        artifact: String,
        path: PathBuf,
    },
    Quit,
}
fn page() -> usize {
    50
}
fn latest() -> String {
    "LATEST".into()
}
fn star() -> String {
    "*".into()
}

#[derive(Serialize)]
struct Event {
    cursor: u64,
    #[serde(flatten)]
    message: Message,
}

/// JSONL requests/replies only on stdout; IRC events remain in a bounded local ring.
pub async fn frontend(
    config: ClientConfig,
    input: impl AsyncBufRead + Unpin + Send + 'static,
    output: impl AsyncWrite + Unpin,
) -> Result<()> {
    frontend_with(config, 1, input, output).await
}

/// JSONL frontend. Version 1 is unchanged raw IRC events. Version 2 adds
/// assembled multiline messages, correlated send receipts and byte bounds.
pub async fn frontend_with(
    config: ClientConfig,
    version: u8,
    input: impl AsyncBufRead + Unpin + Send + 'static,
    mut output: impl AsyncWrite + Unpin,
) -> Result<()> {
    ensure!(matches!(version, 1 | 2), "JSONL version must be 1 or 2");
    let mut connection = connect_with(&config, version == 2).await?;
    let epoch = uuid::Uuid::new_v4().to_string();
    let mut ready = json!({"event":"ready","version":version,"epoch":epoch,"nick":config.nick,"account":config.account});
    if version == 2 {
        ready["multiline"] = json!(connection.multiline);
    }
    output.write_all(format!("{ready}\n").as_bytes()).await?;
    output.flush().await?;
    let (requests, mut incoming) = mpsc::channel(8);
    let bound = if version == 2 { V2_REQUEST } else { 32768 };
    let reader = tokio::spawn(async move {
        let mut input = input;
        loop {
            match read_line(&mut input, bound).await {
                Ok(Some(line)) => {
                    if requests.send(Ok(line)).await.is_err() {
                        break;
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    let _ = requests.send(Err(e)).await;
                    break;
                }
            }
        }
    });
    let result = if version == 2 {
        run_frontend_v2(&config, &epoch, &mut connection, &mut incoming, &mut output).await
    } else {
        run_frontend(&config, &epoch, &mut connection, &mut incoming, &mut output).await
    };
    reader.abort();
    result
}

/// Version 2 request line bound; a 256 KiB message survives JSON escaping.
const V2_REQUEST: usize = 2 * 1024 * 1024;
/// Version 2 retains at most this many serialized event bytes.
const V2_RING_BYTES: usize = 8 * 1024 * 1024;
/// A version 2 read page stops before this many bytes (always one event).
const V2_PAGE_BYTES: usize = 1024 * 1024;
/// Confirmation deadline for one send, extended by its line count.
const SEND_TIMEOUT: Duration = Duration::from_secs(10);

/// The one unresolved send of a version 2 frontend.
struct Pending {
    receipt: String,
    target: String,
    digest: String,
    until: tokio::time::Instant,
}

struct Ring {
    events: VecDeque<(u64, Value, usize)>,
    bytes: usize,
    cursor: u64,
}

impl Ring {
    fn push(&mut self, mut event: Value) {
        self.cursor += 1;
        event["cursor"] = json!(self.cursor);
        let size = event.to_string().len();
        self.bytes += size;
        self.events.push_back((self.cursor, event, size));
        while self.events.len() > 512 || (self.bytes > V2_RING_BYTES && self.events.len() > 1) {
            let (_, _, size) = self.events.pop_front().unwrap();
            self.bytes -= size;
        }
    }
    fn message(&mut self, message: Message) {
        let lines = message
            .params
            .get(1)
            .filter(|text| text.contains('\n') && message.command != "BATCH")
            .map(|text| text.split('\n').count());
        let mut event = serde_json::to_value(&message).unwrap_or(Value::Null);
        if let Some(lines) = lines {
            event["lines"] = json!(lines);
        }
        self.push(event);
    }
    fn receipt(&mut self, pending: &Pending, state: &str, detail: Value) {
        let mut event = json!({"event":"receipt","receipt":pending.receipt,"state":state,"target":pending.target});
        if let (Some(event), Some(detail)) = (event.as_object_mut(), detail.as_object()) {
            event.extend(detail.clone());
        }
        self.push(event);
    }
}

/// Server replies that reject the single unresolved PRIVMSG to `target`.
pub fn rejects(message: &Message, target: &str) -> bool {
    match message.command.as_str() {
        "401" | "403" | "404" => message
            .params
            .get(1)
            .is_some_and(|v| crate::protocol::fold(v) == crate::protocol::fold(target)),
        "417" => true,
        "FAIL" => match message.params.first().map(String::as_str) {
            Some("PRIVMSG") => message
                .params
                .get(2)
                .is_some_and(|v| crate::protocol::fold(v) == crate::protocol::fold(target)),
            Some("BATCH") => true,
            _ => false,
        },
        _ => false,
    }
}

/// True when `message` is this client's own echo of the pending send.
fn confirms(config: &ClientConfig, pending: &Pending, message: &Message) -> bool {
    use crate::protocol::fold;
    ["PRIVMSG", "NOTICE"].contains(&message.command.as_str())
        && message.params.len() == 2
        && message.tags.get(SEND_TAG) == Some(&pending.receipt)
        && message.tags.get("account") == Some(&config.account)
        && message
            .prefix
            .as_deref()
            .and_then(|p| p.split_once('!'))
            .is_some_and(|(nick, _)| fold(nick) == fold(&config.nick))
        && fold(&message.params[0]) == fold(&pending.target)
        && crate::config::token_hash_bytes(message.params[1].as_bytes()) == pending.digest
}

const SEND_TAG: &str = "+airc-send";

/// Record an inbound event; the receipt it resolves follows it in the ring.
fn observe(
    config: &ClientConfig,
    ring: &mut Ring,
    pending: &mut Option<Pending>,
    message: Message,
) {
    let outcome = pending.as_ref().and_then(|p| {
        if confirms(config, p, &message) {
            Some(("confirmed", json!({"msgid": message.tags.get("msgid")})))
        } else if rejects(&message, &p.target) {
            let reason = format!("{} {}", message.command, message.params.join(" "));
            Some(("rejected", json!({"reason": reason})))
        } else {
            None
        }
    });
    ring.message(message);
    // Only a correlated echo or rejection resolves the pending send.
    if let Some((state, detail)) = outcome
        && let Some(p) = pending.take()
    {
        ring.receipt(&p, state, detail);
    }
}

async fn run_frontend_v2(
    config: &ClientConfig,
    epoch: &str,
    connection: &mut Connection,
    incoming: &mut mpsc::Receiver<Result<String>>,
    output: &mut (impl AsyncWrite + Unpin),
) -> Result<()> {
    let mut ring = Ring {
        events: VecDeque::new(),
        bytes: 0,
        cursor: 0,
    };
    let mut pending: Option<Pending> = None;
    let failure = loop {
        let deadline = pending.as_ref().map(|p| p.until);
        let line = tokio::select! {
            message = connection.inbound.recv() => {
                match message {
                    Some(message) => observe(config, &mut ring, &mut pending, message),
                    None => break "IRC disconnected; reconnect explicitly",
                }
                continue;
            }
            _ = tokio::time::sleep_until(deadline.unwrap_or_else(tokio::time::Instant::now)), if deadline.is_some() => {
                break "send confirmation timed out; reconnect explicitly and inspect history before resending";
            }
            line = incoming.recv() => match line { Some(line) => line?, None => return Ok(()) },
        };
        let request = match serde_json::from_str::<Request>(&line) {
            Ok(request) => request,
            Err(_) => {
                write_json(
                    output,
                    &json!({"id":null,"ok":false,"error":"invalid request"}),
                )
                .await?;
                continue;
            }
        };
        if matches!(request.op, Operation::Quit) {
            write_json(output, &json!({"id":request.id,"ok":true})).await?;
            return Ok(());
        }
        let response = match request.op {
            Operation::Read {
                after,
                limit,
                wait_ms,
            } => {
                if limit == 0 || limit > 200 || wait_ms > 60_000 || after > ring.cursor {
                    Err(anyhow::anyhow!("invalid read bounds"))
                } else {
                    if ring.cursor <= after && wait_ms > 0 {
                        let mut until =
                            tokio::time::Instant::now() + Duration::from_millis(wait_ms);
                        if let Some(deadline) = deadline {
                            until = until.min(deadline);
                        }
                        if let Ok(message) =
                            tokio::time::timeout_at(until, connection.inbound.recv()).await
                        {
                            match message {
                                Some(message) => observe(config, &mut ring, &mut pending, message),
                                None => break "IRC disconnected; reconnect explicitly",
                            }
                        }
                    }
                    while let Ok(message) = connection.inbound.try_recv() {
                        observe(config, &mut ring, &mut pending, message);
                    }
                    let mut used = 0;
                    let page = ring
                        .events
                        .iter()
                        .filter(|(cursor, _, _)| *cursor > after)
                        .take(limit)
                        .take_while(|(_, _, size)| {
                            used += size;
                            used == *size || used <= V2_PAGE_BYTES
                        })
                        .collect::<Vec<_>>();
                    let next = page.last().map_or(after, |(cursor, _, _)| *cursor);
                    Ok(json!({
                        "epoch": epoch,
                        "events": page.iter().map(|(_, event, _)| event).collect::<Vec<_>>(),
                        "next": next,
                        "latest": ring.cursor,
                        "gap": ring.events.front().is_some_and(|(cursor, _, _)| after.saturating_add(1) < *cursor),
                    }))
                }
            }
            Operation::Send {
                target,
                text,
                reply_to,
            } => {
                if pending.is_some() {
                    Err(anyhow::anyhow!(
                        "not sent: another send awaits its receipt; retry after it resolves"
                    ))
                } else {
                    let receipt = uuid::Uuid::new_v4().simple().to_string();
                    let prepared = (|| {
                        ensure!(
                            valid_channel(&target) || valid_nick(&target),
                            "invalid target"
                        );
                        let mut tags = std::collections::BTreeMap::new();
                        tags.insert(SEND_TAG.to_owned(), receipt.clone());
                        if let Some(reply) = reply_to {
                            ensure!(
                                reply.len() <= 128 && !reply.contains(char::is_whitespace),
                                "invalid reply id"
                            );
                            tags.insert("+reply".into(), reply);
                        }
                        multiline::outbound(
                            (&config.nick, &config.account),
                            &target,
                            &text,
                            tags,
                            connection.multiline,
                            &format!("s{}", &receipt[..12]),
                        )
                    })();
                    match prepared {
                        Err(error) => Err(anyhow::anyhow!("not sent: {error:#}")),
                        Ok(frames) => {
                            let wait =
                                SEND_TIMEOUT + Duration::from_millis(12 * frames.len() as u64);
                            pending = Some(Pending {
                                receipt: receipt.clone(),
                                target,
                                digest: crate::config::token_hash_bytes(text.as_bytes()),
                                until: tokio::time::Instant::now() + wait,
                            });
                            if connection.outbound.send(frames).await.is_err() {
                                break "IRC disconnected while sending; the send is uncertain";
                            }
                            Ok(json!({"receipt": receipt, "state": "pending"}))
                        }
                    }
                }
            }
            Operation::Upload { channel, path } => upload(config, &channel, &path).await,
            Operation::Download { artifact, path } => download(config, &artifact, &path).await,
            operation => match to_message(operation) {
                Ok(message) => {
                    if connection.outbound.send(vec![message]).await.is_err() {
                        break "IRC disconnected; reconnect explicitly";
                    }
                    Ok(json!({"status":"queued","delivery":"server replies arrive through read"}))
                }
                Err(error) => Err(error),
            },
        };
        let reply = match response {
            Ok(result) => json!({"id":request.id,"ok":true,"result":result}),
            Err(error) => json!({"id":request.id,"ok":false,"error":error.to_string()}),
        };
        write_json(output, &reply).await?;
    };
    // The process ends here; report the unresolved send so it is never resent blindly.
    let uncertain = pending
        .iter()
        .map(|p| json!({"receipt": p.receipt, "target": p.target}))
        .collect::<Vec<_>>();
    write_json(
        output,
        &json!({"event":"closed","error":failure,"uncertain":uncertain}),
    )
    .await?;
    bail!("{failure}")
}

async fn run_frontend(
    config: &ClientConfig,
    epoch: &str,
    connection: &mut Connection,
    incoming: &mut mpsc::Receiver<Result<String>>,
    output: &mut (impl AsyncWrite + Unpin),
) -> Result<()> {
    let mut events = VecDeque::new();
    let mut cursor = 0;
    loop {
        let line = tokio::select! {
            message = connection.inbound.recv() => {
                let message = message.context("IRC disconnected; unsatisfied sends have unknown outcome; reconnect explicitly")?;
                push_event(&mut events, &mut cursor, message);
                continue;
            }
            line = incoming.recv() => match line { Some(line) => line?, None => return Ok(()) },
        };
        let request = match serde_json::from_str::<Request>(&line) {
            Ok(request) => request,
            Err(_) => {
                write_json(
                    output,
                    &json!({"id":null,"ok":false,"error":"invalid request"}),
                )
                .await?;
                continue;
            }
        };
        if matches!(request.op, Operation::Quit) {
            write_json(output, &json!({"id":request.id,"ok":true})).await?;
            return Ok(());
        }
        let response = match request.op {
            Operation::Read {
                after,
                limit,
                wait_ms,
            } => {
                if limit == 0 || limit > 200 || wait_ms > 60_000 || after > cursor {
                    Err(anyhow::anyhow!("invalid read bounds"))
                } else {
                    if cursor <= after
                        && wait_ms > 0
                        && let Ok(message) = tokio::time::timeout(
                            Duration::from_millis(wait_ms),
                            connection.inbound.recv(),
                        )
                        .await
                    {
                        push_event(
                            &mut events,
                            &mut cursor,
                            message.context("IRC disconnected; delivery may be unknown")?,
                        );
                    }
                    while let Ok(message) = connection.inbound.try_recv() {
                        push_event(&mut events, &mut cursor, message);
                    }
                    let page = events
                        .iter()
                        .filter(|e| e.cursor > after)
                        .take(limit)
                        .collect::<Vec<_>>();
                    let next = page.last().map_or(after, |e| e.cursor);
                    Ok(
                        json!({"epoch":epoch,"events":page,"next":next,"latest":cursor,
                        "gap":events.front().is_some_and(|e| after.saturating_add(1) < e.cursor)}),
                    )
                }
            }
            Operation::Upload { channel, path } => upload(config, &channel, &path).await,
            Operation::Download { artifact, path } => download(config, &artifact, &path).await,
            // Version 1 stays unchanged; the topic operation is version 2 only.
            Operation::Topic { .. } => Err(anyhow::anyhow!("topic requires JSONL version 2")),
            operation => match to_message(operation) {
                Ok(message) => {
                    connection
                        .outbound
                        .send(vec![message])
                        .await
                        .context("IRC disconnected; delivery may be unknown")?;
                    Ok(
                        json!({"status":"queued","delivery":"confirm through echo, history, or server reply"}),
                    )
                }
                Err(error) => Err(error),
            },
        };
        let reply = match response {
            Ok(result) => json!({"id":request.id,"ok":true,"result":result}),
            Err(error) => json!({"id":request.id,"ok":false,"error":error.to_string()}),
        };
        write_json(output, &reply).await?;
    }
}

async fn write_json(output: &mut (impl AsyncWrite + Unpin), value: &Value) -> Result<()> {
    output.write_all(format!("{value}\n").as_bytes()).await?;
    output.flush().await?;
    Ok(())
}
fn push_event(events: &mut VecDeque<Event>, cursor: &mut u64, message: Message) {
    *cursor += 1;
    events.push_back(Event {
        cursor: *cursor,
        message,
    });
    if events.len() > 512 {
        events.pop_front();
    }
}

/// Bound explicit availability before queuing. The server advertises AWAYLEN=200;
/// clearing it is a separate operation, so an empty reason cannot imply "back".
pub fn away_message(reason: Option<&str>) -> Result<Message> {
    if let Some(reason) = reason {
        ensure!(
            !reason.is_empty() && reason.len() <= 200,
            "away reason must be 1–200 UTF-8 bytes"
        );
    }
    let message = Message::new("AWAY", reason);
    message.encode()?;
    Ok(message)
}

/// Missing text queries, empty text clears; JSON null is never either action.
pub(crate) fn optional_text<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    String::deserialize(deserializer).map(Some)
}

pub fn topic_message(channel: &str, text: Option<&str>) -> Result<Message> {
    ensure!(valid_channel(channel), "invalid channel");
    if let Some(text) = text {
        ensure!(
            text.len() <= 200 && !text.contains(['\r', '\n', '\0']),
            "topic must be at most 200 UTF-8 bytes without CR/LF/NUL"
        );
    }
    let message = Message::new("TOPIC", std::iter::once(channel).chain(text));
    message.encode()?;
    Ok(message)
}

fn to_message(operation: Operation) -> Result<Message> {
    let m = match operation {
        Operation::Send {
            target,
            text,
            reply_to,
        } => {
            ensure!(
                valid_channel(&target) || valid_nick(&target),
                "invalid target"
            );
            let mut m = Message::new("PRIVMSG", [target, text]);
            if let Some(reply) = reply_to {
                ensure!(
                    reply.len() <= 128 && !reply.contains(char::is_whitespace),
                    "invalid reply id"
                );
                m.tags.insert("+reply".into(), reply);
            }
            m
        }
        Operation::Join { channel } => Message::new("JOIN", [channel]),
        Operation::Part { channel } => Message::new("PART", [channel]),
        Operation::Topic { channel, text } => topic_message(&channel, text.as_deref())?,
        Operation::Members { channel } => Message::new("NAMES", [channel]),
        Operation::Who { channel } => {
            ensure!(valid_channel(&channel), "invalid channel");
            Message::new("WHO", [channel])
        }
        Operation::Away { reason } => away_message(Some(&reason))?,
        Operation::Back => away_message(None)?,
        Operation::History {
            channel,
            direction,
            reference,
            limit,
        } => {
            ensure!(
                ["LATEST", "BEFORE", "AFTER", "AROUND"].contains(&direction.as_str())
                    && (1..=100).contains(&limit),
                "invalid history bounds"
            );
            Message::new(
                "CHATHISTORY",
                [direction, channel, reference, limit.to_string()],
            )
        }
        Operation::Mode {
            channel,
            modes,
            nick,
        } => {
            let mut p = vec![channel, modes];
            p.extend(nick);
            Message::new("MODE", p)
        }
        Operation::Typing { channel, active } => {
            let mut m = Message::new("TAGMSG", [channel]);
            m.tags.insert(
                "+typing".into(),
                if active { "active" } else { "done" }.into(),
            );
            m
        }
        _ => bail!("not an IRC operation"),
    };
    m.encode()?;
    Ok(m)
}

/// Artifact requests use the Unix endpoint when configured; its fixed origin
/// only names the request, it is never resolved.
const UNIX_ARTIFACT_ORIGIN: &str = "http://airc.local";

fn artifact_origin(config: &ClientConfig) -> Result<&str> {
    if config.artifact_unix.is_some() {
        return Ok(UNIX_ARTIFACT_ORIGIN);
    }
    config
        .artifact_url
        .as_deref()
        .context("artifact_url or artifact_unix not configured")
}

fn http_client(config: &ClientConfig) -> Result<reqwest::Client> {
    let mut builder = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .redirect(reqwest::redirect::Policy::none());
    #[cfg(unix)]
    if let Some(path) = &config.artifact_unix {
        return Ok(builder.no_proxy().unix_socket(path.as_path()).build()?);
    }
    if let Some(proxy) = &config.proxy {
        builder = builder.proxy(reqwest::Proxy::all(transport::proxy_url(proxy)?)?);
    }
    if let Some(path) = &config.ca_certificate {
        builder =
            builder.add_root_certificate(reqwest::Certificate::from_pem(&std::fs::read(path)?)?);
    }
    Ok(builder.build()?)
}
async fn upload(config: &ClientConfig, channel: &str, path: &Path) -> Result<Value> {
    ensure!(valid_channel(channel), "invalid channel");
    use tokio::io::AsyncReadExt;
    let file = tokio::fs::File::open(path).await?;
    let metadata = file.metadata().await?;
    ensure!(
        metadata.is_file() && metadata.len() <= 16 * 1024 * 1024,
        "upload requires a regular file of at most 16 MiB"
    );
    let mut bytes = Vec::new();
    file.take(16 * 1024 * 1024 + 1)
        .read_to_end(&mut bytes)
        .await?;
    ensure!(bytes.len() <= 16 * 1024 * 1024, "file too large");
    let filename = path
        .file_name()
        .and_then(|s| s.to_str())
        .context("invalid filename")?;
    let base = artifact_origin(config)?;
    let mut url = reqwest::Url::parse(base)?;
    url.path_segments_mut()
        .map_err(|_| anyhow::anyhow!("invalid artifact URL"))?
        .extend(["v1", "channels", channel, "artifacts"]);
    let response = http_client(config)?
        .post(url)
        .basic_auth(&config.account, Some(config.token()?))
        .header("x-airc-filename", filename)
        .body(bytes)
        .send()
        .await?
        .error_for_status()?;
    Ok(response.json().await?)
}
async fn download(config: &ClientConfig, artifact: &str, path: &Path) -> Result<Value> {
    uuid::Uuid::parse_str(artifact).context("artifact must be an ID, not a URL")?;
    let base = artifact_origin(config)?;
    let mut response = http_client(config)?
        .get(format!(
            "{}/v1/artifacts/{artifact}",
            base.trim_end_matches('/')
        ))
        .basic_auth(&config.account, Some(config.token()?))
        .send()
        .await?
        .error_for_status()?;
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        ensure!(
            bytes.len() + chunk.len() <= 16 * 1024 * 1024,
            "artifact too large"
        );
        bytes.extend_from_slice(&chunk);
    }
    crate::config::write_private(path, &bytes)?;
    Ok(json!({"bytes":bytes.len(),"sha256":crate::config::token_hash_bytes(&bytes)}))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unrelated_events_never_resolve_or_drop_the_pending_send() {
        let config: ClientConfig = toml::from_str(
            "host = \"127.0.0.1\"\nport = 6667\naccount = \"alice\"\nnick = \"alice\"\ntoken_file = \"t\"\n",
        )
        .unwrap();
        let mut ring = Ring {
            events: VecDeque::new(),
            bytes: 0,
            cursor: 0,
        };
        let text = "a\nb";
        let mut pending = Some(Pending {
            receipt: "r1".into(),
            target: "#space".into(),
            digest: crate::config::token_hash_bytes(text.as_bytes()),
            until: tokio::time::Instant::now(),
        });
        let mut join = Message::new("JOIN", ["#space"]);
        join.prefix = Some("bob!bob@airc".into());
        let mut other = Message::new("PRIVMSG", ["#space", text]);
        other.prefix = Some("bob!bob@airc".into());
        other.tags.insert("account".into(), "bob".into());
        other.tags.insert(SEND_TAG.into(), "r1".into());
        let unrelated = Message::new("404", ["alice", "#other", "Cannot send"]);
        for message in [join, other, unrelated] {
            observe(&config, &mut ring, &mut pending, message);
            assert!(
                pending.is_some(),
                "unrelated or forged events keep the send pending"
            );
        }
        let mut echo = Message::new("PRIVMSG", ["#SPACE", text]);
        echo.prefix = Some("Alice!alice@airc".into());
        echo.tags.insert("account".into(), "alice".into());
        echo.tags.insert(SEND_TAG.into(), "r1".into());
        echo.tags.insert("msgid".into(), "m1".into());
        observe(&config, &mut ring, &mut pending, echo);
        assert!(pending.is_none());
        let (_, receipt, _) = ring.events.back().unwrap();
        assert_eq!(receipt["state"], "confirmed");
        assert_eq!(receipt["msgid"], "m1");
        assert_eq!(ring.events.len(), 5, "every message is still recorded");
    }

    #[tokio::test]
    async fn cursor_pages_report_gaps_and_resume_without_skipping() {
        let config = ClientConfig {
            host: "127.0.0.1".into(),
            port: 0,
            tls: false,
            allow_insecure: false,
            ca_certificate: None,
            proxy: None,
            account: "alice".into(),
            token_file: "unused".into(),
            nick: "alice".into(),
            channels: vec![],
            artifact_url: None,
            unix: None,
            artifact_unix: None,
        };
        let (to_client, inbound) = mpsc::channel(16);
        let (outbound, _unused) = mpsc::channel(1);
        let mut connection = Connection {
            outbound,
            inbound,
            capabilities: BTreeSet::new(),
            multiline: None,
            task: tokio::spawn(std::future::pending()),
        };
        let (request, mut input) = mpsc::channel(1);
        let (mut output, response) = tokio::io::duplex(65536);
        let frontend = tokio::spawn(async move {
            run_frontend(
                &config,
                "test-epoch",
                &mut connection,
                &mut input,
                &mut output,
            )
            .await
        });
        for n in 0..600 {
            to_client
                .send(Message::new("NOTICE", ["#work".into(), n.to_string()]))
                .await
                .unwrap();
        }
        let mut response = BufReader::new(response);
        request
            .send(Ok(
                json!({"id":1,"op":"read","after":0,"limit":2}).to_string()
            ))
            .await
            .unwrap();
        let page: Value =
            serde_json::from_str(&read_line(&mut response, 65536).await.unwrap().unwrap()).unwrap();
        assert_eq!(page["result"]["gap"], true);
        assert_eq!(page["result"]["events"].as_array().unwrap().len(), 2);
        let next = page["result"]["next"].as_u64().unwrap();
        assert!(next < page["result"]["latest"].as_u64().unwrap());
        request
            .send(Ok(
                json!({"id":2,"op":"read","after":next,"limit":2}).to_string()
            ))
            .await
            .unwrap();
        let page: Value =
            serde_json::from_str(&read_line(&mut response, 65536).await.unwrap().unwrap()).unwrap();
        assert_eq!(page["result"]["events"][0]["cursor"], next + 1);
        assert_eq!(page["result"]["gap"], false);
        request
            .send(Ok(json!({"id":3,"op":"read","unknown":true}).to_string()))
            .await
            .unwrap();
        let page: Value =
            serde_json::from_str(&read_line(&mut response, 65536).await.unwrap().unwrap()).unwrap();
        assert_eq!(page["ok"], false);
        request
            .send(Ok(json!({"id":4,"op":"quit"}).to_string()))
            .await
            .unwrap();
        frontend.await.unwrap().unwrap();
    }
}

#[cfg(test)]
mod presence_tests {
    use super::*;

    #[test]
    fn topic_operation_distinguishes_query_clear_and_set_and_rejects_null() {
        let operation = |json: &str| serde_json::from_str::<Request>(json).map(|r| r.op);
        for (json, params) in [
            (r##"{"id":1,"op":"topic","channel":"#a"}"##, vec!["#a"]),
            (
                r##"{"id":2,"op":"topic","channel":"#a","text":""}"##,
                vec!["#a", ""],
            ),
            (
                r##"{"id":3,"op":"topic","channel":"#a","text":"Plan: ship"}"##,
                vec!["#a", "Plan: ship"],
            ),
        ] {
            let message = to_message(operation(json).unwrap()).unwrap();
            assert_eq!(message.command, "TOPIC");
            assert_eq!(message.params, params);
        }
        assert!(operation(r##"{"id":4,"op":"topic","channel":"#a","text":null}"##).is_err());
        assert!(operation(r##"{"id":5,"op":"topic","channel":"#a","extra":1}"##).is_err());
        for text in ["é".repeat(101), "a\rb".into(), "a\nb".into(), "a\0b".into()] {
            assert!(topic_message("#a", Some(&text)).is_err());
        }
        assert!(topic_message("not-a-channel", None).is_err());
        assert!(topic_message("#a", Some(&"é".repeat(100))).is_ok());
    }

    #[tokio::test]
    async fn jsonl_version_one_rejects_topic_without_queueing() {
        let config: ClientConfig = toml::from_str(
            "host = \"127.0.0.1\"\nport = 6667\naccount = \"alice\"\nnick = \"alice\"\ntoken_file = \"t\"\n",
        )
        .unwrap();
        let (_to_client, inbound) = mpsc::channel(1);
        let (outbound, mut written) = mpsc::channel(1);
        let mut connection = Connection {
            outbound,
            inbound,
            capabilities: BTreeSet::new(),
            multiline: None,
            task: tokio::spawn(std::future::pending()),
        };
        let (request, mut input) = mpsc::channel(2);
        let (mut output, response) = tokio::io::duplex(65536);
        let frontend = tokio::spawn(async move {
            run_frontend(&config, "epoch", &mut connection, &mut input, &mut output).await
        });
        request
            .send(Ok(r##"{"id":1,"op":"topic","channel":"#a"}"##.into()))
            .await
            .unwrap();
        request
            .send(Ok(r#"{"id":2,"op":"quit"}"#.into()))
            .await
            .unwrap();
        let mut response = BufReader::new(response);
        let reply: Value =
            serde_json::from_str(&read_line(&mut response, 65536).await.unwrap().unwrap()).unwrap();
        assert_eq!(reply["ok"], false, "{reply}");
        assert!(reply["error"].as_str().unwrap().contains("version 2"));
        frontend.await.unwrap().unwrap();
        assert!(written.try_recv().is_err(), "nothing was queued");
    }

    #[test]
    fn explicit_presence_and_roster_operations_validate_before_queueing() {
        for (json, command, params) in [
            (
                r#"{"op":"away","reason":"explicit claim"}"#,
                "AWAY",
                vec!["explicit claim"],
            ),
            (r#"{"op":"back"}"#, "AWAY", vec![]),
            (r##"{"op":"who","channel":"#work"}"##, "WHO", vec!["#work"]),
        ] {
            let operation: Operation = serde_json::from_str(json).unwrap();
            let message = to_message(operation).unwrap();
            assert_eq!(message.command, command);
            assert_eq!(message.params, params);
        }
        for reason in [
            "".to_owned(),
            "é".repeat(101),
            "line\nbreak".into(),
            "x\0y".into(),
            "x\ry".into(),
        ] {
            assert!(away_message(Some(&reason)).is_err());
        }
        assert!(away_message(Some(&"é".repeat(100))).is_ok());
        assert!(
            to_message(Operation::Who {
                channel: "not-a-channel".into()
            })
            .is_err()
        );
    }
}
