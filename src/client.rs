//! Persistent IRC connection with a bounded, cursor-addressed JSONL frontend.
use crate::{
    protocol::{MAX_LINE, Message, read_line, valid_channel, valid_nick},
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
    pub host: String,
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
}
fn yes() -> bool {
    true
}

impl ClientConfig {
    pub fn load(path: &Path) -> Result<Self> {
        let mut config: Self = toml::from_str(&std::fs::read_to_string(path)?)?;
        let parent = path.parent().unwrap_or(Path::new("."));
        config.token_file = parent.join(&config.token_file);
        config.ca_certificate = config.ca_certificate.map(|p| parent.join(p));
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
        ensure!(
            self.tls
                || self.allow_insecure
                || self
                    .host
                    .parse::<std::net::IpAddr>()
                    .is_ok_and(|ip| ip.is_loopback()),
            "remote connections require TLS or explicit allow_insecure"
        );
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
    pub outbound: mpsc::Sender<Message>,
    pub inbound: mpsc::Receiver<Message>,
    pub capabilities: BTreeSet<String>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Connection {
    fn drop(&mut self) {
        self.task.abort();
    }
}

pub async fn connect(config: &ClientConfig) -> Result<Connection> {
    config.validate()?;
    let token = config.token()?;
    tokio::time::timeout(Duration::from_secs(20), open_connection(config, &token))
        .await
        .context("connection/authentication timed out")?
}

async fn open_connection(config: &ClientConfig, token: &str) -> Result<Connection> {
    let stream = transport::connect(
        &config.host,
        config.port,
        config.tls,
        config.ca_certificate.as_deref(),
        config.proxy.as_deref(),
    )
    .await?;
    let (reader, mut writer) = tokio::io::split(stream);
    let mut reader = BufReader::new(reader);
    let mut capabilities = BTreeSet::new();
    write_message(&mut writer, Message::new("CAP", ["LS", "302"])).await?;
    write_message(&mut writer, Message::new("NICK", [&config.nick])).await?;
    write_message(
        &mut writer,
        Message::new("USER", [&config.account, "0", "*", "AIRC agent"]),
    )
    .await?;
    loop {
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
                let requested = [
                    "sasl",
                    "message-tags",
                    "server-time",
                    "batch",
                    "echo-message",
                    "account-tag",
                    "draft/chathistory",
                ]
                .into_iter()
                .filter(|c| {
                    offered
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
            "001" => break,
            "PING" => write_message(&mut writer, Message::new("PONG", m.params)).await?,
            "ERROR" | "432" | "433" | "464" | "904" | "905" | "906" => {
                bail!("IRC registration failed ({})", m.command)
            }
            _ => {}
        }
    }
    let mut startup = Vec::new();
    for channel in &config.channels {
        write_message(&mut writer, Message::new("JOIN", [channel])).await?;
        loop {
            let line = read_line(&mut reader, MAX_LINE)
                .await?
                .context("connection closed while joining")?;
            let message = Message::parse(&line)?;
            if message.command == "PING" {
                write_message(&mut writer, Message::new("PONG", message.params)).await?;
                continue;
            }
            ensure!(
                (message.command == "422" || !message.command.starts_with(['4', '5']))
                    && message.command != "ERROR",
                "channel join failed ({})",
                message.command
            );
            let joined = message.command == "366"
                && message.params.get(1).is_some_and(|name| {
                    crate::protocol::fold(name) == crate::protocol::fold(channel)
                });
            startup.push(message);
            ensure!(startup.len() <= 512, "startup event buffer exceeded");
            if joined {
                break;
            }
        }
    }
    let (outbound, mut outgoing) = mpsc::channel::<Message>(32);
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
                } else if incoming.send(m).await.is_err() {
                    break;
                }
            }
        };
        let output = async {
            loop {
                let message = tokio::select! {
                    message = pongs.recv() => message,
                    message = outgoing.recv() => message,
                };
                let Some(message) = message else {
                    break;
                };
                if !matches!(
                    tokio::time::timeout(
                        Duration::from_secs(10),
                        write_message(&mut writer, message)
                    )
                    .await,
                    Ok(Ok(()))
                ) {
                    break;
                }
            }
        };
        tokio::select! { _ = input => {}, _ = output => {} }
    });
    Ok(Connection {
        outbound,
        inbound,
        capabilities,
        task,
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
    Members {
        channel: String,
    },
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
    mut output: impl AsyncWrite + Unpin,
) -> Result<()> {
    let mut connection = connect(&config).await?;
    let epoch = uuid::Uuid::new_v4().to_string();
    output.write_all(format!("{}\n",json!({"event":"ready","version":1,"epoch":epoch,"nick":config.nick,"account":config.account})).as_bytes()).await?;
    output.flush().await?;
    let (requests, mut incoming) = mpsc::channel(8);
    let reader = tokio::spawn(async move {
        let mut input = input;
        loop {
            match read_line(&mut input, 32768).await {
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
    let result = run_frontend(&config, &epoch, &mut connection, &mut incoming, &mut output).await;
    reader.abort();
    result
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
            operation => match to_message(operation) {
                Ok(message) => {
                    connection
                        .outbound
                        .send(message)
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
        Operation::Members { channel } => Message::new("NAMES", [channel]),
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

fn http_client(config: &ClientConfig) -> Result<reqwest::Client> {
    let mut builder = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .redirect(reqwest::redirect::Policy::none());
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
    let base = config
        .artifact_url
        .as_deref()
        .context("artifact_url not configured")?;
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
    let base = config
        .artifact_url
        .as_deref()
        .context("artifact_url not configured")?;
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
        };
        let (to_client, inbound) = mpsc::channel(16);
        let (outbound, _unused) = mpsc::channel(1);
        let mut connection = Connection {
            outbound,
            inbound,
            capabilities: BTreeSet::new(),
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
