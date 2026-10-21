//! Named, explicitly started local IRC clients. IPC never carries credentials.
mod attention;
mod channels;
mod delivery;
mod history;
mod state;
mod storage;

use crate::{
    client::{self, ClientConfig},
    protocol::read_line,
};
use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};
use state::{SEND_TAG, State, identity, safe};
use std::{
    fs,
    os::unix::{fs::PermissionsExt, process::CommandExt},
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};
use storage::Storage;
use tokio::{
    io::{AsyncWriteExt, BufReader},
    net::{UnixListener, UnixStream},
    sync::{mpsc, oneshot},
    task::JoinSet,
    time::{Instant, timeout},
};

const VERSION: u32 = 2;
const MAX_RESPONSE: usize = 4 * 1024 * 1024;
/// A request carries at most one 256 KiB message, even fully JSON-escaped.
const MAX_REQUEST: usize = 2 * 1024 * 1024;
/// Echo deadline for one line; batches add time for the server's input pacing.
const SEND_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_SEND_WAIT: Duration = Duration::from_secs(50);

/// Which unread events wake `wait`. Every event stays readable regardless.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, clap::ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum Wake {
    /// Every recorded event, including membership changes.
    All,
    /// Others' messages and local problems; membership is quiet (default).
    #[default]
    Messages,
    /// Explicit @nick/@account or leading/group addresses, direct messages, replies to this
    /// session's messages, @all/@here, and local problems.
    Mentions,
}

/// Which command holds the delivery consumer connection.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConsumerKind {
    Wait,
    Subscribe,
}

impl Wake {
    fn mode(self) -> attention::WakeMode {
        match self {
            Self::All => attention::WakeMode::All,
            Self::Messages => attention::WakeMode::Messages,
            Self::Mentions => attention::WakeMode::Mentions,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum Operation {
    Wait {
        wait: u64,
        /// Older clients omit this and get the default.
        #[serde(default)]
        wake: Wake,
    },
    Read {
        wait: u64,
        limit: usize,
        again: bool,
    },
    Send {
        target: String,
        text: String,
        reply: Option<String>,
    },
    Away {
        reason: String,
    },
    Back,
    Status,
    Disconnect,
    /// Channel controls carry a caller-generated ID, bound before any network write.
    Join {
        channel: String,
        operation_id: String,
    },
    Part {
        channel: String,
        operation_id: String,
    },
    Topic {
        channel: String,
        operation_id: String,
        /// Absent queries, empty clears, other text sets; null is invalid.
        #[serde(
            default,
            deserialize_with = "client::optional_text",
            skip_serializing_if = "Option::is_none"
        )]
        text: Option<String>,
    },
    /// Register this connection as the single delivery consumer. Its later
    /// deliver/accept requests use the same connection; closing it releases.
    Consume {
        kind: ConsumerKind,
        /// The exact pending ID the operator authorized for one redelivery.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        retry: Option<String>,
    },
    Deliver {
        wait: u64,
        wake: Wake,
        max_bytes: usize,
        #[serde(default)]
        min_interval: u64,
    },
    /// The consumer's sink accepted this batch; from another connection, an
    /// operator's mark-accepted, refused while a consumer is registered.
    Accept {
        id: String,
    },
    /// Skip the pending batch without claiming delivery; refused while a consumer runs.
    DropPending {
        id: String,
    },
    Deliveries {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        show: Option<String>,
    },
    /// Print one record and up to `depth` reply ancestors, from the local ring
    /// first and then bounded server history. No cursor or transcript change.
    Show {
        reference: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        channel: Option<String>,
        depth: u8,
    },
}

impl Operation {
    fn control(&self) -> Option<channels::Request> {
        let (kind, channel, operation_id, topic) = match self {
            Self::Join {
                channel,
                operation_id,
            } => (channels::Kind::Join, channel, operation_id, None),
            Self::Part {
                channel,
                operation_id,
            } => (channels::Kind::Part, channel, operation_id, None),
            Self::Topic {
                channel,
                operation_id,
                text,
            } => (channels::Kind::Topic, channel, operation_id, text.clone()),
            _ => return None,
        };
        Some(channels::Request {
            operation_id: operation_id.clone(),
            kind,
            channel: channel.clone(),
            topic,
        })
    }
}

/// A fresh caller-side control ID: 128 random bits as lowercase hex.
pub fn operation_id() -> String {
    format!("{:032x}", rand::random::<u128>())
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    version: u32,
    operation: Operation,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Response {
    pub ok: bool,
    pub text: String,
    version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    ready: Option<bool>,
    /// The pending batch ID carried by a delivery.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    delivery: Option<String>,
}
impl Response {
    fn ok(text: impl Into<String>) -> Self {
        Self {
            ok: true,
            text: text.into(),
            version: VERSION,
            ready: None,
            delivery: None,
        }
    }
    fn error(text: impl Into<String>) -> Self {
        Self {
            ok: false,
            text: format!("! {}\n", safe(&text.into())),
            version: VERSION,
            ready: None,
            delivery: None,
        }
    }
    fn delivered(id: String, text: String) -> Self {
        Self {
            ready: Some(true),
            delivery: Some(id),
            ..Self::ok(text)
        }
    }
    fn readiness(ready: bool) -> Self {
        Self {
            ready: Some(ready),
            ..Self::ok("")
        }
    }
}

struct Call {
    /// The local connection; a consumer's later requests share it.
    conn: u64,
    pid: Option<i32>,
    operation: Operation,
    reply: oneshot::Sender<Response>,
}
enum Inbound {
    Call(Call),
    Closed(u64),
}
struct WaitingRead {
    until: Instant,
    limit: usize,
    reply: oneshot::Sender<Response>,
}
struct WaitingNotice {
    armed_at: chrono::DateTime<chrono::Utc>,
    until: Instant,
    wake: Wake,
    reply: oneshot::Sender<Response>,
}
fn waiter_status(waiter: Option<&WaitingNotice>) -> String {
    let Some(waiter) = waiter.filter(|w| !w.reply.is_closed() && w.until > Instant::now()) else {
        return "waiter: none\n".into();
    };
    let wake = match waiter.wake {
        Wake::All => "all",
        Wake::Messages => "messages",
        Wake::Mentions => "mentions",
    };
    let deadline = chrono::Utc::now()
        + chrono::Duration::from_std(waiter.until.saturating_duration_since(Instant::now()))
            .unwrap_or_default();
    format!(
        "waiter: armed wake={wake} since={} deadline={}\n",
        waiter
            .armed_at
            .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
        deadline.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
    )
}

struct Consumer {
    conn: u64,
    kind: ConsumerKind,
    pid: Option<i32>,
    since: chrono::DateTime<chrono::Utc>,
    last_request: chrono::DateTime<chrono::Utc>,
    retry: Option<String>,
    accepted_at: Option<Instant>,
}
fn consumer_status(consumer: Option<&Consumer>) -> String {
    let utc =
        |t: &chrono::DateTime<chrono::Utc>| t.to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    match consumer {
        None => "delivery consumer: none\n".into(),
        Some(c) => format!(
            "delivery consumer: {} pid={} since={} last_request={}\n",
            match c.kind {
                ConsumerKind::Wait => "wait",
                ConsumerKind::Subscribe => "subscribe",
            },
            c.pid.map_or("unknown".into(), |pid| pid.to_string()),
            utc(&c.since),
            utc(&c.last_request)
        ),
    }
}
fn pending_refusal(session: &str, id: &str, since: &str) -> String {
    format!(
        "delivery {id} pending since {since} (outcome uncertain); {}",
        recovery(session, id)
    )
}
fn recovery(session: &str, id: &str) -> String {
    format!(
        "inspect it with airc deliveries {session} --show {id}, then rerun with --retry-pending {id}, or run airc deliveries {session} --mark-accepted {id} | --drop {id}. Nothing was replayed."
    )
}
/// Bytes of `show` output before the chain stops (one record always fits).
const SHOW_BYTES: usize = 1024 * 1024;
const SHOW_DEPTH: u8 = 8;
const SHOW_TIMEOUT: Duration = Duration::from_secs(50);

/// A `show` walk over reply ancestors; newest record first until printed.
struct ShowWalk {
    remaining: u8,
    records: Vec<String>,
    bytes: usize,
    note: Option<String>,
}

enum ShowStep {
    Done(Response),
    Fetch { channel: String, msgid: String },
}

impl ShowWalk {
    /// Resolve references locally until done or one server fetch is needed.
    fn advance(
        &mut self,
        state: &mut State,
        mut next: Option<(String, Option<String>)>,
    ) -> ShowStep {
        while let Some((reference, channel)) = next.take() {
            let (local, msgid) = if reference.starts_with('@') {
                (
                    state.retained_record(&reference).map(str::to_owned),
                    state.resolve_reference(&reference).ok(),
                )
            } else {
                let msgid = reference.strip_prefix("msgid:").unwrap_or(&reference);
                if msgid.ends_with("…[truncated]") {
                    self.note = Some(format!(
                        "{reference}: the reply ID was truncated in the record and cannot be fetched"
                    ));
                    break;
                }
                let local = state
                    .local_reference(msgid)
                    .and_then(|r| state.retained_record(&r).map(str::to_owned));
                (local, Some(msgid.to_string()))
            };
            match (local, msgid, channel) {
                (Some(text), ..) => next = self.push(text),
                (None, Some(msgid), Some(channel)) if crate::protocol::valid_channel(&channel) => {
                    return ShowStep::Fetch { channel, msgid };
                }
                (None, Some(_), Some(target)) => {
                    self.note = Some(format!(
                        "{reference} is no longer retained locally and was a private message to {target}; private messages have no server history"
                    ));
                }
                (None, Some(_), None) => {
                    self.note = Some(format!(
                        "{reference} is no longer retained locally; give --channel to fetch it from server history"
                    ));
                }
                (None, None, _) => {
                    self.note = Some(format!(
                        "{reference} is unknown to this session; its reference expired"
                    ));
                }
            }
        }
        ShowStep::Done(self.finish())
    }

    /// Keep one record; return its parent while ancestors are still wanted.
    fn push(&mut self, text: String) -> Option<(String, Option<String>)> {
        let text = text.strip_prefix("history ").unwrap_or(&text).to_string();
        if !self.records.is_empty() && self.bytes + text.len() > SHOW_BYTES {
            self.note = Some(format!(
                "the chain stopped at the {SHOW_BYTES}-byte output bound"
            ));
            return None;
        }
        let (channel, reply) = state::record_links(&text);
        let parent = (self.remaining > 0)
            .then(|| reply.map(|r| (r.to_string(), channel.map(str::to_string))))
            .flatten();
        self.remaining = self.remaining.saturating_sub(1);
        self.bytes += text.len();
        self.records.push(text);
        parent
    }

    fn finish(&mut self) -> Response {
        let note = self.note.take();
        if self.records.is_empty() {
            return Response::error(note.unwrap_or_else(|| "nothing to show".into()));
        }
        let mut text = String::new();
        for record in self.records.iter().rev() {
            text.push_str("context ");
            text.push_str(record);
            text.push('\n');
        }
        if let Some(note) = note {
            text.push_str(&format!("! {}\n", safe(&note)));
        }
        Response::ok(text)
    }
}

struct WaitingShow {
    until: Instant,
    msgid: String,
    walk: ShowWalk,
    reply: oneshot::Sender<Response>,
}

/// The waiting show that requested exactly this msgid, if any.
fn claim_lookup(showing: &mut Option<WaitingShow>, msgid: &str) -> Option<WaitingShow> {
    showing.take_if(|pending| pending.msgid == msgid)
}

struct WaitingDelivery {
    conn: u64,
    until: Instant,
    wake: Wake,
    max_bytes: usize,
    min_interval: Duration,
    reply: oneshot::Sender<Response>,
}

struct WaitingSend {
    until: Instant,
    token: String,
    target: String,
    reply: oneshot::Sender<Response>,
}

struct WaitingAvailability {
    until: Instant,
    expected: &'static str,
    reason: Option<String>,
    reply: oneshot::Sender<Response>,
}

/// The pending channel control; its binding and intent are in saved state.
struct WaitingControl {
    until: Instant,
    reply: oneshot::Sender<Response>,
}

pub fn default_directory() -> Result<PathBuf> {
    if let Some(path) = std::env::var_os("XDG_STATE_HOME").filter(|v| !v.is_empty()) {
        return Ok(PathBuf::from(path).join("airc"));
    }
    Ok(
        PathBuf::from(std::env::var_os("HOME").context("HOME is unset; supply --state-dir")?)
            .join(".local/state/airc"),
    )
}

/// Start one worker, waiting for verified connection and bounded history recovery.
pub async fn connect(root: &Path, name: &str, config: &Path) -> Result<Response> {
    let config = fs::canonicalize(config)?;
    ClientConfig::load(&config)?;
    let store = Storage::open(root, name)?;
    // A quick ownership check avoids spawning a second worker. The worker takes
    // the same stable lock itself, closing the race with simultaneous connect.
    drop(store.lock()?);
    let mut command = std::process::Command::new(std::env::current_exe()?);
    command
        .arg("--state-dir")
        .arg(fs::canonicalize(root)?)
        .arg("session-worker")
        .arg(name)
        .arg("--config")
        .arg(config)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .process_group(0);
    let mut child = tokio::process::Command::from(command).spawn()?;
    let mut output = BufReader::new(child.stdout.take().context("worker output unavailable")?);
    let startup = async {
        let line = timeout(
            Duration::from_secs(45),
            read_line(&mut output, MAX_RESPONSE),
        )
        .await
        .context("session startup timed out; inspect status before reconnecting")??
        .context("session worker exited before readiness")?;
        let response: Response = serde_json::from_str(&line)?;
        ensure!(response.version == VERSION, "session IPC version mismatch");
        Ok::<_, anyhow::Error>(response)
    }
    .await;
    let response = match startup {
        Ok(response) => response,
        Err(e) => {
            let _ = child.kill().await;
            let _ = child.wait().await;
            return Err(e);
        }
    };
    if !response.ok {
        let _ = child.wait().await;
    }
    Ok(response)
}

/// One bounded local request. It is never automatically replayed.
pub async fn call(root: &Path, name: &str, operation: Operation) -> Result<Response> {
    validate(&operation)?;
    let store = Storage::open(root, name)?;
    let stream = match UnixStream::connect(store.socket()).await {
        Ok(stream) => stream,
        Err(e)
            if matches!(
                e.kind(),
                std::io::ErrorKind::NotFound | std::io::ErrorKind::ConnectionRefused
            ) =>
        {
            let ownership = store.lock();
            // An atomic snapshot is safe to inspect while a worker is starting
            // or finishing shutdown. It is not proof of a live connection.
            if ownership.is_err() && matches!(operation, Operation::Status) {
                return Ok(Response::ok(match store.load()? {
                    Some(state) => format!(
                        "! Session is starting/stopping or unavailable; saved state follows.\n{}waiter: unknown (worker unavailable)\n",
                        state.describe(true).replacen("connected", "unavailable", 1)
                    ),
                    None => "Session is starting; no saved state yet.\nwaiter: unknown (worker unavailable)\n".into(),
                }));
            }
            let _lock = ownership?;
            let state = store.load()?;
            return Ok(match (operation, state) {
                (Operation::Status, Some(state)) => Response::ok(format!(
                    "{}waiter: unknown (worker not running)\n",
                    state.describe(false)
                )),
                (Operation::Read { again: true, .. }, Some(state)) => Response::ok(state.last_read),
                (Operation::Deliveries { show }, Some(state)) => {
                    deliveries(&state, None, show.as_deref())
                }
                (Operation::Disconnect, _) => Response::ok("Already stopped.\n"),
                _ => Response::error(format!(
                    "session {name} is stopped; run airc connect {name} --config FILE. No request was sent."
                )),
            });
        }
        Err(e) => return Err(e.into()),
    };
    ensure!(
        stream.peer_cred()?.uid() == store.uid,
        "session socket owner mismatch"
    );
    let wait = match &operation {
        Operation::Read { wait, .. } | Operation::Wait { wait, .. } => *wait + 5,
        Operation::Send { .. } | Operation::Show { .. } => 60,
        Operation::Away { .. } | Operation::Back => 20,
        Operation::Join { .. } | Operation::Part { .. } | Operation::Topic { .. } => 20,
        _ => 5,
    };
    let control = operation.control().map(|request| request.operation_id);
    let uncertain = matches!(operation, Operation::Send { .. });
    let availability = matches!(operation, Operation::Away { .. } | Operation::Back);
    let disconnect = matches!(operation, Operation::Disconnect);
    let request = Request {
        version: VERSION,
        operation,
    };
    let result = timeout(Duration::from_secs(wait), async {
        let mut stream = BufReader::new(stream);
        write_json(stream.get_mut(), &request).await?;
        let line = read_line(&mut stream, MAX_RESPONSE)
            .await?
            .context("session closed before replying")?;
        let response: Response = serde_json::from_str(&line)?;
        ensure!(response.version == VERSION, "session IPC version mismatch");
        Ok(response)
    })
    .await;
    match result {
        Ok(Ok(response)) => {
            if disconnect && response.ok {
                timeout(Duration::from_secs(3), async {
                    loop {
                        if store.lock().is_ok() {
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                })
                .await
                .context("disconnect requested but worker has not exited; inspect status")?;
            }
            Ok(response)
        }
        error => {
            let detail = match error {
                Ok(Err(e)) => format!("{e:#}"),
                Err(e) => e.to_string(),
                _ => unreachable!(),
            };
            Ok(Response::error(format!(
                "{detail}; {}",
                if let Some(id) = control {
                    format!(
                        "channel control outcome unconfirmed for operation_id={id}; inspect airc status {name} for that operation before repeating it. Nothing was retried"
                    )
                } else if uncertain {
                    "send outcome uncertain. Inspect airc status and history before resending; nothing was retried".into()
                } else if availability {
                    "availability outcome unconfirmed; inspect WHO/WHOIS. Nothing was retried"
                        .into()
                } else {
                    "request was not retried; use read --again to recover a previous read".into()
                }
            )))
        }
    }
}

/// Wait for unread events without advancing the cursor or replacing read replay.
/// Each IPC wait is bounded; only successful idle responses renew the wait.
pub async fn wait(root: &Path, name: &str, seconds: Option<u64>, wake: Wake) -> Result<Response> {
    let deadline = seconds
        .map(|value| {
            Instant::now()
                .checked_add(Duration::from_secs(value))
                .context("wait timeout is too large")
        })
        .transpose()?;
    loop {
        let remaining = deadline.map(|end| end.saturating_duration_since(Instant::now()));
        // Round up so the last fractional second cannot become a busy poll.
        let interval = remaining.map_or(60, |left| {
            left.as_secs()
                .saturating_add(u64::from(left.subsec_nanos() != 0))
                .min(60)
        });
        let response = call(
            root,
            name,
            Operation::Wait {
                wait: interval,
                wake,
            },
        )
        .await?;
        if !response.ok {
            return Ok(response);
        }
        match response.ready {
            Some(true) => {
                return Ok(Response::ok(format!(
                    "Unread events available; run airc read {name}. No events consumed.\n"
                )));
            }
            Some(false) => {}
            None => bail!("session omitted wait readiness; no request was retried"),
        }
        if deadline.is_some_and(|end| Instant::now() >= end) {
            return Ok(Response::error("wait timed out; no events consumed"));
        }
    }
}

/// Settings shared by `wait --deliver` and `subscribe`.
pub struct DeliverOptions {
    pub wake: Wake,
    pub max_bytes: usize,
    pub min_interval: u64,
    /// `wait --deliver` only: stop idling after this many seconds.
    pub timeout: Option<u64>,
    /// The exact pending ID authorized for one redelivery.
    pub retry: Option<String>,
    pub sink_timeout: u64,
}

pub enum Sink {
    /// Print each batch and flush before accepting; `once` is `wait --deliver`.
    Stdout { once: bool },
    /// Run this argv (no shell) per batch with the batch on stdin.
    Exec(Vec<String>),
}

/// Bytes of a failing adapter's stderr kept for the diagnostic.
const SINK_STDERR: usize = 4096;
/// How long stderr may stay open after the adapter itself exited.
const SINK_STDERR_GRACE: Duration = Duration::from_millis(200);
pub const DEFAULT_MAX_BYTES: usize = delivery::DEFAULT_BYTES;

/// Hold the session's single delivery consumer connection, hand each batch to
/// the sink and accept it only after the sink took it. A sink failure leaves the
/// batch pending (uncertain) and ends the command; nothing is ever retried.
pub async fn deliver(
    root: &Path,
    name: &str,
    options: DeliverOptions,
    sink: Sink,
) -> Result<Response> {
    let request = |wait| Operation::Deliver {
        wait,
        wake: options.wake,
        max_bytes: options.max_bytes,
        min_interval: options.min_interval,
    };
    validate(&request(0))?;
    ensure!(
        (1..=3600).contains(&options.sink_timeout),
        "--sink-timeout requires 1–3600 seconds"
    );
    if let Sink::Exec(argv) = &sink {
        ensure!(
            !argv.is_empty(),
            "give an adapter command after --, or - for stdout"
        );
    }
    let kind = match sink {
        Sink::Stdout { once: true } => ConsumerKind::Wait,
        _ => ConsumerKind::Subscribe,
    };
    let store = Storage::open(root, name)?;
    let stream = match UnixStream::connect(store.socket()).await {
        Ok(stream) => stream,
        Err(e)
            if matches!(
                e.kind(),
                std::io::ErrorKind::NotFound | std::io::ErrorKind::ConnectionRefused
            ) =>
        {
            return Ok(Response::error(format!(
                "session {name} is stopped; run airc connect {name} --config FILE. Nothing was delivered."
            )));
        }
        Err(e) => return Err(e.into()),
    };
    ensure!(
        stream.peer_cred()?.uid() == store.uid,
        "session socket owner mismatch"
    );
    let mut stream = BufReader::new(stream);
    let registered = exchange(
        &mut stream,
        Operation::Consume {
            kind,
            retry: options.retry.clone(),
        },
        10,
    )
    .await?;
    if !registered.ok {
        return Ok(registered);
    }
    let deadline = options
        .timeout
        .map(|value| {
            Instant::now()
                .checked_add(Duration::from_secs(value))
                .context("wait timeout is too large")
        })
        .transpose()?;
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let mut interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
    let stopped = || {
        Response::error(format!(
            "stopped by signal; inspect airc deliveries {name} for a pending batch. Nothing was retried."
        ))
    };
    loop {
        let remaining = deadline.map(|end| end.saturating_duration_since(Instant::now()));
        let interval = remaining.map_or(60, |left| {
            left.as_secs()
                .saturating_add(u64::from(left.subsec_nanos() != 0))
                .min(60)
        });
        let response = tokio::select! {
            response = exchange(&mut stream, request(interval), interval + 5) => response,
            _ = terminate.recv() => return Ok(stopped()),
            _ = interrupt.recv() => return Ok(stopped()),
        };
        let response = match response {
            Ok(response) => response,
            Err(e) => {
                return Ok(Response::error(format!(
                    "{e:#}; a batch may be pending; inspect airc deliveries {name}. Nothing was retried."
                )));
            }
        };
        if !response.ok {
            return Ok(response);
        }
        let Some(id) = response.delivery.clone() else {
            match response.ready {
                Some(false) if deadline.is_some_and(|end| Instant::now() >= end) => {
                    return Ok(Response::error("wait timed out; no events consumed"));
                }
                Some(false) => continue,
                _ => bail!("session omitted delivery readiness; no request was retried"),
            }
        };
        let retry = options.retry.as_deref() == Some(id.as_str());
        let handed = match &sink {
            Sink::Stdout { .. } => write_stdout(&response.text),
            Sink::Exec(argv) => tokio::select! {
                result = run_sink(argv, name, &id, retry, &response.text, options.sink_timeout) => result,
                _ = terminate.recv() => Err(anyhow::anyhow!("stopped by signal; the adapter was killed")),
                _ = interrupt.recv() => Err(anyhow::anyhow!("stopped by signal; the adapter was killed")),
            },
        };
        if let Err(e) = handed {
            return Ok(Response::error(format!(
                "delivery {id} not accepted: {e:#}; {}",
                recovery(name, &id)
            )));
        }
        let accepted = match exchange(&mut stream, Operation::Accept { id: id.clone() }, 10).await {
            Ok(accepted) => accepted,
            Err(e) => {
                return Ok(Response::error(format!(
                    "{e:#}; acceptance of {id} is unconfirmed; inspect airc deliveries {name}. Nothing was retried."
                )));
            }
        };
        if !accepted.ok || matches!(sink, Sink::Stdout { once: true }) {
            return Ok(if accepted.ok {
                Response::ok("")
            } else {
                accepted
            });
        }
    }
}

/// One request/reply on a held consumer connection.
async fn exchange(
    stream: &mut BufReader<UnixStream>,
    operation: Operation,
    seconds: u64,
) -> Result<Response> {
    let request = Request {
        version: VERSION,
        operation,
    };
    timeout(Duration::from_secs(seconds), async {
        write_json(stream.get_mut(), &request).await?;
        let line = read_line(stream, MAX_RESPONSE)
            .await?
            .context("session closed before replying")?;
        let response: Response = serde_json::from_str(&line)?;
        ensure!(response.version == VERSION, "session IPC version mismatch");
        Ok(response)
    })
    .await
    .context("session did not reply in time")?
}

fn write_stdout(text: &str) -> Result<()> {
    use std::io::Write;
    let mut out = std::io::stdout().lock();
    out.write_all(text.as_bytes())?;
    out.flush()?;
    Ok(())
}

/// Run one adapter: argv without a shell, the batch on stdin, then EOF. Exit 0
/// in time means the sink accepted it. The direct child is killed on timeout or
/// when this future is dropped; anything it started itself is its own concern.
async fn run_sink(
    argv: &[String],
    session: &str,
    id: &str,
    retry: bool,
    text: &str,
    limit: u64,
) -> Result<()> {
    use tokio::io::AsyncReadExt;
    let (program, args) = argv.split_first().context("adapter command is empty")?;
    let mut child = tokio::process::Command::new(program)
        .args(args)
        .env("AIRC_SESSION", session)
        .env("AIRC_DELIVERY_ID", id)
        .env("AIRC_DELIVERY_RETRY", if retry { "1" } else { "0" })
        .env("AIRC_SUBSCRIBER_PID", std::process::id().to_string())
        .stdin(Stdio::piped())
        .stdout(Stdio::inherit())
        .stderr(Stdio::piped())
        .process_group(0)
        .kill_on_drop(true)
        .spawn()
        .with_context(|| format!("adapter {} did not start", safe(program)))?;
    let mut stdin = child.stdin.take().context("adapter stdin unavailable")?;
    let mut stderr = child.stderr.take().context("adapter stderr unavailable")?;
    let run = async {
        // Drain stderr concurrently so a chatty adapter cannot block on it.
        let mut captured = Vec::new();
        let (written, status) = {
            let errors = async {
                let mut chunk = [0u8; 4096];
                loop {
                    match stderr.read(&mut chunk).await {
                        Ok(0) | Err(_) => break,
                        Ok(n) => {
                            captured.extend_from_slice(&chunk[..n]);
                            let excess = captured.len().saturating_sub(SINK_STDERR);
                            captured.drain(..excess);
                        }
                    }
                }
            };
            let finished = async {
                let written = stdin.write_all(text.as_bytes()).await;
                drop(stdin);
                (written, child.wait().await)
            };
            tokio::pin!(errors, finished);
            let mut drained = false;
            let outcome = loop {
                tokio::select! {
                    outcome = &mut finished => break outcome,
                    () = &mut errors, if !drained => drained = true,
                }
            };
            // A process the adapter left behind may hold stderr open; its exit
            // status, not that pipe's EOF, decides the outcome.
            if !drained {
                let _ = timeout(SINK_STDERR_GRACE, &mut errors).await;
            }
            outcome
        };
        let status = status?;
        // Response::error escapes control bytes once, at the boundary.
        let tail = String::from_utf8_lossy(&captured).trim_end().to_string();
        let detail = if tail.is_empty() {
            String::new()
        } else {
            format!("; adapter stderr: {tail}")
        };
        ensure!(status.success(), "adapter exited with {status}{detail}");
        written.with_context(|| format!("adapter did not read the whole batch{detail}"))?;
        Ok(())
    };
    match timeout(Duration::from_secs(limit), run).await {
        Ok(result) => result,
        Err(_) => {
            let _ = child.start_kill();
            bail!("adapter exceeded {limit} s and was killed")
        }
    }
}

fn validate(operation: &Operation) -> Result<()> {
    if let Operation::Away { reason } = operation {
        client::away_message(Some(reason))?;
    }
    if let Operation::Wait { wait, .. } = operation {
        ensure!(*wait <= 60, "internal wait requires 0–60 seconds");
    }
    if let Operation::Read { wait, limit, again } = operation {
        ensure!(
            *wait <= 60 && (1..=200).contains(limit),
            "read requires --wait 0–60 and --limit 1–200"
        );
        ensure!(!again || *wait == 0, "--again cannot wait");
    }
    if let Operation::Send { text, .. } = operation {
        ensure!(
            text.len() <= crate::multiline::CLIENT_MAX_BYTES,
            "message exceeds {} bytes; share larger content as an artifact",
            crate::multiline::CLIENT_MAX_BYTES
        );
    }
    if let Some(request) = operation.control() {
        request.validate()?;
    }
    if let Operation::Deliver {
        wait,
        max_bytes,
        min_interval,
        ..
    } = operation
    {
        ensure!(
            *wait <= 60
                && (delivery::MIN_BYTES..=delivery::MAX_BYTES).contains(max_bytes)
                && *min_interval <= 3600,
            "delivery requires --max-bytes {}–{} and --min-interval 0–3600",
            delivery::MIN_BYTES,
            delivery::MAX_BYTES
        );
    }
    if let Operation::Show {
        reference,
        channel,
        depth,
    } = operation
    {
        ensure!(
            !reference.is_empty()
                && reference.len() <= 160
                && !reference
                    .chars()
                    .any(|c| c.is_whitespace() || c.is_control())
                && *depth <= SHOW_DEPTH
                && channel
                    .as_deref()
                    .is_none_or(crate::protocol::valid_channel),
            "show needs @N or a msgid, an optional valid --channel and --chain 1–{SHOW_DEPTH}"
        );
    }
    match operation {
        Operation::Accept { id }
        | Operation::DropPending { id }
        | Operation::Consume {
            retry: Some(id), ..
        }
        | Operation::Deliveries { show: Some(id) } => delivery::validate_id(id)?,
        _ => {}
    }
    Ok(())
}

/// The delivery summary, or one pending batch's text for inspection only.
fn deliveries(state: &State, consumer: Option<&Consumer>, show: Option<&str>) -> Response {
    match show {
        None => Response::ok(format!(
            "{}{}",
            state.describe_delivery(),
            consumer_status(consumer)
        )),
        Some(id) => match state.pending_delivery() {
            Some(batch) if batch.id == id => Response::ok(batch.render(false)),
            _ => Response::error(format!("no pending delivery {id}")),
        },
    }
}

/// Internal worker entry point. Its stdout is only a bounded startup response.
pub async fn worker(root: &Path, name: &str, config: &Path) -> Result<()> {
    let started = async {
        let store = Storage::open(root, name)?;
        let lock = store.lock()?;
        let config = ClientConfig::load(config)?;
        let mut state = store.load()?.unwrap_or(State::new(&config)?);
        state.begin_epoch();
        // The ORIGINAL configuration is the identity; only connection setup
        // uses the saved followed channels.
        ensure!(state.identity == identity(&config)?, "session config changed; use a new session name to preserve old recovery state");
        state.upgrade(&config)?;
        let startup = client::connect_named(&config, &state.startup_channels()).await?;
        let mut connection = startup.connection;
        ensure!(["message-tags", "account-tag", "echo-message", "batch", "draft/chathistory"].iter()
            .all(|cap| connection.capabilities.contains(*cap)), "named sessions need message-tags, account-tag, echo-message, batch and draft/chathistory");
        if state.status.starts_with("connected") {
            state.notice("Previous connection ended without a recorded shutdown; pending sends may be uncertain.");
        }
        state.epoch = uuid::Uuid::new_v4().to_string();
        state.status = "connected; no automatic reconnect or resend".into();
        state.context(format!("connected as {}({}); epoch={}; history below is replay", config.nick, config.account, state.epoch));
        state.start_channels(&startup.server_name, &startup.joined, &startup.refusals);
        history::recover(&mut connection, &mut state, &startup.joined).await?;
        store.save(&state)?;
        match fs::remove_file(store.socket()) {
            Ok(()) => {}, Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}, Err(e) => return Err(e.into()),
        }
        let listener = UnixListener::bind(store.socket())?;
        fs::set_permissions(store.socket(), fs::Permissions::from_mode(0o600))?;
        Ok::<_, anyhow::Error>((store, lock, state, connection, listener))
    }.await;
    let (store, _lock, mut state, mut connection, listener) = match started {
        Ok(value) => value,
        Err(e) => {
            write_json(
                &mut tokio::io::stdout(),
                &Response::error(format!("connect failed: {e:#}")),
            )
            .await?;
            return Ok(());
        }
    };
    // If the initiating CLI died before readiness, do not leave an invisible
    // connection behind. After readiness, this client lives until disconnect.
    write_json(
        &mut tokio::io::stdout(),
        &Response::ok(format!(
            "Connected {name} as {}({}). Use airc read {name} for history and events.\n",
            safe(&state.nick),
            safe(&state.account)
        )),
    )
    .await?;
    let result = serve(name, &store, &mut state, &mut connection, listener).await;
    drop(connection);
    if let Err(e) = &result {
        state.status = format!("stopped: {e:#}; pending sends may be uncertain");
        state.notice(state.status.clone());
    }
    let saved = store.save(&state);
    let _ = fs::remove_file(store.socket());
    result.and(saved)
}

async fn serve(
    session: &str,
    store: &Storage,
    state: &mut State,
    connection: &mut client::Connection,
    listener: UnixListener,
) -> Result<()> {
    let (requests, mut request_rx) = mpsc::channel::<Inbound>(16);
    let mut clients = JoinSet::new();
    let mut next_conn = 0u64;
    let mut reader: Option<WaitingRead> = None;
    let mut waiter: Option<WaitingNotice> = None;
    let mut consumer: Option<Consumer> = None;
    let mut delivering: Option<WaitingDelivery> = None;
    let mut showing: Option<WaitingShow> = None;
    let mut sender: Option<WaitingSend> = None;
    let mut availability: Option<WaitingAvailability> = None;
    let mut control: Option<WaitingControl> = None;
    let mut tick = tokio::time::interval(Duration::from_millis(50));
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let mut interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
    loop {
        tokio::select! {
            _ = terminate.recv() => { state.status = "disconnected by signal; inspect send receipts before retrying".into(); break; }
            _ = interrupt.recv() => { state.status = "disconnected by signal; inspect send receipts before retrying".into(); break; }
            accepted = listener.accept(), if clients.len() < 16 => {
                let (stream, _) = accepted?;
                if stream.peer_cred()?.uid() == store.uid {
                    let tx = requests.clone();
                    next_conn += 1;
                    let conn = next_conn;
                    clients.spawn(async move { let _ = handle(stream, tx, conn).await; });
                }
            }
            Some(_) = clients.join_next(), if !clients.is_empty() => {},
            message = connection.inbound.recv() => {
                let Some(message) = message else {
                    state.status = "IRC disconnected; reconnect explicitly; pending sends are uncertain".into();
                    state.notice(state.status.clone());
                    break;
                };
                if availability.as_ref().is_some_and(|pending|
                    message.command == pending.expected
                        && message.params.first().is_some_and(|nick| crate::protocol::fold(nick) == crate::protocol::fold(&state.nick))
                ) {
                    let pending = availability.take().unwrap();
                    let text = match pending.reason {
                        Some(reason) => format!("confirmed away: {}", safe(&reason)),
                        None => "confirmed back (not marked away)".into(),
                    };
                    state.context(text.clone());
                    store.save(state)?;
                    let _ = pending.reply.send(Response::ok(format!("{text}\n")));
                } else if availability.is_some()
                    && matches!(message.command.as_str(), "421" | "461")
                    && message.params.get(1).is_some_and(|command| command == "AWAY")
                {
                    let pending = availability.take().unwrap();
                    let _ = pending.reply.send(Response::error(format!("availability rejected by server {}", message.command)));
                }
                let previous_sequence = state.sequence();
                let (confirmed, settled) = state.ingest_live(&message);
                if let Some((ok, text)) = settled
                    && let Some(pending) = control.take()
                {
                    // Confirmation or refusal is durable before the IPC reply.
                    store.save(state)?;
                    let _ = pending.reply.send(if ok { Response::ok(format!("{}\n", safe(&text))) } else { Response::error(text) });
                }
                if sender.as_ref().is_some_and(|s| confirmed.as_deref() == Some(s.token.as_str())) {
                    let sent = sender.take().unwrap();
                    store.save(state)?;
                    let _ = sent.reply.send(Response::ok(format!("{}\n", safe(&state.outcome(&sent.token)))));
                } else if let Some(pending) = &sender && client::rejects(&message, &pending.target) {
                    let sent = sender.take().unwrap();
                    state.set_outcome(&sent.token, format!("rejected by server {} {}", message.command, message.params.join(" ")));
                    store.save(state)?;
                    let _ = sent.reply.send(Response::error(state.outcome(&sent.token)));
                } else if state.take_dirty() || confirmed.is_some() || state.sequence() != previous_sequence {
                    // Persist changed state; echo/rejection paths already saved
                    // above. Typing/setup numerics require no filesystem writes.
                    store.save(state)?;
                }
            }
            Some(inbound) = request_rx.recv() => {
                let call = match inbound {
                    Inbound::Closed(conn) => {
                        // EOF or death of the consumer process releases its lease.
                        if consumer.as_ref().is_some_and(|c| c.conn == conn) {
                            consumer = None;
                        }
                        if delivering.as_ref().is_some_and(|d| d.conn == conn) {
                            delivering = None;
                        }
                        continue;
                    }
                    Inbound::Call(call) => call,
                };
                let now = chrono::Utc::now();
                if let Some(c) = consumer.as_mut().filter(|c| c.conn == call.conn) {
                    c.last_request = now;
                }
                match call.operation {
                    Operation::Consume { kind, retry } => {
                        let refusal = if let Some(active) = &consumer {
                            Some(format!("delivery consumer active since {} (pid {}); stop it first. Nothing was replayed.",
                                active.since.to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                                active.pid.map_or("unknown".into(), |pid| pid.to_string())))
                        } else {
                            state.begin_delivery();
                            match (state.pending_delivery(), retry.as_deref()) {
                                (Some(batch), Some(retry)) if batch.id == retry => None,
                                (Some(batch), _) => Some(pending_refusal(session, &batch.id, &batch.created)),
                                (None, Some(retry)) => Some(format!("no pending delivery {retry}; nothing was replayed")),
                                (None, None) => None,
                            }
                        };
                        if let Some(text) = refusal {
                            let _ = call.reply.send(Response::error(text));
                        } else {
                            store.save(state)?; // A first consumer's starting cursor is durable.
                            consumer = Some(Consumer { conn: call.conn, kind, pid: call.pid, since: now, last_request: now, retry, accepted_at: None });
                            let _ = call.reply.send(Response::ok("delivery consumer registered\n"));
                        }
                    }
                    Operation::Deliver { wait, wake, max_bytes, min_interval } => {
                        let Some(active) = consumer.as_mut().filter(|c| c.conn == call.conn) else {
                            let _ = call.reply.send(Response::error("deliver requires this connection's consumer registration"));
                            continue;
                        };
                        match state.pending_delivery() {
                            Some(batch) if active.retry.as_deref() == Some(batch.id.as_str()) => {
                                // The single redelivery the operator authorized by ID.
                                active.retry = None;
                                let _ = call.reply.send(Response::delivered(batch.id.clone(), batch.render(true)));
                            }
                            Some(batch) => {
                                let _ = call.reply.send(Response::error(format!("delivery {} awaits acceptance; nothing was replayed", batch.id)));
                            }
                            None => {
                                delivering = Some(WaitingDelivery {
                                    conn: call.conn,
                                    until: Instant::now() + Duration::from_secs(wait),
                                    wake,
                                    max_bytes,
                                    min_interval: Duration::from_secs(min_interval),
                                    reply: call.reply,
                                });
                            }
                        }
                    }
                    Operation::Accept { id } => {
                        let own = consumer.as_ref().is_some_and(|c| c.conn == call.conn);
                        if consumer.is_some() && !own {
                            let _ = call.reply.send(Response::error("a delivery consumer is registered; recovery is refused while it runs. Nothing was changed."));
                            continue;
                        }
                        let response = match state.finish_delivery(&id, "accepted") {
                            Ok(text) => {
                                store.save(state)?;
                                if let Some(c) = consumer.as_mut() {
                                    c.accepted_at = Some(Instant::now());
                                }
                                Response::ok(format!("{text}\n"))
                            }
                            Err(e) => Response::error(format!("{e:#}; nothing was changed")),
                        };
                        let _ = call.reply.send(response);
                    }
                    Operation::DropPending { id } => {
                        let response = if consumer.is_some() {
                            Response::error("a delivery consumer is registered; recovery is refused while it runs. Nothing was changed.")
                        } else {
                            match state.finish_delivery(&id, "dropped") {
                                Ok(text) => { store.save(state)?; Response::ok(format!("{text}; its events remain readable\n")) }
                                Err(e) => Response::error(format!("{e:#}; nothing was changed")),
                            }
                        };
                        let _ = call.reply.send(response);
                    }
                    Operation::Deliveries { show } => {
                        let _ = call.reply.send(deliveries(state, consumer.as_ref(), show.as_deref()));
                    }
                    Operation::Show { reference, channel, depth } => {
                        if showing.is_some() {
                            let _ = call.reply.send(Response::error("another show is fetching history; retry after it finishes"));
                            continue;
                        }
                        let mut walk = ShowWalk { remaining: depth, records: Vec::new(), bytes: 0, note: None };
                        match walk.advance(state, Some((reference, channel))) {
                            ShowStep::Done(response) => { let _ = call.reply.send(response); }
                            ShowStep::Fetch { channel, msgid } => match state.begin_lookup(&channel, &msgid) {
                                Ok(()) => showing = Some(WaitingShow { until: Instant::now() + SHOW_TIMEOUT, msgid, walk, reply: call.reply }),
                                Err(e) => {
                                    walk.note = Some(format!("msgid:{msgid}: {e}"));
                                    let _ = call.reply.send(walk.finish());
                                }
                            },
                        }
                    }
                    Operation::Wait { wait, wake } => {
                        if waiter.as_ref().is_some_and(|w| !w.reply.is_closed()) {
                            let _ = call.reply.send(Response::error("another waiter is active; keep one waiter per session"));
                        } else {
                            waiter = Some(WaitingNotice { armed_at: chrono::Utc::now(), until: Instant::now() + Duration::from_secs(wait), wake, reply: call.reply });
                        }
                    }
                    Operation::Status => { let _ = call.reply.send(Response::ok(format!("{}{}{}", state.describe(true), waiter_status(waiter.as_ref()), consumer_status(consumer.as_ref())))); }
                    Operation::Disconnect => {
                        state.status = "explicitly disconnected".into();
                        store.save(state)?;
                        // Deliver the local reply before dropping client tasks.
                        let _ = call.reply.send(Response::ok("Disconnected. Other clients and the server remain running.\n"));
                        break;
                    }
                    Operation::Read { again: true, .. } => { let _ = call.reply.send(Response::ok(state.last_read.clone())); }
                    Operation::Read { wait, limit, .. } => {
                        if reader.as_ref().is_some_and(|r| !r.reply.is_closed()) {
                            let _ = call.reply.send(Response::error("another read is waiting; reads are serialized, retry after it finishes"));
                        } else { reader = Some(WaitingRead { until: Instant::now() + Duration::from_secs(wait), limit, reply: call.reply }); }
                    }
                    op @ (Operation::Join { .. } | Operation::Part { .. } | Operation::Topic { .. }) => {
                        let Some(request) = op.control() else { continue };
                        // One pending slot across sends, availability and controls.
                        let busy = (sender.is_some() || availability.is_some() || control.is_some())
                            .then_some("another send, availability update or channel control awaits confirmation");
                        match state.begin_control(request, busy) {
                            Err(e) => { let _ = call.reply.send(Response::error(format!("not sent: {e:#}"))); }
                            Ok(channels::Begin::Done { ok, text }) => {
                                store.save(state)?;
                                let _ = call.reply.send(if ok { Response::ok(format!("{}\n", safe(&text))) } else { Response::error(text) });
                            }
                            Ok(channels::Begin::Send(message)) => {
                                store.save(state)?; // Bound ID and intent are durable BEFORE queuing.
                                if connection.outbound.try_send(vec![message]).is_err() {
                                    let text = state.unsent_control("connection queue unavailable").map_or_else(String::new, |(_, text)| text);
                                    store.save(state)?;
                                    let _ = call.reply.send(Response::error(text));
                                } else {
                                    control = Some(WaitingControl { until: Instant::now() + SEND_TIMEOUT, reply: call.reply });
                                }
                            }
                        }
                    }
                    op @ (Operation::Away { .. } | Operation::Back) => {
                        if availability.is_some() || sender.is_some() || control.is_some() {
                            let _ = call.reply.send(Response::error("not sent: another send, availability update or channel control awaits confirmation"));
                            continue;
                        }
                        let reason = match op { Operation::Away { reason } => Some(reason), _ => None };
                        let message = client::away_message(reason.as_deref())?;
                        if connection.outbound.try_send(vec![message]).is_err() {
                            let _ = call.reply.send(Response::error("not sent: connection queue unavailable"));
                        } else {
                            availability = Some(WaitingAvailability {
                                until: Instant::now() + SEND_TIMEOUT,
                                expected: if reason.is_some() { "306" } else { "305" },
                                reason,
                                reply: call.reply,
                            });
                        }
                    }
                    Operation::Send { target, text, reply } => {
                        if availability.is_some() || control.is_some() {
                            let _ = call.reply.send(Response::error("not sent: an availability update or channel control awaits confirmation"));
                            continue;
                        }
                        if sender.is_some() {
                            let _ = call.reply.send(Response::error("not sent: another send awaits confirmation; retry this call after it finishes"));
                            continue;
                        }
                        match state.prepare_send(&target, &text, reply.as_deref(), connection.multiline) {
                            Err(e) => { let _ = call.reply.send(Response::error(format!("not sent: {e:#}"))); }
                            Ok(frames) => {
                                let token = frames[0].tags[SEND_TAG].clone();
                                let wait = (SEND_TIMEOUT + Duration::from_millis(12 * frames.len() as u64)).min(MAX_SEND_WAIT);
                                store.save(state)?; // Prepared uncertainty is durable BEFORE queuing.
                                if connection.outbound.try_send(frames).is_err() {
                                    state.set_outcome(&token, "not sent: connection queue unavailable".into());
                                    store.save(state)?;
                                    let _ = call.reply.send(Response::error(state.outcome(&token)));
                                } else {
                                    sender = Some(WaitingSend { until: Instant::now() + wait, token, target, reply: call.reply });
                                }
                            }
                        }
                    }
                }
            }
            _ = tick.tick() => {},
        }
        // A pending reader is satisfied first, so events it consumes cannot also
        // fire readiness and cause a stale wakeup.
        if reader.as_ref().is_some_and(|r| r.reply.is_closed()) {
            reader = None;
        }
        if reader
            .as_ref()
            .is_some_and(|r| state.has_unread() || Instant::now() >= r.until)
        {
            let read = reader.take().unwrap();
            let text = state.read(read.limit);
            store.save(state)?; // Cache/cursor publication precedes potentially lost stdout.
            let _ = read.reply.send(Response::ok(text));
        }
        if waiter.as_ref().is_some_and(|w| w.reply.is_closed()) {
            waiter = None;
        }
        if let Some(w) = &waiter {
            // Debounced by event times: 2 s of quiet, at most 10 s after the
            // first relevant event; critical events and gaps wake at once.
            let decision = match state.readiness(w.wake.mode(), w.until.into_std()) {
                Ok(attention::Readiness::Ready) => Some(Response::readiness(true)),
                Ok(attention::Readiness::TimedOut) => Some(Response::readiness(false)),
                Ok(attention::Readiness::Pending) => None,
                Err(e) => Some(Response::error(format!("wait failed: {e}; inspect status"))),
            };
            if let Some(response) = decision {
                let _ = waiter.take().unwrap().reply.send(response);
            }
        }
        if let Some((msgid, found)) = state.take_lookup() {
            // Only the show that asked for this msgid takes it; a result left by
            // an interrupted or timed-out show is discarded.
            if let Some(mut pending) = claim_lookup(&mut showing, &msgid) {
                let next = match found {
                    Ok(message) => match state.render_context(&message) {
                        Some(text) => pending.walk.push(text),
                        None => {
                            pending.walk.note = Some(format!(
                                "msgid:{}: the server returned no message record",
                                pending.msgid
                            ));
                            None
                        }
                    },
                    Err(e) => {
                        pending.walk.note = Some(format!("msgid:{}: {e}", pending.msgid));
                        None
                    }
                };
                match pending.walk.advance(state, next) {
                    ShowStep::Done(response) => {
                        let _ = pending.reply.send(response);
                    }
                    ShowStep::Fetch { channel, msgid } => {
                        match state.begin_lookup(&channel, &msgid) {
                            Ok(()) => showing = Some(WaitingShow { msgid, ..pending }),
                            Err(e) => {
                                pending.walk.note = Some(format!("msgid:{msgid}: {e}"));
                                let _ = pending.reply.send(pending.walk.finish());
                            }
                        }
                    }
                }
            }
            store.save(state)?; // References may have been assigned.
        }
        if showing
            .as_ref()
            .is_some_and(|s| s.reply.is_closed() || Instant::now() >= s.until)
        {
            let mut pending = showing.take().unwrap();
            pending.walk.note = Some(format!(
                "msgid:{}: the history lookup did not finish in time",
                pending.msgid
            ));
            let _ = pending.reply.send(pending.walk.finish());
        }
        if delivering.as_ref().is_some_and(|d| d.reply.is_closed()) {
            delivering = None;
        }
        if let Some(d) = &delivering {
            let mode = d.wake.mode();
            let decision = match state.delivery_readiness(mode, d.until.into_std()) {
                Ok(attention::Readiness::Ready) => {
                    let early = consumer
                        .as_ref()
                        .and_then(|c| c.accepted_at)
                        .is_some_and(|at| at.elapsed() < d.min_interval)
                        && !state.delivery_urgent(mode);
                    if early {
                        (Instant::now() >= d.until).then(|| Response::readiness(false))
                    } else if let Some((id, text)) =
                        state.prepare_delivery(session, mode, d.max_bytes)
                    {
                        store.save(state)?; // Pending is durable BEFORE anyone can see it.
                        Some(Response::delivered(id, text))
                    } else {
                        None // Only irrelevant events: the in-memory scan floor moved.
                    }
                }
                Ok(attention::Readiness::TimedOut) => Some(Response::readiness(false)),
                Ok(attention::Readiness::Pending) => None,
                Err(e) => Some(Response::error(format!(
                    "delivery wait failed: {e}; inspect status"
                ))),
            };
            if let Some(response) = decision {
                let id = response.delivery.clone();
                if delivering.take().unwrap().reply.send(response).is_err()
                    && let Some(id) = id
                {
                    // The caller left before any byte was written: certainly undelivered.
                    state.revert_delivery(&id);
                    store.save(state)?;
                }
            }
        }
        if availability
            .as_ref()
            .is_some_and(|pending| Instant::now() >= pending.until)
        {
            state.status = "availability confirmation timed out; reconnect explicitly and inspect WHO/WHOIS; no update was retried".into();
            state.notice(state.status.clone());
            break; // A late numeric must never confirm a subsequent update.
        }
        if sender.as_ref().is_some_and(|s| Instant::now() >= s.until) {
            state.status = "send confirmation timed out; reconnect explicitly and inspect history before resending".into();
            state.notice(state.status.clone());
            break; // Prevent a late rejection being assigned to a later send.
        }
        if control.as_ref().is_some_and(|c| Instant::now() >= c.until) {
            state.status = "channel control confirmation timed out; reconnect explicitly and inspect airc status; nothing was retried".into();
            state.notice(state.status.clone());
            break; // A late reply must never settle a later control.
        }
        // Setup barriers and the single JOIN history request advance on events and ticks.
        if let Some(request) = state.channel_tick(std::time::Instant::now())
            && connection.outbound.try_send(vec![request]).is_err()
        {
            state.history_unsent();
        }
        if state.take_dirty() {
            store.save(state)?;
        }
    }
    let interrupted = control.map(|pending| {
        (
            pending,
            state.interrupt_control("the connection ended before confirmation"),
        )
    });
    store.save(state)?;
    if let Some((pending, text)) = interrupted {
        let _ = pending.reply.send(Response::error(
            text.unwrap_or_else(|| state.status.clone()),
        ));
    }
    if let Some(pending) = availability {
        let _ = pending.reply.send(Response::error(
            "availability outcome unconfirmed; connection ended; nothing was retried",
        ));
    }
    if let Some(send) = sender {
        let _ = send.reply.send(Response::error(state.outcome(&send.token)));
    }
    if let Some(read) = reader {
        let _ = read.reply.send(Response::error(state.status.clone()));
    }
    if let Some(waiting) = waiter {
        let _ = waiting.reply.send(Response::error(state.status.clone()));
    }
    if let Some(waiting) = delivering {
        let _ = waiting.reply.send(Response::error(state.status.clone()));
    }
    if let Some(waiting) = showing {
        let _ = waiting.reply.send(Response::error(state.status.clone()));
    }
    // Every local socket has a bounded writer; allow replies to flush on quit.
    drop(request_rx);
    let _ = timeout(Duration::from_secs(2), async {
        while clients.join_next().await.is_some() {}
    })
    .await;
    Ok(())
}

/// One request per connection, except that a successful `consume` keeps the
/// connection as the delivery consumer's for its later requests. Closing it,
/// including by process death, releases the lease.
async fn handle(stream: UnixStream, tx: mpsc::Sender<Inbound>, conn: u64) -> Result<()> {
    let pid = stream.peer_cred().ok().and_then(|cred| cred.pid());
    let mut stream = BufReader::new(stream);
    let mut consuming = false;
    let result = async {
        loop {
            let line = if consuming {
                // A consumer may be idle for its sink's whole run.
                match read_line(&mut stream, MAX_REQUEST).await? {
                    Some(line) => line,
                    None => return Ok(()),
                }
            } else {
                timeout(Duration::from_secs(5), read_line(&mut stream, MAX_REQUEST))
                    .await??
                    .context("empty request")?
            };
            let parsed = (|| {
                let request: Request = serde_json::from_str(&line)?;
                ensure!(
                    request.version == VERSION,
                    "session IPC version mismatch; disconnect with the original binary before upgrading"
                );
                validate(&request.operation)?;
                ensure!(
                    !consuming
                        || matches!(
                            request.operation,
                            Operation::Deliver { .. } | Operation::Accept { .. }
                        ),
                    "a consumer connection only delivers and accepts"
                );
                Ok::<_, anyhow::Error>(request)
            })();
            let consume = matches!(
                parsed,
                Ok(Request {
                    operation: Operation::Consume { .. },
                    ..
                })
            );
            let response = match parsed {
                Err(e) => Response::error(format!("invalid request: {e:#}")),
                Ok(request) => {
                    let (reply, received) = oneshot::channel();
                    tx.send(Inbound::Call(Call {
                        conn,
                        pid,
                        operation: request.operation,
                        reply,
                    }))
                    .await
                    .map_err(|_| anyhow::anyhow!("worker stopped"))?;
                    // Client EOF while waiting cancels its pending read, not a send that
                    // may already have reached the server. No transport retries.
                    tokio::select! {
                        response = received => response.context("worker stopped before replying")?,
                        _ = read_line(&mut stream, 1) => { bail!("caller disconnected"); }
                    }
                }
            };
            let ok = response.ok;
            timeout(
                Duration::from_secs(2),
                write_json(stream.get_mut(), &response),
            )
            .await??;
            if !consuming && !(consume && ok) {
                return Ok(());
            }
            consuming = true;
        }
    }
    .await;
    let _ = tx.send(Inbound::Closed(conn)).await;
    result
}

async fn write_json(
    writer: &mut (impl tokio::io::AsyncWrite + Unpin),
    value: &impl Serialize,
) -> Result<()> {
    let mut line = serde_json::to_vec(value)?;
    line.push(b'\n');
    writer.write_all(&line).await?;
    writer.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn indefinite_wait_renews_only_a_successful_idle_response() {
        let root = tempfile::tempdir().unwrap();
        fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700)).unwrap();
        let store = Storage::open(root.path(), "waiter").unwrap();
        let listener = UnixListener::bind(store.socket()).unwrap();
        let peer = tokio::spawn(async move {
            for ready in [false, true] {
                let (socket, _) = listener.accept().await.unwrap();
                let mut socket = BufReader::new(socket);
                let line = read_line(&mut socket, 16384).await.unwrap().unwrap();
                let request: Request = serde_json::from_str(&line).unwrap();
                assert_eq!(request.version, VERSION);
                assert!(matches!(
                    request.operation,
                    Operation::Wait { wait: 60, .. }
                ));
                write_json(socket.get_mut(), &Response::readiness(ready))
                    .await
                    .unwrap();
            }
        });
        let response = timeout(
            Duration::from_secs(5),
            wait(root.path(), "waiter", None, Wake::default()),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(response.ok);
        assert!(response.text.contains("airc read waiter"));
        peer.await.unwrap();
    }

    #[test]
    fn channel_control_requests_are_strict_about_ids_channels_and_null_text() {
        let id = operation_id();
        assert!(channels::valid_operation_id(&id));
        assert_ne!(id, operation_id());
        let parse = |operation: serde_json::Value| {
            serde_json::from_value::<Request>(
                serde_json::json!({"version": VERSION, "operation": operation}),
            )
            .map_err(anyhow::Error::from)
            .and_then(|request| validate(&request.operation).map(|()| request.operation))
        };
        let query =
            parse(serde_json::json!({"op": "topic", "channel": "#a", "operation_id": id})).unwrap();
        assert!(matches!(query, Operation::Topic { text: None, .. }));
        assert!(
            !serde_json::to_string(&query).unwrap().contains("text"),
            "a query omits text"
        );
        let clear = parse(
            serde_json::json!({"op": "topic", "channel": "#a", "operation_id": id, "text": ""}),
        )
        .unwrap();
        assert!(matches!(clear, Operation::Topic { text: Some(ref t), .. } if t.is_empty()));
        for invalid in [
            serde_json::json!({"op": "topic", "channel": "#a", "operation_id": id, "text": null}),
            serde_json::json!({"op": "topic", "channel": "#a", "operation_id": id, "text": "a\nb"}),
            serde_json::json!({"op": "join", "channel": "#a"}),
            serde_json::json!({"op": "join", "channel": "#a", "operation_id": id.to_uppercase()}),
            serde_json::json!({"op": "join", "channel": "a", "operation_id": id}),
            serde_json::json!({"op": "part", "channel": "#a", "operation_id": id, "text": "x"}),
            serde_json::json!({"op": "join", "channel": "#a", "operation_id": id, "reason": "x"}),
        ] {
            assert!(parse(invalid.clone()).is_err(), "{invalid}");
        }
    }

    #[tokio::test]
    async fn wait_does_not_retry_an_error_or_missing_readiness() {
        for response in [Response::error("stopped"), Response::ok("malformed reply")] {
            let root = tempfile::tempdir().unwrap();
            fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700)).unwrap();
            let store = Storage::open(root.path(), "waiter").unwrap();
            let listener = UnixListener::bind(store.socket()).unwrap();
            let peer = tokio::spawn(async move {
                let (socket, _) = listener.accept().await.unwrap();
                let mut socket = BufReader::new(socket);
                read_line(&mut socket, 16384).await.unwrap().unwrap();
                write_json(socket.get_mut(), &response).await.unwrap();
                assert!(
                    timeout(Duration::from_millis(100), listener.accept())
                        .await
                        .is_err()
                );
            });
            let response = wait(root.path(), "waiter", None, Wake::default()).await;
            assert!(response.is_err() || !response.unwrap().ok);
            peer.await.unwrap();
        }
    }

    fn walk(remaining: u8) -> ShowWalk {
        ShowWalk {
            remaining,
            records: Vec::new(),
            bytes: 0,
            note: None,
        }
    }

    #[test]
    fn a_stale_lookup_result_never_answers_a_newer_show() {
        let (reply, _received) = oneshot::channel();
        let mut showing = Some(WaitingShow {
            until: Instant::now() + SHOW_TIMEOUT,
            msgid: "newer".into(),
            walk: walk(1),
            reply,
        });
        // A result for an interrupted show's msgid is discarded, not attributed.
        assert!(claim_lookup(&mut showing, "older").is_none());
        assert!(showing.is_some());
        assert_eq!(claim_lookup(&mut showing, "newer").unwrap().msgid, "newer");
        assert!(showing.is_none());
    }

    #[test]
    fn private_ancestors_are_never_fetched_from_channel_history() {
        let config = ClientConfig {
            host: "localhost".into(),
            port: 6697,
            tls: true,
            allow_insecure: false,
            ca_certificate: None,
            proxy: None,
            account: "alice".into(),
            nick: "Alice".into(),
            token_file: "unused".into(),
            channels: vec!["#work".into()],
            artifact_url: None,
            unix: None,
            artifact_unix: None,
        };
        let mut state = State::new(&config).unwrap();
        state.begin_epoch();
        let mut dm = crate::protocol::Message::new("PRIVMSG", ["Alice", "hi"]);
        dm.prefix = Some("Human!human@airc".into());
        dm.tags.insert("account".into(), "human".into());
        dm.tags.insert("msgid".into(), "dm1".into());
        dm.tags.insert("+reply".into(), "gone".into());
        state.ingest(&dm, false);
        let mut walk = walk(1);
        let ShowStep::Done(response) = walk.advance(&mut state, Some(("@1".into(), None))) else {
            panic!("a private ancestor must not trigger a channel history lookup");
        };
        assert!(response.ok);
        assert!(
            response
                .text
                .starts_with("context @1 Alice Human(human) reply=msgid:gone: hi\n"),
            "{}",
            response.text
        );
        assert!(
            response
                .text
                .contains("private messages have no server history"),
            "{}",
            response.text
        );
    }
}
