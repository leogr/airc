//! Named, explicitly started local IRC clients. IPC never carries credentials.
mod history;
mod state;
mod storage;

use crate::{
    client::{self, ClientConfig},
    protocol::{Message, fold, read_line},
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
const SEND_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum Operation {
    Wait {
        wait: u64,
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
    Status,
    Disconnect,
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
}
impl Response {
    fn ok(text: impl Into<String>) -> Self {
        Self {
            ok: true,
            text: text.into(),
            version: VERSION,
            ready: None,
        }
    }
    fn error(text: impl Into<String>) -> Self {
        Self {
            ok: false,
            text: format!("! {}\n", safe(&text.into())),
            version: VERSION,
            ready: None,
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
    operation: Operation,
    reply: oneshot::Sender<Response>,
}
struct WaitingRead {
    until: Instant,
    limit: usize,
    reply: oneshot::Sender<Response>,
}
struct WaitingNotice {
    until: Instant,
    reply: oneshot::Sender<Response>,
}
struct WaitingSend {
    until: Instant,
    token: String,
    target: String,
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
                        "! Session is starting/stopping or unavailable; saved state follows.\n{}",
                        state.describe(true).replacen("connected", "unavailable", 1)
                    ),
                    None => "Session is starting; no saved state yet.\n".into(),
                }));
            }
            let _lock = ownership?;
            let state = store.load()?;
            return Ok(match (operation, state) {
                (Operation::Status, Some(state)) => Response::ok(state.describe(false)),
                (Operation::Read { again: true, .. }, Some(state)) => Response::ok(state.last_read),
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
        Operation::Read { wait, .. } | Operation::Wait { wait } => *wait + 5,
        Operation::Send { .. } => 15,
        _ => 5,
    };
    let uncertain = matches!(operation, Operation::Send { .. });
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
                if uncertain {
                    "send outcome uncertain. Inspect airc status and history before resending; nothing was retried"
                } else {
                    "request was not retried; use read --again to recover a previous read"
                }
            )))
        }
    }
}

/// Wait for unread events without advancing the cursor or replacing read replay.
/// Each IPC wait is bounded; only successful idle responses renew the wait.
pub async fn wait(root: &Path, name: &str, seconds: Option<u64>) -> Result<Response> {
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
        let response = call(root, name, Operation::Wait { wait: interval }).await?;
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

fn validate(operation: &Operation) -> Result<()> {
    if let Operation::Wait { wait } = operation {
        ensure!(*wait <= 60, "internal wait requires 0–60 seconds");
    }
    if let Operation::Read { wait, limit, again } = operation {
        ensure!(
            *wait <= 60 && (1..=200).contains(limit),
            "read requires --wait 0–60 and --limit 1–200"
        );
        ensure!(!again || *wait == 0, "--again cannot wait");
    }
    Ok(())
}

/// Internal worker entry point. Its stdout is only a bounded startup response.
pub async fn worker(root: &Path, name: &str, config: &Path) -> Result<()> {
    let started = async {
        let store = Storage::open(root, name)?;
        let lock = store.lock()?;
        let config = ClientConfig::load(config)?;
        let mut state = store.load()?.unwrap_or(State::new(&config)?);
        ensure!(state.identity == identity(&config)?, "session config changed; use a new session name to preserve old recovery state");
        let mut connection = client::connect(&config).await?;
        ensure!(["message-tags", "account-tag", "echo-message", "batch", "draft/chathistory"].iter()
            .all(|cap| connection.capabilities.contains(*cap)), "named sessions need message-tags, account-tag, echo-message, batch and draft/chathistory");
        if state.status.starts_with("connected") {
            state.notice("Previous connection ended without a recorded shutdown; pending sends may be uncertain.");
        }
        state.epoch = uuid::Uuid::new_v4().to_string();
        state.status = "connected; no automatic reconnect or resend".into();
        state.notice(format!("connected as {}({}); epoch={}; history below is replay", config.nick, config.account, state.epoch));
        history::recover(&mut connection, &mut state, &config.channels).await?;
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
    let result = serve(&store, &mut state, &mut connection, listener).await;
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
    store: &Storage,
    state: &mut State,
    connection: &mut client::Connection,
    listener: UnixListener,
) -> Result<()> {
    let (requests, mut request_rx) = mpsc::channel::<Call>(16);
    let mut clients = JoinSet::new();
    let mut reader: Option<WaitingRead> = None;
    let mut waiter: Option<WaitingNotice> = None;
    let mut sender: Option<WaitingSend> = None;
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
                    clients.spawn(async move { let _ = handle(stream, tx).await; });
                }
            }
            Some(_) = clients.join_next(), if !clients.is_empty() => {},
            message = connection.inbound.recv() => {
                let Some(message) = message else {
                    state.status = "IRC disconnected; reconnect explicitly; pending sends are uncertain".into();
                    state.notice(state.status.clone());
                    break;
                };
                let previous_sequence = state.sequence();
                let confirmed = state.ingest(&message, false);
                if sender.as_ref().is_some_and(|s| confirmed.as_deref() == Some(s.token.as_str())) {
                    let sent = sender.take().unwrap();
                    store.save(state)?;
                    let _ = sent.reply.send(Response::ok(format!("{}\n", safe(&state.outcome(&sent.token)))));
                } else if let Some(pending) = &sender && rejects(&message, &pending.target) {
                    let sent = sender.take().unwrap();
                    state.set_outcome(&sent.token, format!("rejected by server {} {}", message.command, message.params.join(" ")));
                    store.save(state)?;
                    let _ = sent.reply.send(Response::error(state.outcome(&sent.token)));
                } else if confirmed.is_some() || state.sequence() != previous_sequence {
                    // Persist changed state; echo/rejection paths already saved
                    // above. Typing/setup numerics require no filesystem writes.
                    store.save(state)?;
                }
            }
            Some(call) = request_rx.recv() => {
                match call.operation {
                    Operation::Wait { wait } => {
                        if waiter.as_ref().is_some_and(|w| !w.reply.is_closed()) {
                            let _ = call.reply.send(Response::error("another waiter is active; keep one waiter per session"));
                        } else {
                            waiter = Some(WaitingNotice { until: Instant::now() + Duration::from_secs(wait), reply: call.reply });
                        }
                    }
                    Operation::Status => { let _ = call.reply.send(Response::ok(state.describe(true))); }
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
                    Operation::Send { target, text, reply } => {
                        if sender.is_some() {
                            let _ = call.reply.send(Response::error("not sent: another send awaits confirmation; retry this call after it finishes"));
                            continue;
                        }
                        match state.prepare_send(&target, &text, reply.as_deref()) {
                            Err(e) => { let _ = call.reply.send(Response::error(format!("not sent: {e:#}"))); }
                            Ok(message) => {
                                let token = message.tags[SEND_TAG].clone();
                                store.save(state)?; // Prepared uncertainty is durable BEFORE queuing.
                                if connection.outbound.try_send(message).is_err() {
                                    state.set_outcome(&token, "not sent: connection queue unavailable".into());
                                    store.save(state)?;
                                    let _ = call.reply.send(Response::error(state.outcome(&token)));
                                } else {
                                    sender = Some(WaitingSend { until: Instant::now() + SEND_TIMEOUT, token, target, reply: call.reply });
                                }
                            }
                        }
                    }
                }
            }
            _ = tick.tick() => {},
        }
        if waiter.as_ref().is_some_and(|w| w.reply.is_closed()) {
            waiter = None;
        }
        if waiter
            .as_ref()
            .is_some_and(|w| state.has_unread() || Instant::now() >= w.until)
        {
            let waiting = waiter.take().unwrap();
            let _ = waiting.reply.send(Response::readiness(state.has_unread()));
        }
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
        if sender.as_ref().is_some_and(|s| Instant::now() >= s.until) {
            state.status = "send confirmation timed out; reconnect explicitly and inspect history before resending".into();
            state.notice(state.status.clone());
            break; // Prevent a late rejection being assigned to a later send.
        }
    }
    store.save(state)?;
    if let Some(send) = sender {
        let _ = send.reply.send(Response::error(state.outcome(&send.token)));
    }
    if let Some(read) = reader {
        let _ = read.reply.send(Response::error(state.status.clone()));
    }
    if let Some(waiting) = waiter {
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

fn rejects(message: &Message, target: &str) -> bool {
    match message.command.as_str() {
        "401" | "403" | "404" => message
            .params
            .get(1)
            .is_some_and(|v| fold(v) == fold(target)),
        "417" => true,
        "FAIL" => {
            message.params.first().is_some_and(|v| v == "PRIVMSG")
                && message
                    .params
                    .get(2)
                    .is_some_and(|v| fold(v) == fold(target))
        }
        _ => false,
    }
}

async fn handle(stream: UnixStream, tx: mpsc::Sender<Call>) -> Result<()> {
    let mut stream = BufReader::new(stream);
    let line = timeout(Duration::from_secs(5), read_line(&mut stream, 16384))
        .await??
        .context("empty request")?;
    let parsed = (|| {
        let request: Request = serde_json::from_str(&line)?;
        ensure!(
            request.version == VERSION,
            "session IPC version mismatch; disconnect with the original binary before upgrading"
        );
        validate(&request.operation)?;
        Ok::<_, anyhow::Error>(request)
    })();
    let response = match parsed {
        Err(e) => Response::error(format!("invalid request: {e:#}")),
        Ok(request) => {
            let (reply, received) = oneshot::channel();
            tx.send(Call {
                operation: request.operation,
                reply,
            })
            .await?;
            // Client EOF while waiting cancels its pending read, not a send that
            // may already have reached the server. No transport retries.
            tokio::select! {
                response = received => response.context("worker stopped before replying")?,
                _ = read_line(&mut stream, 1) => { bail!("caller disconnected"); }
            }
        }
    };
    timeout(
        Duration::from_secs(2),
        write_json(stream.get_mut(), &response),
    )
    .await??;
    Ok(())
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
                assert!(matches!(request.operation, Operation::Wait { wait: 60 }));
                write_json(socket.get_mut(), &Response::readiness(ready))
                    .await
                    .unwrap();
            }
        });
        let response = timeout(Duration::from_secs(5), wait(root.path(), "waiter", None))
            .await
            .unwrap()
            .unwrap();
        assert!(response.ok);
        assert!(response.text.contains("airc read waiter"));
        peer.await.unwrap();
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
            let response = wait(root.path(), "waiter", None).await;
            assert!(response.is_err() || !response.unwrap().ok);
            peer.await.unwrap();
        }
    }
}
