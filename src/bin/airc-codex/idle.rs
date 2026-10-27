//! `--wait-for-idle`: observe the target thread through the local Codex
//! app-server control socket until a fresh `thread/read` reports it idle.
//!
//! Read-only. It sends only `initialize`, `initialized` and `thread/read`; it
//! never starts or restarts a daemon, subscribes to or resumes the thread, or
//! enqueues anything. Transient endpoint failures keep the same adapter and
//! batch waiting with capped backoff; anything that is not a usable, same-user
//! status answer is a definite refusal. An idle observation never survives a
//! disconnect or an endpoint change.

use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    io::ErrorKind,
    os::unix::{
        ffi::OsStrExt,
        fs::{FileTypeExt, MetadataExt},
    },
    path::PathBuf,
    time::Duration,
};
use tokio::{net::UnixStream, time::Instant};
use tokio_tungstenite::{
    WebSocketStream, client_async_with_config,
    tungstenite::{Error as WsError, Message, error::ProtocolError, protocol::WebSocketConfig},
};

/// Bound on one app-server message; status replies are small.
const MAX_MESSAGE_BYTES: usize = 4 << 20;
/// The request URI Codex 0.164 clients use over the control socket. The
/// bytes travel over the Unix socket; this is not a durable path contract.
const HANDSHAKE_URL: &str = "ws://localhost/rpc";
/// Reconnect delays in `Timing::backoff_unit`s; the last one repeats.
const BACKOFF: [u32; 6] = [1, 2, 4, 8, 15, 30];

#[derive(Clone, Copy)]
pub struct Timing {
    /// Connect, handshake and each request's reply.
    pub rpc: Duration,
    /// Re-read while active even without a status event. Not a deadline.
    pub recheck: Duration,
    pub backoff_unit: Duration,
}

impl Timing {
    pub const DEFAULT: Self = Self {
        rpc: Duration::from_secs(5),
        recheck: Duration::from_secs(30),
        backoff_unit: Duration::from_secs(1),
    };
}

/// Why the wait ended without permission to send.
#[derive(Debug, PartialEq, Eq)]
pub enum Refusal {
    /// The endpoint or thread answered in a way that waiting cannot fix.
    Definite(String),
    /// The configured `--idle-timeout` elapsed; `last` is the latest reason.
    Deadline { seconds: u64, last: String },
}

/// One attempt's failure: retry the read-only discovery, or stop.
#[derive(Debug)]
enum Failure {
    Transient(String),
    Definite(String),
}

/// The local daemon endpoint as Codex 0.164 publishes it: a per-home alias
/// symlink whose target is a socket in the fixed per-user directory.
#[derive(Clone)]
pub struct Endpoint {
    pub alias: PathBuf,
    pub daemon_dir: PathBuf,
    pub uid: u32,
}

/// The socket a connection was made to; a change invalidates any idle
/// observation made through it.
#[derive(Debug, PartialEq, Eq)]
struct Identity {
    path: PathBuf,
    dev: u64,
    ino: u64,
}

impl Endpoint {
    /// `$CODEX_HOME/app-server-control/app-server-control.sock` (home
    /// defaulting to `~/.codex`) and `/tmp/codex-daemon-<euid>`. No other
    /// home, process scan or remote endpoint is considered.
    pub fn local() -> Result<Self, String> {
        let home = match std::env::var_os("CODEX_HOME").filter(|home| !home.is_empty()) {
            Some(home) => PathBuf::from(home),
            None => std::env::var_os("HOME")
                .filter(|home| !home.is_empty())
                .map(|home| PathBuf::from(home).join(".codex"))
                .ok_or("neither CODEX_HOME nor HOME is set")?,
        };
        if !home.is_absolute() {
            return Err("the Codex home must be an absolute path".into());
        }
        let uid = rustix::process::geteuid().as_raw();
        let tmp = std::fs::canonicalize("/tmp").map_err(|error| format!("/tmp: {error}"))?;
        Ok(Self {
            alias: home
                .join("app-server-control")
                .join("app-server-control.sock"),
            daemon_dir: tmp.join(format!("codex-daemon-{uid}")),
            uid,
        })
    }

    /// Follows the alias only to the socket Codex itself would bind for it.
    fn resolve(&self) -> Result<Identity, Failure> {
        let gone = |what: &str| Failure::Transient(format!("{what} is absent"));
        let alias = match std::fs::symlink_metadata(&self.alias) {
            Ok(meta) => meta,
            Err(error) if error.kind() == ErrorKind::NotFound => return Err(gone("control alias")),
            Err(error) => return Err(Failure::Definite(format!("control alias: {error}"))),
        };
        if !alias.file_type().is_symlink() || alias.uid() != self.uid {
            return Err(Failure::Definite(
                "control alias is not this user's daemon link".into(),
            ));
        }
        let target = std::fs::read_link(&self.alias).map_err(|_| gone("control alias"))?;
        let (Some(parent), Some(name)) = (self.alias.parent(), self.alias.file_name()) else {
            return Err(Failure::Definite("control alias has no parent".into()));
        };
        let parent = std::fs::canonicalize(parent).map_err(|_| gone("Codex home"))?;
        let hash = Sha256::digest(parent.join(name).as_os_str().as_bytes());
        let expected = self.daemon_dir.join(format!("{hash:x}"));
        if target != expected {
            return Err(Failure::Definite(
                "control alias names another socket".into(),
            ));
        }
        match std::fs::symlink_metadata(&self.daemon_dir) {
            Ok(dir) if dir.is_dir() && dir.uid() == self.uid && dir.mode() & 0o777 == 0o700 => {}
            Ok(_) => {
                return Err(Failure::Definite(
                    "daemon socket directory is not private to this user".into(),
                ));
            }
            Err(error) if error.kind() == ErrorKind::NotFound => {
                return Err(gone("daemon socket directory"));
            }
            Err(error) => return Err(Failure::Definite(format!("daemon directory: {error}"))),
        }
        let socket = match std::fs::symlink_metadata(&expected) {
            Ok(meta) => meta,
            Err(error) if error.kind() == ErrorKind::NotFound => {
                return Err(Failure::Transient("control alias is dangling".into()));
            }
            Err(error) => return Err(Failure::Definite(format!("daemon socket: {error}"))),
        };
        if !socket.file_type().is_socket() || socket.uid() != self.uid {
            return Err(Failure::Definite(
                "daemon socket is not this user's socket".into(),
            ));
        }
        Ok(Identity {
            path: expected,
            dev: socket.dev(),
            ino: socket.ino(),
        })
    }
}

/// Waits until `thread` is idle, then rechecks `still_ours` (the subscriber
/// parent) and the endpoint identity. `idle_timeout` 0 waits indefinitely;
/// a positive value covers the whole wait, reconnects and backoff included,
/// and one too large to represent is refused before connecting.
pub async fn wait(
    endpoint: &Endpoint,
    thread: &str,
    idle_timeout: u64,
    timing: Timing,
    still_ours: impl Fn() -> Result<(), String>,
) -> Result<(), Refusal> {
    let deadline = match idle_timeout {
        0 => None,
        seconds => Some(
            Instant::now()
                .checked_add(Duration::from_secs(seconds))
                .ok_or_else(|| {
                    Refusal::Definite(format!("--idle-timeout {seconds} is too large"))
                })?,
        ),
    };
    let mut last = String::from("no status read yet");
    let mut attempt = 0usize;
    loop {
        let session = observe(endpoint, thread, timing, &still_ours, &mut attempt);
        let outcome = match deadline {
            Some(end) => match tokio::time::timeout_at(end, session).await {
                Ok(outcome) => outcome,
                Err(_) => {
                    return Err(Refusal::Deadline {
                        seconds: idle_timeout,
                        last,
                    });
                }
            },
            None => session.await,
        };
        let reason = match outcome {
            Ok(()) => return Ok(()),
            Err(Failure::Definite(reason)) => return Err(Refusal::Definite(reason)),
            Err(Failure::Transient(reason)) => reason,
        };
        let step = BACKOFF[attempt.min(BACKOFF.len() - 1)];
        attempt += 1;
        let delay = timing.backoff_unit * step;
        if reason != last {
            eprintln!("airc-codex: waiting for idle: {reason}; retrying status discovery");
        }
        last = reason;
        let wake = Instant::now() + delay;
        match deadline {
            Some(end) if end <= wake => {
                tokio::time::sleep_until(end).await;
                return Err(Refusal::Deadline {
                    seconds: idle_timeout,
                    last,
                });
            }
            _ => tokio::time::sleep_until(wake).await,
        }
    }
}

/// One connection: initialize, then read until a fresh idle answer that is
/// still valid for this subscriber and endpoint. A valid status read resets
/// the reconnect backoff.
async fn observe(
    endpoint: &Endpoint,
    thread: &str,
    timing: Timing,
    still_ours: &impl Fn() -> Result<(), String>,
    attempt: &mut usize,
) -> Result<(), Failure> {
    let mut rpc = Rpc::connect(endpoint, timing).await?;
    let mut recheck = Instant::now() + timing.recheck;
    // Read now; afterwards on a matching status event or the recheck timer.
    let mut stale = true;
    loop {
        if stale {
            stale = false;
            let id = rpc
                .request(
                    "thread/read",
                    json!({"threadId": thread, "includeTurns": false}),
                )
                .await?;
            let reply_by = Instant::now() + timing.rpc;
            let result = loop {
                tokio::select! {
                    incoming = rpc.next() => match incoming? {
                        Incoming::Reply(reply_id, result) if reply_id == id => break result?,
                        Incoming::Reply(..) => {
                            return Err(Failure::Definite("reply to an unknown request".into()));
                        }
                        Incoming::Status(changed) if changed == thread => stale = true,
                        Incoming::Status(_) | Incoming::Other => {}
                    },
                    () = tokio::time::sleep_until(reply_by) => {
                        return Err(Failure::Transient("thread/read reply timed out".into()));
                    }
                    () = tokio::time::sleep_until(recheck) => {
                        recheck += timing.recheck;
                        stale = true;
                    }
                }
            };
            match thread_status(&result, thread)? {
                Status::Idle => {
                    *attempt = 0;
                    still_ours().map_err(Failure::Definite)?;
                    if endpoint.resolve()? != rpc.identity {
                        return Err(Failure::Transient(
                            "control socket changed; idle must be observed again".into(),
                        ));
                    }
                    rpc.close().await;
                    return Ok(());
                }
                Status::Active => *attempt = 0,
            }
            continue;
        }
        tokio::select! {
            incoming = rpc.next() => match incoming? {
                Incoming::Reply(..) => {
                    return Err(Failure::Definite("reply to an unknown request".into()));
                }
                Incoming::Status(changed) if changed == thread => stale = true,
                Incoming::Status(_) | Incoming::Other => {}
            },
            () = tokio::time::sleep_until(recheck) => {
                recheck += timing.recheck;
                stale = true;
            }
        }
    }
}

enum Status {
    Idle,
    Active,
}

/// Only `idle` permits sending and only `active` keeps waiting.
fn thread_status(result: &Value, thread: &str) -> Result<Status, Failure> {
    let refused = |reason: &str| Err(Failure::Definite(reason.into()));
    let Some(found) = result.get("thread") else {
        return refused("thread/read reply has no thread");
    };
    if found.get("id").and_then(Value::as_str) != Some(thread) {
        return refused("thread/read answered for another thread");
    }
    match found.pointer("/status/type").and_then(Value::as_str) {
        Some("idle") => Ok(Status::Idle),
        Some("active") => Ok(Status::Active),
        Some("notLoaded") => refused("thread is not loaded by the app server"),
        Some("systemError") => refused("thread is in a system error"),
        _ => refused("thread/read reply has an unknown status"),
    }
}

/// One classified incoming app-server message.
enum Incoming {
    /// A reply: its ID, then the result or the explicit RPC error.
    Reply(Value, Result<Value, Failure>),
    /// `thread/status/changed` for this thread ID.
    Status(String),
    /// Any other notification or server request; ignored unread.
    Other,
}

struct Rpc {
    ws: WebSocketStream<UnixStream>,
    identity: Identity,
    next_id: u64,
    rpc: Duration,
}

impl Rpc {
    async fn connect(endpoint: &Endpoint, timing: Timing) -> Result<Self, Failure> {
        let identity = endpoint.resolve()?;
        let stream = tokio::time::timeout(timing.rpc, UnixStream::connect(&identity.path))
            .await
            .map_err(|_| Failure::Transient("connect timed out".into()))?
            .map_err(|error| io_failure("connect", &error))?;
        let peer = stream
            .peer_cred()
            .map_err(|error| io_failure("peer check", &error))?;
        if peer.uid() != endpoint.uid {
            return Err(Failure::Definite(format!(
                "control socket peer runs as uid {}",
                peer.uid()
            )));
        }
        if endpoint.resolve()? != identity {
            return Err(Failure::Transient(
                "control socket changed while connecting".into(),
            ));
        }
        let config = WebSocketConfig::default()
            .max_message_size(Some(MAX_MESSAGE_BYTES))
            .max_frame_size(Some(MAX_MESSAGE_BYTES));
        let (ws, _) = tokio::time::timeout(
            timing.rpc,
            client_async_with_config(HANDSHAKE_URL, stream, Some(config)),
        )
        .await
        .map_err(|_| Failure::Transient("handshake timed out".into()))?
        .map_err(|error| ws_failure("handshake", error))?;
        let mut rpc = Self {
            ws,
            identity,
            next_id: 0,
            rpc: timing.rpc,
        };
        let params = json!({
            "clientInfo": {"name": "airc-codex", "version": env!("CARGO_PKG_VERSION")},
            "capabilities": {"experimentalApi": false},
        });
        let id = rpc.request("initialize", params).await?;
        let reply_by = Instant::now() + timing.rpc;
        loop {
            let incoming = tokio::time::timeout_at(reply_by, rpc.next())
                .await
                .map_err(|_| Failure::Transient("initialize reply timed out".into()))??;
            match incoming {
                Incoming::Reply(reply_id, result) if reply_id == id => {
                    result?;
                    break;
                }
                Incoming::Reply(..) => {
                    return Err(Failure::Definite("reply to an unknown request".into()));
                }
                Incoming::Status(_) | Incoming::Other => {}
            }
        }
        rpc.send(json!({"method": "initialized"})).await?;
        Ok(rpc)
    }

    async fn request(&mut self, method: &str, params: Value) -> Result<Value, Failure> {
        self.next_id += 1;
        let id = json!(self.next_id);
        self.send(json!({"id": id, "method": method, "params": params}))
            .await?;
        Ok(id)
    }

    async fn send(&mut self, message: Value) -> Result<(), Failure> {
        tokio::time::timeout(self.rpc, self.ws.send(Message::text(message.to_string())))
            .await
            .map_err(|_| Failure::Transient("app server write timed out".into()))?
            .map_err(|error| ws_failure("write", error))
    }

    /// The next message, skipping control frames. Cancel-safe.
    async fn next(&mut self) -> Result<Incoming, Failure> {
        loop {
            let text = match self.ws.next().await {
                Some(Ok(Message::Text(text))) => text,
                Some(Ok(Message::Ping(_) | Message::Pong(_) | Message::Frame(_))) => continue,
                Some(Ok(Message::Binary(_))) => {
                    return Err(Failure::Definite("app server sent a binary message".into()));
                }
                Some(Ok(Message::Close(_))) | None => {
                    return Err(Failure::Transient(
                        "app server closed the connection".into(),
                    ));
                }
                Some(Err(error)) => return Err(ws_failure("read", error)),
            };
            return classify(text.as_str());
        }
    }

    async fn close(mut self) {
        let _ = tokio::time::timeout(Duration::from_secs(1), self.ws.close(None)).await;
    }
}

fn classify(text: &str) -> Result<Incoming, Failure> {
    let malformed = || Failure::Definite("app server sent malformed JSON-RPC".into());
    let message: Value = serde_json::from_str(text).map_err(|_| malformed())?;
    let object = message.as_object().ok_or_else(malformed)?;
    if let Some(method) = object.get("method") {
        // Unrelated notifications and server requests are dropped unread and
        // unlogged; this connection subscribed to nothing and answers nothing.
        if method.as_str() != Some("thread/status/changed") || object.contains_key("id") {
            return Ok(Incoming::Other);
        }
        let changed = message
            .pointer("/params/threadId")
            .and_then(Value::as_str)
            .ok_or_else(malformed)?;
        return Ok(Incoming::Status(changed.to_string()));
    }
    let id = object.get("id").cloned().ok_or_else(malformed)?;
    if let Some(result) = object.get("result") {
        return Ok(Incoming::Reply(id, Ok(result.clone())));
    }
    let error = object.get("error").ok_or_else(malformed)?;
    let detail = error
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("no message");
    let detail: String = detail
        .chars()
        .filter(|c| !c.is_control())
        .take(200)
        .collect();
    Ok(Incoming::Reply(
        id,
        Err(Failure::Definite(format!("app server refused: {detail}"))),
    ))
}

/// A missing, refused, reset or closed endpoint can come back; anything else
/// (such as permission errors) is not something waiting fixes.
fn io_failure(phase: &str, error: &std::io::Error) -> Failure {
    let transient = matches!(
        error.kind(),
        ErrorKind::NotFound
            | ErrorKind::ConnectionRefused
            | ErrorKind::ConnectionReset
            | ErrorKind::ConnectionAborted
            | ErrorKind::BrokenPipe
            | ErrorKind::UnexpectedEof
            | ErrorKind::NotConnected
            | ErrorKind::TimedOut
    );
    let reason = format!("{phase}: {}", error.kind());
    if transient {
        Failure::Transient(reason)
    } else {
        Failure::Definite(reason)
    }
}

fn ws_failure(phase: &str, error: WsError) -> Failure {
    match error {
        WsError::Io(error) => io_failure(phase, &error),
        // A daemon that exits or restarts drops the stream without a close.
        WsError::ConnectionClosed
        | WsError::AlreadyClosed
        | WsError::Protocol(
            ProtocolError::ResetWithoutClosingHandshake | ProtocolError::HandshakeIncomplete,
        ) => Failure::Transient(format!("{phase}: app server closed the connection")),
        error => Failure::Definite(format!("{phase}: {error}")),
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::{
        os::unix::fs::{DirBuilderExt, PermissionsExt},
        sync::{Arc, Mutex},
    };
    use tokio::net::UnixListener;
    use tokio_tungstenite::accept_async;

    pub const THREAD: &str = "0199a3b2-6c1d-7e00-8f00-123456789abc";

    pub fn fast() -> Timing {
        Timing {
            rpc: Duration::from_millis(500),
            recheck: Duration::from_millis(300),
            backoff_unit: Duration::from_millis(20),
        }
    }

    /// A private Codex home and daemon directory laid out like Codex 0.164.
    pub struct Layout {
        pub dir: tempfile::TempDir,
        pub endpoint: Endpoint,
    }

    impl Layout {
        pub fn new() -> Self {
            let dir = tempfile::tempdir().unwrap();
            let home = dir.path().join("home");
            std::fs::create_dir_all(home.join("app-server-control")).unwrap();
            let daemon_dir = dir.path().join("daemon");
            std::fs::DirBuilder::new()
                .mode(0o700)
                .create(&daemon_dir)
                .unwrap();
            let endpoint = Endpoint {
                alias: home
                    .join("app-server-control")
                    .join("app-server-control.sock"),
                daemon_dir,
                uid: rustix::process::geteuid().as_raw(),
            };
            Self { dir, endpoint }
        }

        pub fn socket(&self) -> PathBuf {
            let parent = std::fs::canonicalize(self.endpoint.alias.parent().unwrap()).unwrap();
            let hash = Sha256::digest(
                parent
                    .join(self.endpoint.alias.file_name().unwrap())
                    .as_os_str()
                    .as_bytes(),
            );
            self.endpoint.daemon_dir.join(format!("{hash:x}"))
        }

        /// Binds the daemon socket and publishes the alias, as Codex does.
        pub fn bind(&self) -> UnixListener {
            let socket = self.socket();
            let _ = std::fs::remove_file(&socket);
            let listener = UnixListener::bind(&socket).unwrap();
            let _ = std::fs::remove_file(&self.endpoint.alias);
            std::os::unix::fs::symlink(&socket, &self.endpoint.alias).unwrap();
            listener
        }
    }

    /// What one mock connection answers to each `thread/read`, in order; the
    /// last entry repeats.
    #[derive(Clone)]
    pub enum Reply {
        Status(&'static str),
        /// Answer `active`, then publish idle after this many milliseconds.
        ActiveThenEvent(u64),
        Raw(&'static str),
        /// Answer `idle`, then at once publish `active`: a turn started
        /// between the read and the queue.
        IdleThenActive,
        /// Never answer.
        Silent,
        /// Close the connection instead of answering.
        Hangup,
    }

    /// Requests every mock connection received, as (connection, method).
    pub type Log = Arc<Mutex<Vec<(usize, String)>>>;

    /// Serves Codex-shaped connections; `script[n]` drives connection `n`
    /// (the last script repeats).
    pub fn serve(listener: UnixListener, script: Vec<Vec<Reply>>, log: Log) {
        tokio::spawn(async move {
            let mut connection = 0;
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                let replies = script[connection.min(script.len() - 1)].clone();
                tokio::spawn(connection_task(stream, connection, replies, log.clone()));
                connection += 1;
            }
        });
    }

    async fn connection_task(stream: UnixStream, n: usize, replies: Vec<Reply>, log: Log) {
        let Ok(mut ws) = accept_async(stream).await else {
            return;
        };
        let mut reads = 0;
        // Interleave unrelated traffic before anything else.
        let noise = json!({"method": "thread/status/changed",
            "params": {"threadId": "0199a3b2-6c1d-7e00-8f00-000000000000", "status": {"type": "idle"}}});
        let _ = ws.send(Message::text(noise.to_string())).await;
        while let Some(Ok(Message::Text(text))) = ws.next().await {
            let message: Value = serde_json::from_str(text.as_str()).unwrap();
            let method = message["method"].as_str().unwrap().to_string();
            log.lock().unwrap().push((n, method.clone()));
            let id = message["id"].clone();
            let reply = |status: &str| {
                json!({"id": id, "result": {"thread": {"id": THREAD, "status": {"type": status, "activeFlags": []}}}})
                    .to_string()
            };
            match method.as_str() {
                "initialize" => {
                    let other = json!({"method": "account/updated", "params": {"secret": "x"}});
                    let _ = ws.send(Message::text(other.to_string())).await;
                    let ok = json!({"id": id, "result": {"userAgent": "codex/0.164.0"}});
                    let _ = ws.send(Message::text(ok.to_string())).await;
                }
                "thread/read" => {
                    let step = replies[reads.min(replies.len() - 1)].clone();
                    reads += 1;
                    match step {
                        Reply::Status(status) => {
                            let _ = ws.send(Message::text(reply(status))).await;
                        }
                        Reply::ActiveThenEvent(after) => {
                            let _ = ws.send(Message::text(reply("active"))).await;
                            tokio::time::sleep(Duration::from_millis(after)).await;
                            let event = json!({"method": "thread/status/changed",
                                "params": {"threadId": THREAD, "status": {"type": "idle"}}});
                            let _ = ws.send(Message::text(event.to_string())).await;
                        }
                        Reply::IdleThenActive => {
                            let _ = ws.send(Message::text(reply("idle"))).await;
                            let event = json!({"method": "thread/status/changed",
                                "params": {"threadId": THREAD, "status": {"type": "active", "activeFlags": []}}});
                            let _ = ws.send(Message::text(event.to_string())).await;
                        }
                        Reply::Raw(raw) => {
                            let raw = raw.replace("ID", &id.to_string());
                            let _ = ws.send(Message::text(raw)).await;
                        }
                        Reply::Silent => {}
                        Reply::Hangup => return,
                    }
                }
                _ => {}
            }
        }
    }

    pub fn reads(log: &Log) -> Vec<(usize, String)> {
        log.lock().unwrap().clone()
    }

    fn mine() -> Result<(), String> {
        Ok(())
    }

    async fn run(
        layout: &Layout,
        script: Vec<Vec<Reply>>,
        timeout: u64,
    ) -> (Result<(), Refusal>, Log) {
        let log = Log::default();
        serve(layout.bind(), script, log.clone());
        let outcome = wait(&layout.endpoint, THREAD, timeout, fast(), mine).await;
        (outcome, log)
    }

    #[tokio::test]
    async fn already_idle_reads_once_after_initialize() {
        let layout = Layout::new();
        let (outcome, log) = run(&layout, vec![vec![Reply::Status("idle")]], 0).await;
        assert_eq!(outcome, Ok(()));
        let methods: Vec<_> = reads(&log).into_iter().map(|(_, m)| m).collect();
        assert_eq!(methods, ["initialize", "initialized", "thread/read"]);
    }

    #[tokio::test]
    async fn an_idle_event_triggers_a_fresh_read() {
        let layout = Layout::new();
        let started = Instant::now();
        let script = vec![vec![Reply::ActiveThenEvent(50), Reply::Status("idle")]];
        let (outcome, log) = run(&layout, script, 0).await;
        assert_eq!(outcome, Ok(()));
        // The event, not the 300 ms recheck, released it, through a new read.
        assert!(started.elapsed() < Duration::from_millis(250));
        let read = reads(&log)
            .iter()
            .filter(|(_, m)| m == "thread/read")
            .count();
        assert_eq!(read, 2);
    }

    #[tokio::test]
    async fn a_quiet_active_thread_is_rechecked_on_the_timer() {
        let layout = Layout::new();
        let started = Instant::now();
        let script = vec![vec![
            Reply::Status("active"),
            Reply::Status("active"),
            Reply::Status("idle"),
        ]];
        let (outcome, log) = run(&layout, script, 0).await;
        assert_eq!(outcome, Ok(()));
        let elapsed = started.elapsed();
        // Two rechecks of 300 ms, no busy polling and no reconnect.
        assert!(elapsed >= Duration::from_millis(600), "{elapsed:?}");
        assert!(elapsed < Duration::from_millis(1500), "{elapsed:?}");
        let log = reads(&log);
        assert_eq!(log.iter().filter(|(_, m)| m == "thread/read").count(), 3);
        assert!(log.iter().all(|(n, _)| *n == 0));
    }

    #[tokio::test]
    async fn definite_answers_refuse() {
        for (raw, detail) in [
            (
                r#"{"id":ID,"result":{"thread":{"id":"0199a3b2-6c1d-7e00-8f00-123456789abc","status":{"type":"notLoaded"}}}}"#,
                "not loaded",
            ),
            (
                r#"{"id":ID,"result":{"thread":{"id":"0199a3b2-6c1d-7e00-8f00-123456789abc","status":{"type":"systemError"}}}}"#,
                "system error",
            ),
            (
                r#"{"id":ID,"result":{"thread":{"id":"0199a3b2-6c1d-7e00-8f00-123456789abc","status":{"type":"paused"}}}}"#,
                "unknown status",
            ),
            (
                r#"{"id":ID,"result":{"thread":{"id":"0199a3b2-6c1d-7e00-8f00-000000000000","status":{"type":"idle"}}}}"#,
                "another thread",
            ),
            (
                r#"{"id":ID,"error":{"code":-32600,"message":"thread not found"}}"#,
                "thread not found",
            ),
            (r#"{"id":ID,"result":{}}"#, "no thread"),
            (r#"{"id":999,"result":{}}"#, "unknown request"),
            ("not json", "malformed"),
        ] {
            let layout = Layout::new();
            let (outcome, _) = run(&layout, vec![vec![Reply::Raw(raw)]], 0).await;
            match outcome {
                Err(Refusal::Definite(reason)) => assert!(reason.contains(detail), "{reason}"),
                other => panic!("{raw}: {other:?}"),
            }
        }
    }

    #[tokio::test]
    async fn transient_losses_reconnect_and_never_keep_an_old_idle() {
        let layout = Layout::new();
        // Connection 0 hangs up, 1 never answers (read deadline), 2 is idle.
        let script = vec![
            vec![Reply::Hangup],
            vec![Reply::Silent],
            vec![Reply::Status("idle")],
        ];
        let (outcome, log) = run(&layout, script, 0).await;
        assert_eq!(outcome, Ok(()));
        let log = reads(&log);
        assert_eq!(log.last(), Some(&(2, "thread/read".to_string())));
        assert_eq!(log.iter().filter(|(n, _)| *n == 2).count(), 3);
    }

    #[tokio::test]
    async fn a_missing_socket_is_waited_for() {
        let layout = Layout::new();
        let log = Log::default();
        let endpoint = &layout.endpoint;
        let later = async {
            tokio::time::sleep(Duration::from_millis(150)).await;
            serve(
                layout.bind(),
                vec![vec![Reply::Status("idle")]],
                log.clone(),
            );
        };
        let (outcome, ()) = tokio::join!(wait(endpoint, THREAD, 0, fast(), mine), later);
        assert_eq!(outcome, Ok(()));
        assert_eq!(reads(&log).len(), 3);
    }

    #[tokio::test]
    async fn a_dangling_alias_is_transient_until_the_deadline() {
        let layout = Layout::new();
        drop(layout.bind());
        std::fs::remove_file(layout.socket()).unwrap();
        let started = Instant::now();
        let outcome = wait(&layout.endpoint, THREAD, 1, fast(), mine).await;
        assert_eq!(
            outcome,
            Err(Refusal::Deadline {
                seconds: 1,
                last: "control alias is dangling".into()
            })
        );
        let elapsed = started.elapsed();
        assert!(elapsed >= Duration::from_secs(1) && elapsed < Duration::from_secs(2));
    }

    #[tokio::test]
    async fn an_unrepresentable_deadline_is_refused_before_connecting() {
        let layout = Layout::new();
        let log = Log::default();
        serve(
            layout.bind(),
            vec![vec![Reply::Status("idle")]],
            log.clone(),
        );
        let started = Instant::now();
        let outcome = wait(&layout.endpoint, THREAD, u64::MAX, fast(), mine).await;
        assert!(matches!(outcome, Err(Refusal::Definite(r)) if r.contains("is too large")));
        assert!(started.elapsed() < Duration::from_secs(1));
        assert!(reads(&log).is_empty());
    }

    #[tokio::test]
    async fn the_deadline_includes_waiting_while_active() {
        let layout = Layout::new();
        let started = Instant::now();
        let (outcome, _) = run(&layout, vec![vec![Reply::Status("active")]], 1).await;
        assert!(matches!(outcome, Err(Refusal::Deadline { seconds: 1, .. })));
        assert!(started.elapsed() < Duration::from_secs(2));
    }

    #[tokio::test]
    async fn unsafe_endpoints_refuse() {
        // Alias that is a plain socket rather than the daemon link.
        let layout = Layout::new();
        let _listener = UnixListener::bind(&layout.endpoint.alias).unwrap();
        let outcome = wait(&layout.endpoint, THREAD, 0, fast(), mine).await;
        assert!(matches!(outcome, Err(Refusal::Definite(r)) if r.contains("daemon link")));
        // Alias naming another socket.
        let layout = Layout::new();
        let other = layout.endpoint.daemon_dir.join("other");
        let _listener = UnixListener::bind(&other).unwrap();
        std::os::unix::fs::symlink(&other, &layout.endpoint.alias).unwrap();
        let outcome = wait(&layout.endpoint, THREAD, 0, fast(), mine).await;
        assert!(matches!(outcome, Err(Refusal::Definite(r)) if r.contains("another socket")));
        // A shared daemon directory.
        let layout = Layout::new();
        let _listener = layout.bind();
        let dir = &layout.endpoint.daemon_dir;
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o755)).unwrap();
        let outcome = wait(&layout.endpoint, THREAD, 0, fast(), mine).await;
        assert!(matches!(outcome, Err(Refusal::Definite(r)) if r.contains("not private")));
        // Another user's endpoint.
        let layout = Layout::new();
        serve(
            layout.bind(),
            vec![vec![Reply::Status("idle")]],
            Log::default(),
        );
        let mut endpoint = layout.endpoint.clone();
        endpoint.uid += 1;
        let outcome = wait(&endpoint, THREAD, 0, fast(), mine).await;
        assert!(matches!(outcome, Err(Refusal::Definite(r)) if r.contains("this user")));
    }

    #[tokio::test]
    async fn a_replaced_endpoint_invalidates_idle() {
        let layout = Layout::new();
        let log = Log::default();
        serve(
            layout.bind(),
            vec![vec![Reply::Status("idle")]],
            log.clone(),
        );
        let replaced = std::sync::atomic::AtomicBool::new(false);
        // The first idle answer is seen after the daemon was replaced: the
        // check re-resolves the socket, so idle is observed again on the new one.
        let still_ours = || {
            if !replaced.swap(true, std::sync::atomic::Ordering::SeqCst) {
                let log = log.clone();
                serve(layout.bind(), vec![vec![Reply::Status("idle")]], log);
            }
            Ok(())
        };
        let outcome = wait(&layout.endpoint, THREAD, 0, fast(), still_ours).await;
        assert_eq!(outcome, Ok(()));
        let log = reads(&log);
        let second = log
            .iter()
            .filter(|(n, m)| *n == 0 && m == "thread/read")
            .count();
        assert_eq!(second, 2, "{log:?}");
    }

    #[tokio::test]
    async fn a_gone_subscriber_refuses_at_idle() {
        let layout = Layout::new();
        serve(
            layout.bind(),
            vec![vec![Reply::Status("idle")]],
            Log::default(),
        );
        let gone = || Err("subscriber 1 is gone (parent is 2)".to_string());
        let outcome = wait(&layout.endpoint, THREAD, 0, fast(), gone).await;
        assert!(matches!(outcome, Err(Refusal::Definite(r)) if r.contains("is gone")));
    }

    #[test]
    fn local_endpoint_follows_codex_home() {
        // Only path computation; nothing is created or connected.
        let endpoint = Endpoint::local().unwrap();
        assert!(
            endpoint
                .alias
                .ends_with("app-server-control/app-server-control.sock")
        );
        assert!(
            endpoint
                .daemon_dir
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with("codex-daemon-")
        );
    }

    #[test]
    fn unrelated_notifications_are_not_parsed_for_content() {
        assert!(matches!(
            classify(r#"{"method":"item/agentMessage/delta","params":"anything"}"#),
            Ok(Incoming::Other)
        ));
        assert!(matches!(
            classify(r#"{"id":4,"method":"item/tool/requestUserInput","params":{}}"#),
            Ok(Incoming::Other)
        ));
        assert!(matches!(
            classify(r#"{"method":"thread/status/changed","params":{}}"#),
            Err(Failure::Definite(_))
        ));
    }
}
