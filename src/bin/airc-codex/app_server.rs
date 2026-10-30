//! Opt-in, bounded app-server delivery. The legacy queue path does not call it.
//! No daemon start, thread resume, permission override or approval response.

#[path = "app_server/journal.rs"]
mod journal;

use futures_util::{SinkExt, StreamExt};
use journal::{Attempt, Journal, Phase};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    os::unix::fs::{FileTypeExt, MetadataExt},
    path::{Path, PathBuf},
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};
use tokio::{net::UnixStream, time::Instant};
use tokio_tungstenite::{
    WebSocketStream, client_async_with_config,
    tungstenite::{Message, protocol::WebSocketConfig},
};

const RPC_TIMEOUT: Duration = Duration::from_secs(5);
const FRAME_LIMIT: usize = 4 << 20;
const PAGE_SIZE: usize = 16;
const MAX_PAGES: usize = 64;

#[derive(Clone, Debug, PartialEq, Eq)]
struct Identity {
    path: PathBuf,
    dev: u64,
    ino: u64,
}

impl Identity {
    fn inspect(path: &Path, daemon_dir: &Path, uid: u32) -> Result<Self, String> {
        if !path.is_absolute()
            || path.parent() != Some(daemon_dir)
            || !path.file_name().is_some_and(|name| {
                name.to_str().is_some_and(|s| {
                    s.len() == 64
                        && s.bytes()
                            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
                })
            })
        {
            return Err("--app-server-socket must name the verified local daemon socket, not a remote endpoint or another directory".into());
        }
        let dir = std::fs::symlink_metadata(daemon_dir)
            .map_err(|e| format!("daemon directory: {}", e.kind()))?;
        if !dir.is_dir() || dir.uid() != uid || dir.mode() & 0o777 != 0o700 {
            return Err("daemon directory is not private to the current user".into());
        }
        let socket =
            std::fs::symlink_metadata(path).map_err(|e| format!("daemon socket: {}", e.kind()))?;
        if !socket.file_type().is_socket() || socket.uid() != uid || socket.mode() & 0o777 != 0o600
        {
            return Err("daemon endpoint is not this user's private socket".into());
        }
        Ok(Self {
            path: path.to_path_buf(),
            dev: socket.dev(),
            ino: socket.ino(),
        })
    }
}

#[derive(Debug)]
enum Failure {
    Transport(&'static str),
    Protocol(&'static str),
    Rejected(i64, String),
}

impl std::fmt::Display for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Transport(reason) | Self::Protocol(reason) => f.write_str(reason),
            // Do not print arbitrary app-server errors or message bodies.
            Self::Rejected(code, _) => write!(f, "app server rejected RPC with code {code}"),
        }
    }
}

struct Rpc {
    ws: WebSocketStream<UnixStream>,
    next_id: u64,
    timeout: Duration,
}

impl Rpc {
    async fn connect(identity: &Identity, uid: u32, timeout: Duration) -> Result<Self, Failure> {
        let stream = tokio::time::timeout(timeout, UnixStream::connect(&identity.path))
            .await
            .map_err(|_| Failure::Transport("connect timed out"))?
            .map_err(|_| Failure::Transport("cannot connect to existing daemon"))?;
        if stream
            .peer_cred()
            .map_err(|_| Failure::Protocol("cannot verify daemon peer"))?
            .uid()
            != uid
        {
            return Err(Failure::Protocol("daemon peer belongs to another user"));
        }
        let config = WebSocketConfig::default()
            .max_message_size(Some(FRAME_LIMIT))
            .max_frame_size(Some(FRAME_LIMIT));
        let (ws, _) = tokio::time::timeout(
            timeout,
            client_async_with_config("ws://localhost/rpc", stream, Some(config)),
        )
        .await
        .map_err(|_| Failure::Transport("handshake timed out"))?
        .map_err(|_| Failure::Transport("existing daemon handshake failed"))?;
        let mut rpc = Self {
            ws,
            next_id: 0,
            timeout,
        };
        rpc.call("initialize", json!({"clientInfo":{"name":"airc-codex","version":env!("CARGO_PKG_VERSION")},"capabilities":{"experimentalApi":true}})).await?;
        rpc.send(json!({"method":"initialized"})).await?;
        Ok(rpc)
    }

    async fn send(&mut self, value: Value) -> Result<(), Failure> {
        tokio::time::timeout(self.timeout, self.ws.send(Message::text(value.to_string())))
            .await
            .map_err(|_| Failure::Transport("app-server write timed out"))?
            .map_err(|_| Failure::Transport("app-server write failed"))
    }

    async fn call(&mut self, method: &str, params: Value) -> Result<Value, Failure> {
        self.next_id = self
            .next_id
            .checked_add(1)
            .ok_or(Failure::Protocol("request ID overflow"))?;
        let id = self.next_id;
        self.send(json!({"id":id,"method":method,"params":params}))
            .await?;
        let deadline = Instant::now() + self.timeout;
        loop {
            let incoming = tokio::time::timeout_at(deadline, self.ws.next())
                .await
                .map_err(|_| Failure::Transport("app-server reply timed out"))?;
            let text = match incoming {
                Some(Ok(Message::Text(text))) => text,
                Some(Ok(Message::Ping(_) | Message::Pong(_) | Message::Frame(_))) => continue,
                Some(Ok(Message::Binary(_))) => {
                    return Err(Failure::Protocol("binary app-server message"));
                }
                _ => return Err(Failure::Transport("app-server connection ended")),
            };
            let value: Value = serde_json::from_str(text.as_str())
                .map_err(|_| Failure::Protocol("malformed JSON-RPC"))?;
            let object = value
                .as_object()
                .ok_or(Failure::Protocol("non-object JSON-RPC"))?;
            // No approval requests or notifications are answered or logged.
            if object.contains_key("method") {
                continue;
            }
            if value.get("id").and_then(Value::as_u64) != Some(id) {
                return Err(Failure::Protocol("reply names another request"));
            }
            match (object.get("result"), object.get("error")) {
                (Some(result), None) => return Ok(result.clone()),
                (None, Some(error)) => {
                    let code = error
                        .get("code")
                        .and_then(Value::as_i64)
                        .ok_or(Failure::Protocol("malformed RPC error"))?;
                    let message = error
                        .get("message")
                        .and_then(Value::as_str)
                        .ok_or(Failure::Protocol("malformed RPC error"))?;
                    return Err(Failure::Rejected(code, message.chars().take(200).collect()));
                }
                _ => return Err(Failure::Protocol("ambiguous JSON-RPC reply")),
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
enum Route {
    Start,
    Steer(String),
}

fn turn_id(value: &Value) -> Option<String> {
    let id = value.as_str()?;
    let uuid = uuid::Uuid::parse_str(id).ok()?;
    (!uuid.is_nil() && uuid.hyphenated().to_string() == id).then(|| id.to_string())
}

async fn route(rpc: &mut Rpc, thread: &str) -> Result<Route, String> {
    let result = rpc
        .call(
            "thread/read",
            json!({"threadId":thread,"includeTurns":false}),
        )
        .await
        .map_err(|e| e.to_string())?;
    if result.pointer("/thread/id").and_then(Value::as_str) != Some(thread) {
        return Err("thread/read answered for another thread".into());
    }
    match result
        .pointer("/thread/status/type")
        .and_then(Value::as_str)
    {
        Some("idle") => return Ok(Route::Start),
        Some("active") => {}
        _ => return Err("target thread is not loaded and usable on this daemon".into()),
    }
    let mut cursor = None;
    let mut seen = std::collections::HashSet::new();
    for _ in 0..MAX_PAGES {
        let mut params = json!({"threadId":thread,"limit":PAGE_SIZE,"sortDirection":"desc","itemsView":"notLoaded"});
        if let Some(cursor) = &cursor {
            params["cursor"] = json!(cursor);
        }
        let page = rpc
            .call("thread/turns/list", params)
            .await
            .map_err(|e| e.to_string())?;
        let turns = page
            .get("data")
            .and_then(Value::as_array)
            .ok_or("turn page has no data array")?;
        if turns.len() > PAGE_SIZE {
            return Err("turn page exceeds requested bound".into());
        }
        let mut active = None;
        for turn in turns {
            let id = turn
                .get("id")
                .and_then(turn_id)
                .ok_or("turn page has an invalid ID")?;
            if !seen.insert(id.clone()) {
                return Err("turn page repeats an ID".into());
            }
            match turn.get("status").and_then(Value::as_str) {
                Some("inProgress") => {
                    if active.replace(id).is_some() {
                        return Err("multiple active turns reported".into());
                    }
                }
                Some("completed" | "failed" | "interrupted") => {}
                _ => return Err("turn page has an unknown status".into()),
            }
        }
        if let Some(id) = active {
            return Ok(Route::Steer(id));
        }
        match page.get("nextCursor") {
            Some(Value::Null) => {
                return Err("active thread has no discoverable active turn ID".into());
            }
            Some(Value::String(next)) if !next.is_empty() && next.len() <= 4096 => {
                if cursor.as_ref() == Some(next) {
                    return Err("turn cursor did not advance".into());
                }
                cursor = Some(next.clone());
            }
            _ => return Err("turn page has no valid cursor".into()),
        }
    }
    Err("active-turn history exceeded the bounded page budget".into())
}

#[derive(Debug)]
enum Outcome {
    Accepted(String, &'static str),
    NotSent(String),
    Uncertain(String),
}

struct Delivery<'a> {
    journal: &'a Journal,
    identity: &'a Identity,
    daemon_dir: &'a Path,
    thread: &'a str,
    batch_id: &'a str,
    batch: &'a str,
    uid: u32,
    inflight: &'a AtomicBool,
    still_ours: &'a dyn Fn() -> Result<(), String>,
    timeout: Duration,
}

async fn deliver(delivery: Delivery<'_>) -> Outcome {
    let Delivery {
        journal,
        identity,
        daemon_dir,
        thread,
        batch_id,
        batch,
        uid,
        inflight,
        still_ours,
        timeout,
    } = delivery;
    let mut rpc = match Rpc::connect(identity, uid, timeout).await {
        Ok(rpc) => rpc,
        Err(error) => return Outcome::NotSent(error.to_string()),
    };
    for retry in 0..2 {
        let route = match route(&mut rpc, thread).await {
            Ok(route) => route,
            Err(error) => return Outcome::NotSent(error),
        };
        if let Err(error) = still_ours() {
            return Outcome::NotSent(error);
        }
        match Identity::inspect(&identity.path, daemon_dir, uid) {
            Ok(current) if current == *identity => {}
            _ => return Outcome::NotSent("daemon socket changed before submission".into()),
        }
        let (method, expected_turn) = match route {
            Route::Start => ("turn/start", None),
            Route::Steer(id) => ("turn/steer", Some(id)),
        };
        let mut params = json!({"threadId":thread,"input":[{"type":"text","text":batch}]});
        if let Some(id) = &expected_turn {
            params["expectedTurnId"] = json!(id);
        }
        let mut record = Attempt {
            version: 1,
            batch: batch_id.into(),
            thread: thread.into(),
            payload_sha256: format!("{:x}", Sha256::digest(batch.as_bytes())),
            endpoint: identity.path.clone(),
            endpoint_dev: identity.dev,
            endpoint_ino: identity.ino,
            phase: Phase::InFlight,
            method: method.into(),
            rpc_id: rpc.next_id + 1,
            expected_turn,
            accepted_turn: None,
        };
        if let Err(error) = journal.save(&record) {
            return Outcome::NotSent(format!("cannot persist pre-submit identity: {error}"));
        }
        inflight.store(true, Ordering::SeqCst);
        let response = rpc.call(method, params).await;
        let receipt = match response {
            Ok(value) => match method {
                "turn/start"
                    if value.pointer("/turn/status").and_then(Value::as_str)
                        == Some("inProgress") =>
                {
                    value.pointer("/turn/id").and_then(turn_id)
                }
                "turn/steer" => value
                    .get("turnId")
                    .and_then(turn_id)
                    .filter(|id| Some(id) == record.expected_turn.as_ref()),
                _ => None,
            },
            Err(Failure::Rejected(-32600, message))
                if method == "turn/steer" && message == "no active turn to steer" =>
            {
                // Only the exact proved pre-effect rejection allows one fresh
                // classification. Unknown errors never cause another submission.
                record.phase = Phase::NotSent;
                if let Err(error) = journal.save(&record) {
                    return Outcome::Uncertain(format!("cannot persist rejection: {error}"));
                }
                inflight.store(false, Ordering::SeqCst);
                if retry == 0 {
                    continue;
                }
                return Outcome::NotSent("thread changed twice before admission; no retry".into());
            }
            Err(error) => return Outcome::Uncertain(error.to_string()),
        };
        let Some(turn) = receipt else {
            return Outcome::Uncertain("submission has no matching typed receipt".into());
        };
        record.phase = Phase::Accepted;
        record.accepted_turn = Some(turn.clone());
        if let Err(error) = journal.save(&record) {
            return Outcome::Uncertain(format!("receipt was not durably recorded: {error}"));
        }
        return Outcome::Accepted(turn, method);
    }
    unreachable!("bounded route loop returns")
}

/// Called only for the new explicit mode, after the original batch and parent
/// checks. Retained receipts settle acceptance without sending a second RPC;
/// in-flight records stop even an explicitly retried subscriber.
pub fn run(
    socket: &Path,
    state_dir: &Path,
    thread: &str,
    batch_id: &str,
    batch: &str,
    still_ours: impl Fn() -> Result<(), String>,
) -> u8 {
    let journal = match Journal::open(state_dir) {
        Ok(journal) => journal,
        Err(error) => {
            eprintln!("airc-codex: app-server journal refused: {error}");
            return super::NOT_SENT;
        }
    };
    match journal.load(batch_id) {
        Ok(Some(record)) if record.thread != thread => {
            eprintln!("airc-codex: pending batch is bound to another thread; nothing resent");
            return super::UNCERTAIN;
        }
        Ok(Some(record)) if record.phase == Phase::Accepted => {
            eprintln!(
                "airc-codex: delivery {batch_id}: already accepted on thread {thread}, turn {}; no submission repeated",
                record.accepted_turn.unwrap_or_default()
            );
            return 0;
        }
        Ok(Some(record)) if record.phase == Phase::InFlight => {
            eprintln!(
                "airc-codex: delivery {batch_id} has an in-flight/uncertain submission; inspect the journal and thread; nothing resent"
            );
            return super::UNCERTAIN;
        }
        Ok(_) => {}
        Err(error) => {
            eprintln!(
                "airc-codex: journal load refused: {error}; prior submission unknown; nothing resent"
            );
            return super::UNCERTAIN;
        }
    }
    let uid = rustix::process::geteuid().as_raw();
    let daemon_dir = match std::fs::canonicalize("/tmp") {
        Ok(tmp) => tmp.join(format!("codex-daemon-{uid}")),
        Err(error) => {
            eprintln!(
                "airc-codex: local daemon directory unavailable: {}",
                error.kind()
            );
            return super::NOT_SENT;
        }
    };
    let identity = match Identity::inspect(socket, &daemon_dir, uid) {
        Ok(identity) => identity,
        Err(error) => {
            eprintln!("airc-codex: app-server endpoint refused: {error}");
            return super::NOT_SENT;
        }
    };
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("airc-codex: cannot start app-server client: {error}");
            return super::NOT_SENT;
        }
    };
    let inflight = AtomicBool::new(false);
    let outcome = runtime.block_on(async {
        use tokio::signal::unix::{SignalKind, signal};
        let signals = (signal(SignalKind::terminate()), signal(SignalKind::interrupt()), signal(SignalKind::hangup()));
        let (Ok(mut term), Ok(mut int), Ok(mut hup)) = signals else { return Outcome::NotSent("cannot install cancellation handlers".into()); };
        tokio::select! {
            result = deliver(Delivery { journal: &journal, identity: &identity, daemon_dir: &daemon_dir, thread, batch_id, batch, uid, inflight: &inflight, still_ours: &still_ours, timeout: RPC_TIMEOUT }) => result,
            _ = async { tokio::select! { _ = term.recv() => {}, _ = int.recv() => {}, _ = hup.recv() => {} } } => {
                if inflight.load(Ordering::SeqCst) { Outcome::Uncertain("cancelled after submission permission; nothing resent".into()) }
                else { Outcome::NotSent("cancelled before submission".into()) }
            }
        }
    });
    match outcome {
        Outcome::Accepted(turn, method) => {
            eprintln!(
                "airc-codex: delivery {batch_id} accepted via {method} on thread {thread}, turn {turn}; not a model acknowledgment"
            );
            0
        }
        Outcome::NotSent(reason) => {
            eprintln!("airc-codex: app-server delivery not sent: {reason}");
            super::NOT_SENT
        }
        Outcome::Uncertain(reason) => {
            eprintln!(
                "airc-codex: app-server delivery uncertain: {reason}; inspect the retained pending batch and journal; nothing retried"
            );
            super::UNCERTAIN
        }
    }
}

#[cfg(test)]
#[path = "app_server/tests.rs"]
mod tests;
