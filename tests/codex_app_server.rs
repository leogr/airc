#![cfg(unix)]

//! Real adapter processes, simulated app server. No Codex conversation is used.
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::{
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::PathBuf,
    process::Stdio,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::AsyncWriteExt,
    net::UnixListener,
    process::{Child, Command},
    time::timeout,
};
use tokio_tungstenite::{accept_async, tungstenite::Message};

const THREAD: &str = "0199a3b2-6c1d-7e00-8f00-000000000001";
const TURN: &str = "0199a3b2-6c1d-7e00-8f00-000000000002";
const FIRST: &str = "0199a3b2-6c1d-7e00-8f00-000000000003";
const SECOND: &str = "0199a3b2-6c1d-7e00-8f00-000000000004";

struct Fixture {
    socket: PathBuf,
    socket_identity: (u64, u64),
    state: tempfile::TempDir,
}
impl Fixture {
    fn new() -> (Self, UnixListener) {
        let uid = rustix::process::geteuid().as_raw();
        let daemon = std::fs::canonicalize("/tmp")
            .unwrap()
            .join(format!("codex-daemon-{uid}"));
        match std::fs::DirBuilder::new().mode(0o700).create(&daemon) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(e) => panic!("{e}"),
        }
        let metadata = std::fs::symlink_metadata(&daemon).unwrap();
        assert!(metadata.is_dir() && metadata.uid() == uid && metadata.mode() & 0o777 == 0o700);
        // Only this random fixture socket is owned/removed, never the shared
        // daemon directory or an existing daemon's endpoint.
        let socket = daemon.join(format!(
            "{}{}",
            uuid::Uuid::new_v4().simple(),
            uuid::Uuid::new_v4().simple()
        ));
        let listener = UnixListener::bind(&socket).unwrap();
        std::fs::set_permissions(&socket, std::fs::Permissions::from_mode(0o600)).unwrap();
        let meta = std::fs::symlink_metadata(&socket).unwrap();
        let state = tempfile::tempdir().unwrap();
        std::fs::set_permissions(state.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        (
            Self {
                socket,
                socket_identity: (meta.dev(), meta.ino()),
                state,
            },
            listener,
        )
    }
    async fn child(&self, id: &str, text: &str) -> Child {
        let mut child = Command::new(env!("CARGO_BIN_EXE_airc-codex"))
            .args(["--thread", THREAD, "--app-server", "--app-server-socket"])
            .arg(&self.socket)
            .args(["--codex", "/never-spawn-codex"])
            .env("AIRC_DELIVERY_ID", id)
            .env("AIRC_SUBSCRIBER_PID", std::process::id().to_string())
            .env("AIRC_DELIVERY_STATE_DIR", self.state.path())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let batch = format!(
            "[airc id={id} session=trial]\nm:Ej5FZ-ibQtOkVkJmFBdAAA #trial human(account): @worker {text}\n"
        );
        child
            .stdin
            .take()
            .unwrap()
            .write_all(batch.as_bytes())
            .await
            .unwrap();
        child
    }
    fn record(&self, id: &str) -> Value {
        serde_json::from_slice(
            &std::fs::read(
                self.state
                    .path()
                    .join("codex-app-server")
                    .join(format!("{id}.json")),
            )
            .unwrap(),
        )
        .unwrap()
    }
}
use std::os::unix::fs::DirBuilderExt;
impl Drop for Fixture {
    fn drop(&mut self) {
        if std::fs::symlink_metadata(&self.socket)
            .is_ok_and(|m| (m.dev(), m.ino()) == self.socket_identity)
        {
            std::fs::remove_file(&self.socket).unwrap();
        }
    }
}

async fn finish(child: Child) -> std::process::Output {
    timeout(Duration::from_secs(10), child.wait_with_output())
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn process_receipts_settle_once_and_later_batches_steer_in_order() {
    let (fixture, listener) = Fixture::new();
    let accepted = Arc::new(AtomicUsize::new(0));
    let count = accepted.clone();
    let task = tokio::spawn(async move {
        for (batch, route, text) in [
            (FIRST, "turn/start", "first literal $(no shell)"),
            (SECOND, "turn/steer", "second"),
        ] {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = accept_async(stream).await.unwrap();
            loop {
                let request = ws.next().await.unwrap().unwrap().into_text().unwrap();
                let value: Value = serde_json::from_str(&request).unwrap();
                let method = value["method"].as_str().unwrap();
                let result = match method {
                    "initialize" => json!({"userAgent":"fixture"}),
                    "initialized" => continue,
                    "thread/read" => {
                        assert_eq!(value["params"]["threadId"], THREAD);
                        json!({"thread":{"id":THREAD,"status":{"type":if route=="turn/start" {"idle"} else {"active"}}}})
                    }
                    "thread/turns/list" => {
                        json!({"data":[{"id":TURN,"status":"inProgress"}],"nextCursor":null})
                    }
                    "turn/start" | "turn/steer" => {
                        assert_eq!(method, route);
                        assert_eq!(
                            value["params"].as_object().unwrap().len(),
                            if route == "turn/start" { 2 } else { 3 }
                        );
                        assert!(
                            value["params"]["input"][0]["text"]
                                .as_str()
                                .unwrap()
                                .contains(&format!("id={batch}"))
                        );
                        assert!(
                            value["params"]["input"][0]["text"]
                                .as_str()
                                .unwrap()
                                .contains(text)
                        );
                        if route == "turn/steer" {
                            assert_eq!(value["params"]["expectedTurnId"], TURN);
                        }
                        count.fetch_add(1, Ordering::SeqCst);
                        if route == "turn/start" {
                            json!({"turn":{"id":TURN,"status":"inProgress"}})
                        } else {
                            json!({"turnId":TURN})
                        }
                    }
                    other => panic!("unexpected method {other}"),
                };
                ws.send(Message::text(
                    json!({"id":value["id"],"result":result}).to_string(),
                ))
                .await
                .unwrap();
                if method == route {
                    break;
                }
            }
        }
    });
    let first = finish(fixture.child(FIRST, "first literal $(no shell)").await).await;
    assert_eq!(
        first.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&first.stderr)
    );
    assert_eq!(fixture.record(FIRST)["phase"], "accepted");
    let replay = finish(fixture.child(FIRST, "changed retry rendering").await).await;
    assert_eq!(
        replay.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&replay.stderr)
    );
    assert!(String::from_utf8_lossy(&replay.stderr).contains("no submission repeated"));
    let second = finish(fixture.child(SECOND, "second").await).await;
    assert_eq!(
        second.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&second.stderr)
    );
    timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(accepted.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn termination_after_mutation_never_interrupts_or_resends_the_turn() {
    let (fixture, listener) = Fixture::new();
    let (sent, received) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut ws = accept_async(stream).await.unwrap();
        loop {
            let request = ws.next().await.unwrap().unwrap().into_text().unwrap();
            let value: Value = serde_json::from_str(&request).unwrap();
            match value["method"].as_str().unwrap() {
                "initialized"=>continue,
                "turn/start"=>{sent.send(()).unwrap();break;},
                "initialize"=>ws.send(Message::text(json!({"id":value["id"],"result":{}}).to_string())).await.unwrap(),
                "thread/read"=>ws.send(Message::text(json!({"id":value["id"],"result":{"thread":{"id":THREAD,"status":{"type":"idle"}}}}).to_string())).await.unwrap(),
                method=>panic!("unexpected method {method}"),
            }
        }
        // No receipt is returned. Cancellation may close transport but must
        // never issue turn/interrupt or any second mutation.
        while let Some(Ok(message)) = ws.next().await {
            if let Message::Text(text) = message {
                panic!("unexpected post-submit RPC {text}");
            }
        }
    });
    let child = fixture.child(FIRST, "cancelled sender").await;
    timeout(Duration::from_secs(5), received)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(fixture.record(FIRST)["phase"], "in_flight");
    let pid = rustix::process::Pid::from_raw(child.id().unwrap() as i32).unwrap();
    rustix::process::kill_process(pid, rustix::process::Signal::TERM).unwrap();
    let output = finish(child).await;
    assert_eq!(
        output.status.code(),
        Some(4),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap();
    let retry = finish(fixture.child(FIRST, "retry after lost receipt").await).await;
    assert_eq!(
        retry.status.code(),
        Some(4),
        "{}",
        String::from_utf8_lossy(&retry.stderr)
    );
    assert!(String::from_utf8_lossy(&retry.stderr).contains("nothing resent"));
}
