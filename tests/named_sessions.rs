#![cfg(unix)]

use airc::{
    client::{self, ClientConfig},
    config::{Account, Channel, Config, Limits, token_hash, write_private},
    protocol::{Message, read_line},
    server::{self, RunningServer},
};
use serde_json::json;
use std::{
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::PathBuf,
    process::{Output, Stdio},
    time::Duration,
};
use tokio::{
    io::{AsyncWriteExt, BufReader},
    net::UnixStream,
    time::{Instant, timeout},
};

const TOKEN: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

/// The shared `m:` reference of a UUID msgid, computed independently of AIRC.
fn shared(msgid: &str) -> String {
    use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
    let uuid = uuid::Uuid::parse_str(msgid).unwrap();
    assert_eq!(
        uuid.hyphenated().to_string(),
        msgid,
        "canonical server msgid"
    );
    format!("m:{}", URL_SAFE_NO_PAD.encode(uuid.as_bytes()))
}

struct Fixture {
    dir: tempfile::TempDir,
    config: Config,
    root: PathBuf,
}
impl Fixture {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("sessions");
        let config = Config {
            server_name: "test.airc".into(),
            listen: Some("127.0.0.1:0".parse().unwrap()),
            listen_unix: None,
            control_unix: None,
            membership_generation: None,
            database: dir.path().join("state.sqlite3"),
            allow_insecure: false,
            allow_private_messages: false,
            removed_channels: Vec::new(),
            tls: None,
            artifacts: None,
            limits: Limits {
                history_per_channel: 4,
                ..Limits::default()
            },
            accounts: ["alice", "human"]
                .map(|name| Account {
                    name: name.into(),
                    token_sha256: token_hash(TOKEN),
                })
                .to_vec(),
            channels: vec![Channel {
                name: "#work".into(),
                members: vec!["alice".into(), "human".into()],
                operators: vec!["human".into()],
                moderated: false,
            }],
        };
        Self { dir, config, root }
    }
    async fn start(&self) -> RunningServer {
        server::start(self.config.clone()).await.unwrap()
    }
    fn client(&self, server: &RunningServer, name: &str) -> ClientConfig {
        let token = self.dir.path().join(format!("{name}.token"));
        if !token.exists() {
            write_private(&token, TOKEN.as_bytes()).unwrap();
        }
        ClientConfig {
            host: "127.0.0.1".into(),
            port: server.address.unwrap().port(),
            tls: false,
            allow_insecure: false,
            ca_certificate: None,
            proxy: None,
            account: name.into(),
            nick: name.into(),
            token_file: token,
            channels: vec!["#work".into()],
            artifact_url: None,
            unix: None,
            artifact_unix: None,
            regain: false,
        }
    }
    fn path(&self, server: &RunningServer) -> PathBuf {
        let path = self.dir.path().join("alice.toml");
        if !path.exists() {
            write_private(
                &path,
                toml::to_string(&self.client(server, "alice"))
                    .unwrap()
                    .as_bytes(),
            )
            .unwrap();
        }
        path
    }
    async fn cli(&self, args: &[&str]) -> Output {
        timeout(
            Duration::from_secs(15),
            tokio::process::Command::new(env!("CARGO_BIN_EXE_airc"))
                .arg("--state-dir")
                .arg(&self.root)
                .args(args)
                .stdin(Stdio::null())
                .kill_on_drop(true)
                .output(),
        )
        .await
        .unwrap()
        .unwrap()
    }
    async fn ok(&self, args: &[&str]) -> String {
        let output = self.cli(args).await;
        assert!(
            output.status.success(),
            "args={args:?}, stdout={}, stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap()
    }
    async fn connect(&self, server: &RunningServer) {
        self.ok(&[
            "connect",
            "trial",
            "--config",
            self.path(server).to_str().unwrap(),
        ])
        .await;
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        // Explicitly close the disposable children even when an assertion panics.
        for session in ["trial", "peer"] {
            let _ = std::process::Command::new(env!("CARGO_BIN_EXE_airc"))
                .arg("--state-dir")
                .arg(&self.root)
                .args(["disconnect", session])
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
        }
    }
}

async fn next(connection: &mut client::Connection, command: &str) -> Message {
    timeout(Duration::from_secs(5), async {
        loop {
            let m = connection.inbound.recv().await.unwrap();
            if m.command == command {
                break m;
            }
        }
    })
    .await
    .unwrap()
}

async fn send(connection: &mut client::Connection, text: &str) -> Message {
    connection
        .outbound
        .send(vec![Message::new("PRIVMSG", ["#work", text])])
        .await
        .unwrap();
    next(connection, "PRIVMSG").await
}

#[tokio::test]
async fn named_connection_send_reply_read_replay_and_stopped_lifecycle() {
    let f = Fixture::new();
    let server = f.start().await;
    let mut human = client::connect(&f.client(&server, "human")).await.unwrap();
    let before = send(&mut human, "before connect").await;
    let root = shared(&before.tags["msgid"]);
    f.connect(&server).await;
    let output = f.ok(&["read", "trial"]).await;
    assert!(
        output.contains(&format!(
            "history {root} #work human(human): before connect"
        )),
        "{output}"
    );
    assert_eq!(f.ok(&["read", "trial", "--again"]).await, output);
    // A local number is refused before anything is sent.
    let local = f
        .cli(&["send", "trial", "#work", "identical", "--reply", "@1"])
        .await;
    assert!(!local.status.success());
    assert!(
        text(&local).starts_with("! not sent: --reply local @N")
            && text(&local).contains("use the message's m: reference"),
        "{}",
        text(&local)
    );
    let confirmed = f
        .ok(&["send", "trial", "#work", "identical", "--reply", &root])
        .await;
    let first = next(&mut human, "PRIVMSG").await;
    assert_eq!(first.params[1], "identical");
    assert_eq!(first.tags["+reply"], before.tags["msgid"]);
    assert!(
        confirmed.contains(&format!("confirmed #work {}", shared(&first.tags["msgid"]))),
        "{confirmed}"
    );
    let confirmed = f.ok(&["send", "trial", "#work", "identical"]).await;
    let second = next(&mut human, "PRIVMSG").await;
    assert!(
        confirmed.contains(&format!(
            "confirmed #work {}",
            shared(&second.tags["msgid"])
        )),
        "{confirmed}"
    );
    assert_ne!(first.tags["msgid"], second.tags["msgid"]);
    assert_ne!(first.tags["+airc-send"], second.tags["+airc-send"]);
    assert!(f.ok(&["read", "trial"]).await.contains("No new events"));
    let before = Instant::now();
    let duplicate = f
        .cli(&[
            "connect",
            "trial",
            "--config",
            f.path(&server).to_str().unwrap(),
        ])
        .await;
    assert!(!duplicate.status.success());
    assert!(before.elapsed() < Duration::from_secs(2));
    let rejected = f.cli(&["send", "trial", "human", "private denied"]).await;
    assert!(!rejected.status.success());
    assert!(
        String::from_utf8_lossy(&rejected.stdout).contains("rejected by server"),
        "{}",
        String::from_utf8_lossy(&rejected.stdout)
    );
    assert!(
        !String::from_utf8_lossy(&rejected.stdout).contains("\\n"),
        "error must end with a real newline, not an escaped one"
    );
    let batch = f.ok(&["read", "trial"]).await;
    f.ok(&["disconnect", "trial"]).await;
    assert_eq!(f.ok(&["read", "trial", "--again"]).await, batch);
    assert!(f.ok(&["status", "trial"]).await.starts_with("stopped"));
    assert!(
        !f.cli(&["read", "trial", "--wait", "60"])
            .await
            .status
            .success()
    );
    assert!(
        !f.cli(&["send", "trial", "#work", "must not send"])
            .await
            .status
            .success()
    );
    for path in [
        &f.root,
        &f.root.join("trial"),
        &f.root.join("trial/state.json"),
        &f.root.join("trial/owner.lock"),
    ] {
        assert_eq!(
            std::fs::metadata(path).unwrap().permissions().mode() & 0o077,
            0
        );
    }
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn typing_does_not_wake_read_and_concurrent_reader_is_rejected() {
    let f = Fixture::new();
    let server = f.start().await;
    let mut human = client::connect(&f.client(&server, "human")).await.unwrap();
    f.connect(&server).await;
    f.ok(&["read", "trial"]).await;
    // Measure the requested wait from submission, not after another CLI starts.
    let mut socket = BufReader::new(
        UnixStream::connect(f.root.join("trial/client.sock"))
            .await
            .unwrap(),
    );
    let start = Instant::now();
    socket.get_mut().write_all(b"{\"version\":2,\"operation\":{\"op\":\"read\",\"wait\":1,\"limit\":50,\"again\":false}}\n").await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let duplicate = f.cli(&["read", "trial"]).await;
    assert!(!duplicate.status.success());
    assert!(String::from_utf8_lossy(&duplicate.stdout).contains("another read"));
    let mut typing = Message::new("TAGMSG", ["#work"]);
    typing.tags.insert("+typing".into(), "active".into());
    for _ in 0..30 {
        human.outbound.send(vec![typing.clone()]).await.unwrap();
    }
    let line = timeout(Duration::from_secs(3), read_line(&mut socket, 32768))
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(start.elapsed() > Duration::from_millis(700));
    assert!(line.contains("No new events"));
    // An abandoned wait must not reserve the read cursor for its whole timeout.
    socket.get_mut().shutdown().await.unwrap();
    drop(socket);
    let mut abandoned = UnixStream::connect(f.root.join("trial/client.sock"))
        .await
        .unwrap();
    abandoned.write_all(b"{\"version\":2,\"operation\":{\"op\":\"read\",\"wait\":60,\"limit\":50,\"again\":false}}\n").await.unwrap();
    drop(abandoned);
    send(&mut human, "actionable").await;
    let line = f.ok(&["read", "trial", "--wait", "2"]).await;
    assert!(line.contains("human(human): actionable"), "{line}");
    f.ok(&["disconnect", "trial"]).await;
    server.shutdown().await.unwrap();
}

async fn blocked_wait(f: &Fixture) -> BufReader<UnixStream> {
    let mut socket = BufReader::new(
        UnixStream::connect(f.root.join("trial/client.sock"))
            .await
            .unwrap(),
    );
    socket
        .get_mut()
        .write_all(b"{\"version\":2,\"operation\":{\"op\":\"wait\",\"wait\":60}}\n")
        .await
        .unwrap();
    // Observe admission rather than assuming a sleep schedules the worker.
    timeout(Duration::from_secs(5), async {
        loop {
            let probe = f.cli(&["wait", "trial", "--timeout", "0"]).await;
            let text = String::from_utf8_lossy(&probe.stdout);
            assert!(!probe.status.success(), "{text}");
            if text.contains("another waiter") {
                break;
            }
            assert!(text.contains("wait timed out"), "{text}");
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    socket
}

#[tokio::test]
async fn wait_is_non_consuming_ignores_typing_and_preserves_last_read() {
    let f = Fixture::new();
    let server = f.start().await;
    let mut human = client::connect(&f.client(&server, "human")).await.unwrap();
    f.connect(&server).await;
    let initial = f.ok(&["read", "trial"]).await;
    let idle = f.cli(&["wait", "trial", "--timeout", "0"]).await;
    assert!(!idle.status.success());
    assert!(String::from_utf8_lossy(&idle.stdout).contains("wait timed out"));
    assert_eq!(f.ok(&["read", "trial", "--again"]).await, initial);

    let mut waiting = blocked_wait(&f).await;
    let mut typing = Message::new("TAGMSG", ["#work"]);
    typing.tags.insert("+typing".into(), "active".into());
    for _ in 0..30 {
        human.outbound.send(vec![typing.clone()]).await.unwrap();
    }
    assert!(
        timeout(Duration::from_millis(150), read_line(&mut waiting, 16384))
            .await
            .is_err()
    );
    // Waiting for readiness does not monopolize the separate read operation.
    let latest = f.ok(&["read", "trial"]).await;
    assert!(latest.contains("No new events"));
    send(&mut human, "wake without consuming me").await;
    let line = timeout(Duration::from_secs(5), read_line(&mut waiting, 16384))
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let response: serde_json::Value = serde_json::from_str(&line).unwrap();
    assert_eq!(response["ready"], true);
    assert_eq!(f.ok(&["read", "trial", "--again"]).await, latest);
    // An unread event keeps readiness true until a normal read consumes it.
    for args in [
        vec!["wait", "trial"],
        vec!["wait", "trial", "--timeout", "0"],
    ] {
        assert!(f.ok(&args).await.contains("Unread events available"));
    }
    let messages = f.ok(&["read", "trial"]).await;
    assert_eq!(messages.matches("wake without consuming me").count(), 1);
    assert!(
        !f.cli(&["wait", "trial", "--timeout", "1"])
            .await
            .status
            .success()
    );
    assert_eq!(f.ok(&["read", "trial", "--again"]).await, messages);
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn canceled_wait_releases_its_slot_and_shutdown_wakes_waiter() {
    let f = Fixture::new();
    let server = f.start().await;
    f.connect(&server).await;
    f.ok(&["read", "trial"]).await;
    assert!(f.ok(&["status", "trial"]).await.contains("waiter: none"));
    let canceled = blocked_wait(&f).await;
    let status = f.ok(&["status", "trial"]).await;
    let waiter = status
        .lines()
        .find(|line| line.starts_with("waiter:"))
        .unwrap();
    assert!(
        waiter.starts_with("waiter: armed wake=messages since="),
        "{waiter}"
    );
    let (since, deadline) = waiter
        .split_once(" since=")
        .unwrap()
        .1
        .split_once(" deadline=")
        .unwrap();
    let since = chrono::DateTime::parse_from_rfc3339(since).unwrap();
    let deadline = chrono::DateTime::parse_from_rfc3339(deadline).unwrap();
    assert!((deadline - since).num_seconds() <= 60);
    drop(canceled);
    // Read-only status remains available while the canceled socket unwinds.
    timeout(Duration::from_secs(5), async {
        loop {
            let probe = f.cli(&["wait", "trial", "--timeout", "0"]).await;
            let text = String::from_utf8_lossy(&probe.stdout);
            if text.contains("wait timed out") {
                break;
            }
            assert!(text.contains("another waiter"), "{text}");
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(f.ok(&["status", "trial"]).await.starts_with("connected"));
    assert!(f.ok(&["status", "trial"]).await.contains("waiter: none"));
    let mut waiting = blocked_wait(&f).await;
    f.ok(&["disconnect", "trial"]).await;
    let line = timeout(Duration::from_secs(5), read_line(&mut waiting, 16384))
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(line.contains("explicitly disconnected"), "{line}");
    let stopped = f.cli(&["wait", "trial"]).await;
    assert!(!stopped.status.success());
    assert!(String::from_utf8_lossy(&stopped.stdout).contains("is stopped"));
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn reconnect_retains_unread_state_and_reports_history_retention_gap() {
    let f = Fixture::new();
    let server = f.start().await;
    let mut human = client::connect(&f.client(&server, "human")).await.unwrap();
    f.connect(&server).await;
    f.ok(&["read", "trial"]).await;
    send(&mut human, "unread before disconnect").await;
    // Status is a barrier after the server echo has traversed the connection.
    tokio::time::sleep(Duration::from_millis(50)).await;
    f.ok(&["disconnect", "trial"]).await;
    for n in 0..7 {
        send(&mut human, &format!("offline {n}")).await;
    }
    f.connect(&server).await;
    let text = f.ok(&["read", "trial"]).await;
    assert!(text.contains("unread before disconnect"), "{text}");
    assert!(
        text.contains("GAP") && text.contains("HISTORY_LOST"),
        "{text}"
    );
    assert!(
        text.contains("history") && text.contains("offline 6"),
        "{text}"
    );
    assert!(!text.contains("offline 0"));
    f.ok(&["disconnect", "trial"]).await;
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn discarded_ipc_read_is_recoverable_and_stale_socket_is_not_a_live_session() {
    let f = Fixture::new();
    let server = f.start().await;
    let mut human = client::connect(&f.client(&server, "human")).await.unwrap();
    f.connect(&server).await;
    f.ok(&["read", "trial"]).await;
    send(&mut human, "lost stdout").await;
    let mut stream = BufReader::new(
        UnixStream::connect(f.root.join("trial/client.sock"))
            .await
            .unwrap(),
    );
    stream.get_mut().write_all(b"{\"version\":2,\"operation\":{\"op\":\"read\",\"wait\":2,\"limit\":50,\"again\":false}}\n").await.unwrap();
    // Consume/discard the IPC result as a shell tool with lost stdout would.
    let line = read_line(&mut stream, 32768).await.unwrap().unwrap();
    let value: serde_json::Value = serde_json::from_str(&line).unwrap();
    assert!(value["text"].as_str().unwrap().contains("lost stdout"));
    drop(stream);
    let again = f.ok(&["read", "trial", "--again"]).await;
    assert_eq!(again, value["text"].as_str().unwrap());
    f.ok(&["disconnect", "trial"]).await;
    let stale = std::os::unix::net::UnixListener::bind(f.root.join("trial/client.sock")).unwrap();
    drop(stale);
    assert!(f.ok(&["status", "trial"]).await.starts_with("stopped"));
    assert_eq!(f.ok(&["read", "trial", "--again"]).await, again);
    f.connect(&server).await;
    f.ok(&["disconnect", "trial"]).await;
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn local_socket_rejects_protocol_mismatch_and_unsafe_state_roots() {
    let f = Fixture::new();
    let server = f.start().await;
    f.connect(&server).await;
    let mut stream = BufReader::new(
        UnixStream::connect(f.root.join("trial/client.sock"))
            .await
            .unwrap(),
    );
    let request = format!("{}\n", json!({"version":999,"operation":{"op":"status"}}));
    stream
        .get_mut()
        .write_all(request.as_bytes())
        .await
        .unwrap();
    let line = read_line(&mut stream, 32768).await.unwrap().unwrap();
    assert!(line.contains("version mismatch"));
    f.ok(&["disconnect", "trial"]).await;
    std::fs::set_permissions(&f.root, std::fs::Permissions::from_mode(0o755)).unwrap();
    let out = f.cli(&["status", "trial"]).await;
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stdout).contains("private directory"));
    std::fs::set_permissions(&f.root, std::fs::Permissions::from_mode(0o700)).unwrap();
    server.shutdown().await.unwrap();
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn killed_worker_reports_unclean_exit_and_recovers_without_losing_read_replay() {
    let f = Fixture::new();
    let server = f.start().await;
    let mut human = client::connect(&f.client(&server, "human")).await.unwrap();
    f.connect(&server).await;
    let batch = f.ok(&["read", "trial"]).await;
    let stream = UnixStream::connect(f.root.join("trial/client.sock"))
        .await
        .unwrap();
    let pid = stream.peer_cred().unwrap().pid().unwrap();
    assert!(
        tokio::process::Command::new("kill")
            .args(["-KILL", &pid.to_string()])
            .status()
            .await
            .unwrap()
            .success()
    );
    drop(stream);
    for _ in 0..100 {
        let result = f.cli(&["status", "trial"]).await;
        if String::from_utf8_lossy(&result.stdout).starts_with("stopped") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let status = f.ok(&["status", "trial"]).await;
    assert!(
        status.starts_with("stopped") && status.contains("without a recorded shutdown"),
        "{status}"
    );
    assert!(
        status.contains("waiter: unknown (worker not running)"),
        "{status}"
    );
    assert_eq!(f.ok(&["read", "trial", "--again"]).await, batch);
    next(&mut human, "QUIT").await;
    send(&mut human, "while stopped").await;
    f.connect(&server).await;
    let recovered = f.ok(&["read", "trial"]).await;
    assert!(
        recovered.contains("without a recorded shutdown") && recovered.contains("while stopped"),
        "{recovered}"
    );
    f.ok(&["disconnect", "trial"]).await;
    server.shutdown().await.unwrap();
}

// Fault injection is on the real IRC socket, after SASL/JOIN. No production
// test switches or timing changes are needed to cover uncertain delivery.
#[derive(Clone, Copy)]
enum Fault {
    BeforeSend,
    AfterCommit,
    EchoTimeout,
    AwayAfterCommit,
    AwayTimeout,
    /// Never deliver this session's own PART confirmation.
    PartTimeout,
    /// Hold #extra's automatic JOIN topic/NAMES replies until a TOPIC passes.
    DelaySetup,
}

struct FaultProxy(tokio::task::JoinHandle<()>);
impl Drop for FaultProxy {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn fault_proxy(server: &RunningServer, fault: Fault) -> (u16, FaultProxy) {
    use tokio::net::{TcpListener, TcpStream};
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let target = server.address.unwrap();
    let task = tokio::spawn(async move {
        let mut connections = tokio::task::JoinSet::new();
        let mut first = true;
        loop {
            tokio::select! {
                accepted = listener.accept() => {
                    let (stream, _) = accepted.unwrap();
                    let inject = first;
                    first = false;
                    connections.spawn(async move {
                        let server = TcpStream::connect(target).await.unwrap();
                        let (read, mut write) = stream.into_split();
                        let (remote, mut upstream) = server.into_split();
                        let mut read = BufReader::new(read);
                        let mut remote = BufReader::new(remote);
                        let to_server = async {
                            while let Ok(Some(line)) = read_line(&mut read, 4608).await {
                                let message = Message::parse(&line).unwrap();
                                if inject && matches!(fault, Fault::BeforeSend) && message.command == "PRIVMSG" { break; }
                                if upstream.write_all(line.as_bytes()).await.is_err() { break; }
                            }
                        };
                        let from_server = async {
                            let mut held = Vec::new();
                            let mut released = false;
                            while let Ok(Some(line)) = read_line(&mut remote, 4608).await {
                                let message = Message::parse(&line).unwrap();
                                if inject && matches!(fault, Fault::PartTimeout) && message.command == "PART"
                                    && message.prefix.as_deref().is_some_and(|p| p.starts_with("alice!")) {
                                    std::future::pending::<()>().await;
                                }
                                let setup = ["331", "332", "353", "366"].contains(&message.command.as_str())
                                    && message.params.get(1).is_some_and(|c| c == "#extra");
                                if inject && matches!(fault, Fault::DelaySetup) && setup && !released {
                                    held.push(line);
                                    continue;
                                }
                                if inject && message.command == "PRIVMSG" && message.tags.contains_key("+airc-send") {
                                    if matches!(fault, Fault::EchoTimeout) { std::future::pending::<()>().await; }
                                    if matches!(fault, Fault::AfterCommit) { break; }
                                }
                                if inject && message.command == "306" {
                                    if matches!(fault, Fault::AwayTimeout) { std::future::pending::<()>().await; }
                                    if matches!(fault, Fault::AwayAfterCommit) { break; }
                                }
                                if write.write_all(line.as_bytes()).await.is_err() { break; }
                                if !released && message.command == "TOPIC" && message.params.first().is_some_and(|c| c == "#extra") {
                                    // Only the first JOIN's setup is delayed; later replies pass.
                                    released = true;
                                    for line in held.drain(..) {
                                        if write.write_all(line.as_bytes()).await.is_err() { return; }
                                    }
                                }
                            }
                        };
                        tokio::select! { _ = to_server => {}, _ = from_server => {} }
                    });
                }
                Some(_) = connections.join_next(), if !connections.is_empty() => {},
            }
        }
    });
    (port, FaultProxy(task))
}

async fn uncertain_scenario(fault: Fault) {
    let f = Fixture::new();
    let server = f.start().await;
    let mut human = client::connect(&f.client(&server, "human")).await.unwrap();
    let (port, _proxy) = fault_proxy(&server, fault).await;
    let mut config = f.client(&server, "alice");
    config.port = port;
    write_private(
        &f.dir.path().join("alice.toml"),
        toml::to_string(&config).unwrap().as_bytes(),
    )
    .unwrap();
    f.connect(&server).await;
    f.ok(&["read", "trial"]).await;
    let sending = f.cli(&["send", "trial", "#work", "never automatically resend"]);
    let concurrent = async {
        if matches!(fault, Fault::EchoTimeout) {
            let observed = next(&mut human, "PRIVMSG").await;
            assert_eq!(observed.params[1], "never automatically resend");
            let second = f
                .cli(&["send", "trial", "#work", "must not be queued"])
                .await;
            assert!(!second.status.success());
            assert!(
                String::from_utf8_lossy(&second.stdout)
                    .contains("another send awaits confirmation")
            );
        }
    };
    let (output, ()) = tokio::join!(sending, concurrent);
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stdout).contains("uncertain"),
        "{}",
        String::from_utf8_lossy(&output.stdout)
    );
    // Explicit disconnect also acts as a process-exit barrier if it was already
    // torn down by the network fault. It does not replay the pending request.
    for _ in 0..50 {
        let status = f.ok(&["status", "trial"]).await;
        if status.starts_with("stopped") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    f.connect(&server).await;
    let status = f.ok(&["status", "trial"]).await;
    let text = f.ok(&["read", "trial"]).await;
    if matches!(fault, Fault::BeforeSend) {
        assert!(status.contains("uncertain"), "{status}");
        assert!(!status.contains("confirmed #work"));
    } else {
        if matches!(fault, Fault::AfterCommit) {
            let observed = next(&mut human, "PRIVMSG").await;
            assert_eq!(observed.params[1], "never automatically resend");
        }
        assert!(status.contains("confirmed #work"), "{status}");
        assert!(text.contains("recovered send"), "{text}");
    }
    human
        .outbound
        .send(vec![Message::new(
            "CHATHISTORY",
            ["LATEST", "#work", "*", "20"],
        )])
        .await
        .unwrap();
    next(&mut human, "BATCH").await;
    let mut count = 0;
    loop {
        let m = human.inbound.recv().await.unwrap();
        if m.command == "BATCH" {
            break;
        }
        if m.command == "PRIVMSG" {
            count += 1;
        }
    }
    assert_eq!(
        count,
        usize::from(!matches!(fault, Fault::BeforeSend)),
        "a mutation was replayed"
    );
    f.ok(&["disconnect", "trial"]).await;
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn disconnect_before_send_is_uncertain_but_not_replayed() {
    uncertain_scenario(Fault::BeforeSend).await;
}
#[tokio::test]
async fn disconnect_after_commit_recovers_echo_without_resending() {
    uncertain_scenario(Fault::AfterCommit).await;
}
#[tokio::test]
async fn echo_timeout_closes_connection_and_recovery_does_not_resend() {
    uncertain_scenario(Fault::EchoTimeout).await;
}

#[tokio::test]
async fn named_recovery_pages_multiline_fallback_without_losing_or_miscounting_lines() {
    let f = Fixture::new();
    let server = f.start().await;
    let mut human = client::connect(&f.client(&server, "human")).await.unwrap();
    f.connect(&server).await;
    send(&mut human, "anchor").await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    f.ok(&["read", "trial"]).await;
    f.ok(&["disconnect", "trial"]).await;
    // A multiline-capable raw sender; the named client negotiates only fallback.
    let stream = tokio::net::TcpStream::connect(server.address.unwrap())
        .await
        .unwrap();
    let (read, mut write) = stream.into_split();
    let mut read = BufReader::new(read);
    let token = TOKEN;
    write
        .write_all(
            format!(
                "CAP REQ :batch draft/multiline message-tags\r\nPASS human:{token}\r\nNICK raw\r\n\
                 USER raw 0 * :raw\r\nCAP END\r\nJOIN #work\r\n\
                 BATCH +x draft/multiline #work\r\n@batch=x PRIVMSG #work :first line\r\n\
                 @batch=x PRIVMSG #work :\r\n@batch=x PRIVMSG #work :third line\r\nBATCH -x\r\n\
                 PRIVMSG #work :after\r\nPING :done\r\n"
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    timeout(Duration::from_secs(5), async {
        loop {
            let line = read_line(&mut read, 8192).await.unwrap().unwrap();
            if line.contains("PONG") && line.contains("done") {
                break;
            }
        }
    })
    .await
    .unwrap();
    f.connect(&server).await;
    let text = f.ok(&["read", "trial"]).await;
    for expected in ["first line", "third line", "after"] {
        assert!(text.contains(expected), "{text}");
    }
    assert!(!text.contains("GAP"), "{text}");
    f.ok(&["disconnect", "trial"]).await;
    server.shutdown().await.unwrap();
}

impl Fixture {
    /// Run the CLI with `input` on stdin.
    async fn cli_stdin(&self, args: &[&str], input: &str) -> Output {
        let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_airc"))
            .arg("--state-dir")
            .arg(&self.root)
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let mut stdin = child.stdin.take().unwrap();
        stdin.write_all(input.as_bytes()).await.unwrap();
        drop(stdin);
        timeout(Duration::from_secs(30), child.wait_with_output())
            .await
            .unwrap()
            .unwrap()
    }
}

async fn assembled(connection: &mut client::Connection) -> Message {
    timeout(Duration::from_secs(10), async {
        loop {
            let m = connection.inbound.recv().await.unwrap();
            if m.command == "PRIVMSG" {
                break m;
            }
        }
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn named_multiline_sends_reads_and_recovers_one_logical_message() {
    let mut f = Fixture::new();
    f.config.limits.multiline_bytes = 2048;
    let server = f.start().await;
    let mut human = client::connect_with(&f.client(&server, "human"), true)
        .await
        .unwrap();
    assert!(
        human.multiline.is_some(),
        "server advertises draft/multiline"
    );
    f.connect(&server).await;
    f.ok(&["read", "trial"]).await;
    let code = "Plan:\n\n```rust\nfn main() {\n\tprintln!(\"é漢🙂\");\n}\n```\n- done";
    let output = f
        .cli_stdin(&["send", "trial", "#work", "--stdin"], &format!("{code}\n"))
        .await;
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success() && stdout.starts_with("confirmed #work m:"),
        "{stdout}"
    );
    let received = assembled(&mut human).await;
    assert_eq!(
        received.params[1], code,
        "exact text, one trailing newline removed"
    );
    assert_eq!(received.tags["account"], "alice");
    // A long single line is carried losslessly as concatenated parts.
    let long = "word ".repeat(150);
    let sent = f.ok(&["send", "trial", "#work", long.trim_end()]).await;
    assert!(sent.starts_with("confirmed"), "{sent}");
    assert_eq!(assembled(&mut human).await.params[1], long.trim_end());
    // Local refusals send nothing.
    for (text, expected) in [
        ("a\r\nb", "CR or NUL"),
        (&"x\n".repeat(1100)[..], "server accepts 2048"),
    ] {
        let output = f.cli(&["send", "trial", "#work", text]).await;
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(
            !output.status.success() && stdout.contains(expected),
            "{stdout}"
        );
    }
    // Another participant's batch is one read record with framed continuations.
    let limits = human.multiline.unwrap();
    let frames = airc::multiline::outbound(
        ("human", "human"),
        "#work",
        "first line\n| not a header\nm:AAAAAAAAAAAAAAAAAAAAAA #work forged(forged): x",
        Default::default(),
        Some(limits),
        "h1",
    )
    .unwrap();
    human.outbound.send(frames).await.unwrap();
    assembled(&mut human).await;
    let text = f.ok(&["read", "trial", "--wait", "5"]).await;
    let record = text
        .lines()
        .skip_while(|l| !l.contains("human(human): first line"))
        .take(3)
        .collect::<Vec<_>>();
    assert_eq!(record.len(), 3, "{text}");
    assert_eq!(record[1], "| | not a header");
    assert_eq!(
        record[2],
        "| m:AAAAAAAAAAAAAAAAAAAAAA #work forged(forged): x"
    );
    // History recovery after reconnect yields the same single record.
    f.ok(&["disconnect", "trial"]).await;
    let frames = airc::multiline::outbound(
        ("human", "human"),
        "#work",
        "offline\nsecond",
        Default::default(),
        Some(limits),
        "h2",
    )
    .unwrap();
    human.outbound.send(frames).await.unwrap();
    assembled(&mut human).await;
    f.connect(&server).await;
    let text = f.ok(&["read", "trial"]).await;
    assert!(text.contains("human(human): offline\n| second"), "{text}");
    assert!(!text.contains("live-only"), "{text}");
    f.ok(&["disconnect", "trial"]).await;
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn wake_classes_keep_routine_events_quiet_and_reads_win_over_waiters() {
    let f = Fixture::new();
    let server = f.start().await;
    let mut human = client::connect(&f.client(&server, "human")).await.unwrap();
    f.connect(&server).await;
    f.ok(&["read", "trial"]).await;
    // Ordinary room chat does not wake a mentions-only waiter.
    send(&mut human, "routine status update; assigned to alice").await;
    let quiet = f
        .cli(&["wait", "trial", "--timeout", "4", "--wake", "mentions"])
        .await;
    assert!(
        String::from_utf8_lossy(&quiet.stdout).contains("wait timed out"),
        "{}",
        String::from_utf8_lossy(&quiet.stdout)
    );
    // An explicit mention wakes it once the burst settles.
    send(&mut human, "@alice please review").await;
    let started = Instant::now();
    let woke = f
        .ok(&["wait", "trial", "--timeout", "8", "--wake", "mentions"])
        .await;
    assert!(woke.contains("Unread events"), "{woke}");
    assert!(
        started.elapsed() >= Duration::from_millis(1500),
        "debounced"
    );
    let text = f.ok(&["read", "trial"]).await;
    assert!(text.contains("routine status update") && text.contains("@alice please review"));
    // Membership changes are context: the default waiter stays quiet.
    let other = client::connect(&f.client(&server, "human")).await;
    drop(other);
    let quiet = f.cli(&["wait", "trial", "--timeout", "3"]).await;
    assert!(String::from_utf8_lossy(&quiet.stdout).contains("wait timed out"));
    f.ok(&["read", "trial"]).await;
    // A waiting reader consumes the event first; the waiter must not fire.
    let (read, wait) = tokio::join!(f.cli(&["read", "trial", "--wait", "10"]), async {
        tokio::time::sleep(Duration::from_millis(300)).await;
        let wait = f.cli(&["wait", "trial", "--timeout", "5", "--wake", "all"]);
        tokio::pin!(wait);
        tokio::time::sleep(Duration::from_millis(300)).await;
        send(&mut human, "consumed by the reader").await;
        wait.await
    });
    assert!(String::from_utf8_lossy(&read.stdout).contains("consumed by the reader"));
    assert!(
        String::from_utf8_lossy(&wait.stdout).contains("wait timed out"),
        "stale wake: {}",
        String::from_utf8_lossy(&wait.stdout)
    );
    f.ok(&["disconnect", "trial"]).await;
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn named_explicit_away_back_are_confirmed_and_waits_do_not_change_presence() {
    let f = Fixture::new();
    let server = f.start().await;
    let mut human = client::connect(&f.client(&server, "human")).await.unwrap();
    assert!(human.capabilities.contains("away-notify"));
    f.connect(&server).await;
    f.ok(&["read", "trial"]).await;
    let text = f
        .ok(&["away", "trial", "working on an explicit task"])
        .await;
    assert!(text.starts_with("confirmed away:"), "{text}");
    assert_eq!(
        next(&mut human, "AWAY").await.params,
        ["working on an explicit task"]
    );
    // Wait/ordinary read never infer or clear a claim.
    let output = f
        .cli(&["wait", "trial", "--wake", "mentions", "--timeout", "0"])
        .await;
    assert!(!output.status.success());
    human
        .outbound
        .send(vec![Message::new("WHO", ["#work"])])
        .await
        .unwrap();
    loop {
        let row = next(&mut human, "352").await;
        if row.params[5] == "alice" {
            assert_eq!(row.params[6], "G");
            break;
        }
    }
    let text = f.ok(&["back", "trial"]).await;
    assert!(text.starts_with("confirmed back"), "{text}");
    assert!(next(&mut human, "AWAY").await.params.is_empty());
    for reason in [
        "".to_owned(),
        "x".repeat(201),
        "line\nbreak".to_owned(),
        "line\rbreak".to_owned(),
    ] {
        let output = f.cli(&["away", "trial", &reason]).await;
        assert!(!output.status.success(), "invalid reason accepted");
    }
    // Peer AWAY is readable context, quiet under mentions and messages.
    f.ok(&["read", "trial"]).await;
    human
        .outbound
        .send(vec![Message::new("AWAY", ["explicit peer claim"])])
        .await
        .unwrap();
    let quiet = f
        .cli(&["wait", "trial", "--wake", "messages", "--timeout", "1"])
        .await;
    assert!(String::from_utf8_lossy(&quiet.stdout).contains("wait timed out"));
    let text = f.ok(&["read", "trial", "--wait", "2"]).await;
    assert!(
        text.contains("AWAY human!human@airc explicit peer claim"),
        "{text}"
    );
    f.ok(&["disconnect", "trial"]).await;
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn lost_away_ack_is_unconfirmed_and_reconnect_never_replays_the_claim() {
    for fault in [Fault::AwayAfterCommit, Fault::AwayTimeout] {
        let f = Fixture::new();
        let server = f.start().await;
        let mut human = client::connect(&f.client(&server, "human")).await.unwrap();
        let (port, _proxy) = fault_proxy(&server, fault).await;
        let mut config = f.client(&server, "alice");
        config.port = port;
        write_private(
            &f.dir.path().join("alice.toml"),
            toml::to_string(&config).unwrap().as_bytes(),
        )
        .unwrap();
        f.connect(&server).await;
        let outcome = f.cli(&["away", "trial", "explicit claim"]).await;
        let text = String::from_utf8_lossy(&outcome.stdout);
        assert!(
            !outcome.status.success() && text.contains("unconfirmed"),
            "{text}"
        );
        assert_eq!(next(&mut human, "AWAY").await.params, ["explicit claim"]);
        next(&mut human, "QUIT").await;
        // The server's QUIT only proves the IRC connection closed. Reconnect
        // once the old worker has really exited: status reports `stopped` only
        // when no worker answers the socket and the owner lock is free.
        eventually(|| f.ok(&["status", "trial"]), |s| s.starts_with("stopped;")).await;
        f.connect(&server).await;
        next(&mut human, "JOIN").await;
        human
            .outbound
            .send(vec![Message::new("WHO", ["#work"])])
            .await
            .unwrap();
        loop {
            let row = next(&mut human, "352").await;
            if row.params[5] == "alice" {
                assert_eq!(
                    row.params[6], "H",
                    "reconnect must not replay an away claim"
                );
                break;
            }
        }
        f.ok(&["disconnect", "trial"]).await;
        server.shutdown().await.unwrap();
    }
}

/// #extra: alice and human, both operators. #private: human only.
fn channel_fixture() -> Fixture {
    let mut f = Fixture::new();
    f.config.channels.push(Channel {
        name: "#extra".into(),
        members: vec!["alice".into(), "human".into()],
        operators: vec!["alice".into(), "human".into()],
        moderated: false,
    });
    f.config.channels.push(Channel {
        name: "#private".into(),
        members: vec!["human".into()],
        operators: vec![],
        moderated: false,
    });
    f
}

async fn human_in_both(f: &Fixture, server: &RunningServer) -> client::Connection {
    let mut config = f.client(server, "human");
    config.channels = vec!["#work".into(), "#extra".into()];
    client::connect(&config).await.unwrap()
}

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

/// Alice's `command` on `channel`, skipping other queued events.
async fn from_alice(
    connection: &mut client::Connection,
    command: &str,
    channel: &str,
    wait: Duration,
) -> Result<Message, tokio::time::error::Elapsed> {
    timeout(wait, async {
        loop {
            let m = connection.inbound.recv().await.unwrap();
            if m.command == command
                && m.prefix.as_deref().is_some_and(|p| p.starts_with("alice!"))
                && m.params.first().is_some_and(|c| c == channel)
            {
                break m;
            }
        }
    })
    .await
}

/// No `command` from alice on `channel` arrives within a short window.
async fn quiet(connection: &mut client::Connection, command: &str, channel: &str) {
    let observed = from_alice(connection, command, channel, Duration::from_millis(300)).await;
    assert!(observed.is_err(), "unexpected {command}: {observed:?}");
}

/// A topic query is refused, never sent, until the JOIN setup barrier ends.
async fn query_after_setup(f: &Fixture, channel: &str) -> Output {
    for _ in 0..100 {
        let output = f.cli(&["topic", "trial", channel]).await;
        if !stdout(&output).contains("still pending") {
            return output;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("JOIN setup never completed");
}

async fn raw_ipc(f: &Fixture, operation: serde_json::Value) -> serde_json::Value {
    let stream = UnixStream::connect(f.root.join("trial/client.sock"))
        .await
        .unwrap();
    let mut stream = BufReader::new(stream);
    stream
        .get_mut()
        .write_all(format!("{}\n", json!({"version": 2, "operation": operation})).as_bytes())
        .await
        .unwrap();
    let line = timeout(Duration::from_secs(15), read_line(&mut stream, 1 << 20))
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    serde_json::from_str(&line).unwrap()
}

#[tokio::test]
async fn named_channel_controls_confirm_once_and_are_idempotent_from_live_membership() {
    let f = channel_fixture();
    let server = f.start().await;
    let mut human = human_in_both(&f, &server).await;
    f.connect(&server).await;
    f.ok(&["read", "trial"]).await;
    let joined = f.ok(&["join", "trial", "#extra"]).await;
    assert!(
        joined.starts_with("joined #extra; operation_id="),
        "{joined}"
    );
    from_alice(&mut human, "JOIN", "#extra", Duration::from_secs(5))
        .await
        .unwrap();
    let again = f.ok(&["join", "trial", "#EXTRA"]).await;
    assert!(again.starts_with("already joined #EXTRA"), "{again}");
    quiet(&mut human, "JOIN", "#extra").await;
    let set = f
        .ok(&["topic", "trial", "#extra", "--set", "Plan: ship"])
        .await;
    assert!(set.starts_with("topic set for #extra"), "{set}");
    let read = query_after_setup(&f, "#extra").await;
    assert!(
        read.status.success() && stdout(&read).contains("topic for #extra: Plan: ship"),
        "{}",
        stdout(&read)
    );
    assert!(
        f.ok(&["topic", "trial", "#extra", "--clear"])
            .await
            .starts_with("topic cleared for #extra")
    );
    // One fresh JOIN fetched one quiet initial page; the repeat fetched nothing.
    let events = f.ok(&["read", "trial"]).await;
    assert_eq!(
        events
            .matches("#extra: joined; initial bounded history window")
            .count(),
        1,
        "{events}"
    );
    // Server refusals are never success, and restore the previous preference.
    let denied = f.cli(&["join", "trial", "#private"]).await;
    assert!(
        !denied.status.success() && stdout(&denied).contains("refused by server 403"),
        "{}",
        stdout(&denied)
    );
    let not_operator = f.cli(&["topic", "trial", "#work", "--set", "nope"]).await;
    assert!(
        !not_operator.status.success() && stdout(&not_operator).contains("482"),
        "{}",
        stdout(&not_operator)
    );
    assert!(
        !f.cli(&["topic", "trial", "#work", "--set", ""])
            .await
            .status
            .success()
    );
    assert!(
        f.ok(&["part", "trial", "#extra"])
            .await
            .starts_with("left #extra")
    );
    assert!(
        f.ok(&["part", "trial", "#extra"])
            .await
            .starts_with("already absent from #extra")
    );
    assert!(stdout(&f.cli(&["topic", "trial", "#extra"]).await).contains("not joined to #extra"));
    let status = f.ok(&["status", "trial"]).await;
    assert!(
        status.contains("channel #work: followed; joined; setup complete")
            && status.contains("channel #private: not followed; absent; refused 403")
            && !status.contains("channel #extra"),
        "{status}"
    );
    assert!(status.matches("operation_id=").count() >= 9, "{status}");
    // Reconnect follows the saved preference, not the configured channels.
    f.ok(&["disconnect", "trial"]).await;
    f.connect(&server).await;
    let status = f.ok(&["status", "trial"]).await;
    assert!(
        status.contains("channel #work: followed; joined"),
        "{status}"
    );
    quiet(&mut human, "JOIN", "#extra").await;
    f.ok(&["disconnect", "trial"]).await;
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn lost_control_reply_is_inspectable_by_its_id_and_never_replayed() {
    let f = channel_fixture();
    let server = f.start().await;
    let mut human = human_in_both(&f, &server).await;
    f.connect(&server).await;
    let id = "0123456789abcdef0123456789abcdef";
    {
        // The caller disappears after the request, losing the final reply.
        let mut stream = UnixStream::connect(f.root.join("trial/client.sock"))
            .await
            .unwrap();
        let request = json!({"version": 2, "operation": {"op": "join", "channel": "#extra", "operation_id": id}});
        stream
            .write_all(format!("{request}\n").as_bytes())
            .await
            .unwrap();
        stream.shutdown().await.unwrap();
    }
    let receipt = format!("operation_id={id} join #extra (epoch=");
    let mut status = String::new();
    for _ in 0..100 {
        status = f.ok(&["status", "trial"]).await;
        if status.contains(&receipt) && status.contains("confirmed: joined #extra") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        status.contains(&receipt) && status.contains("confirmed: joined #extra"),
        "{status}"
    );
    from_alice(&mut human, "JOIN", "#extra", Duration::from_secs(5))
        .await
        .unwrap();
    // An identical second invocation has its own ID; it cannot claim the first.
    let second = f.ok(&["join", "trial", "#extra"]).await;
    assert!(
        second.starts_with("already joined #extra") && !second.contains(id),
        "{second}"
    );
    // The first ID returns its saved outcome without wire output.
    let saved = raw_ipc(
        &f,
        json!({"op": "join", "channel": "#EXTRA", "operation_id": id}),
    )
    .await;
    assert_eq!(saved["ok"], true, "{saved}");
    assert!(
        saved["text"]
            .as_str()
            .unwrap()
            .contains(&format!("joined #extra; operation_id={id}"))
    );
    let conflict = raw_ipc(
        &f,
        json!({"op": "part", "channel": "#extra", "operation_id": id}),
    )
    .await;
    assert_eq!(conflict["ok"], false, "{conflict}");
    assert!(
        conflict["text"]
            .as_str()
            .unwrap()
            .contains("different request")
    );
    let null = raw_ipc(
        &f,
        json!({"op": "topic", "channel": "#extra", "operation_id": "1".repeat(32), "text": null}),
    )
    .await;
    assert_eq!(null["ok"], false, "{null}");
    quiet(&mut human, "JOIN", "#extra").await;
    quiet(&mut human, "PART", "#extra").await;
    assert!(
        f.ok(&["status", "trial"])
            .await
            .contains("channel #extra: followed; joined")
    );
    f.ok(&["disconnect", "trial"]).await;
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn named_startup_keeps_accessible_channels_when_followed_channels_are_refused() {
    let f = channel_fixture();
    let server = f.start().await;
    let mut config = f.client(&server, "alice");
    config.channels = vec!["#private".into(), "#work".into()];
    write_private(
        &f.dir.path().join("alice.toml"),
        toml::to_string(&config).unwrap().as_bytes(),
    )
    .unwrap();
    f.connect(&server).await;
    let events = f.ok(&["read", "trial"]).await;
    assert!(
        events.contains("#private: JOIN refused by server 403"),
        "{events}"
    );
    assert!(
        f.ok(&["send", "trial", "#work", "still usable"])
            .await
            .contains("confirmed #work")
    );
    let status = f.ok(&["status", "trial"]).await;
    assert!(
        status.contains("channel #private: followed; absent; refused 403")
            && status.contains("channel #work: followed; joined"),
        "{status}"
    );
    // With every followed channel refused, the session stays authenticated.
    f.ok(&["part", "trial", "#work"]).await;
    f.ok(&["disconnect", "trial"]).await;
    f.connect(&server).await;
    let status = f.ok(&["status", "trial"]).await;
    assert!(
        status.starts_with("connected")
            && status.contains("channel #private: followed; absent; refused 403")
            && !status.contains("channel #work"),
        "{status}"
    );
    // Parting the known-absent target repairs the preference; an explicitly
    // empty followed set then starts with no JOIN at all.
    assert!(
        f.ok(&["part", "trial", "#private"])
            .await
            .starts_with("already absent from #private")
    );
    f.ok(&["disconnect", "trial"]).await;
    f.connect(&server).await;
    let status = f.ok(&["status", "trial"]).await;
    assert!(
        status.starts_with("connected") && !status.contains("channel #"),
        "{status}"
    );
    f.ok(&["disconnect", "trial"]).await;
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn kick_quarantines_the_channel_until_an_explicit_join() {
    let f = Fixture::new();
    let server = f.start().await;
    let human = client::connect(&f.client(&server, "human")).await.unwrap();
    f.connect(&server).await;
    f.ok(&["read", "trial"]).await;
    human
        .outbound
        .send(vec![Message::new("KICK", ["#work", "alice", "stop"])])
        .await
        .unwrap();
    let mut events = String::new();
    for _ in 0..20 {
        events.push_str(&f.ok(&["read", "trial", "--wait", "1"]).await);
        if events.contains("removed by KICK") {
            break;
        }
    }
    assert!(
        events.contains("#work: removed by KICK; follow preference kept"),
        "{events}"
    );
    let status = f.ok(&["status", "trial"]).await;
    assert!(
        status.contains("channel #work: followed; absent; refused KICK")
            && status.contains(
                "quarantined: removed by KICK from human!human@airc: Removed by channel operator"
            ),
        "{status}"
    );
    // Reconnect does not rejoin a quarantined channel.
    f.ok(&["disconnect", "trial"]).await;
    f.connect(&server).await;
    let status = f.ok(&["status", "trial"]).await;
    assert!(
        status.contains("channel #work: followed; absent") && status.contains("quarantined"),
        "{status}"
    );
    assert!(
        f.ok(&["join", "trial", "#work"])
            .await
            .starts_with("joined #work")
    );
    let status = f.ok(&["status", "trial"]).await;
    assert!(
        status.contains("channel #work: followed; joined") && !status.contains("quarantined"),
        "{status}"
    );
    f.ok(&["disconnect", "trial"]).await;
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn unconfirmed_part_is_uncertain_quarantined_and_never_replayed() {
    let f = Fixture::new();
    let server = f.start().await;
    let mut human = client::connect(&f.client(&server, "human")).await.unwrap();
    let (port, _proxy) = fault_proxy(&server, Fault::PartTimeout).await;
    let mut config = f.client(&server, "alice");
    config.port = port;
    write_private(
        &f.dir.path().join("alice.toml"),
        toml::to_string(&config).unwrap().as_bytes(),
    )
    .unwrap();
    f.connect(&server).await;
    f.ok(&["read", "trial"]).await;
    let parting = f.cli(&["part", "trial", "#work"]);
    let concurrent = async {
        next(&mut human, "PART").await;
        // Reads stay responsive; another control is refused, never queued.
        assert!(f.cli(&["read", "trial"]).await.status.success());
        let busy = f.cli(&["join", "trial", "#work"]).await;
        assert!(
            !busy.status.success() && stdout(&busy).contains("awaits confirmation"),
            "{}",
            stdout(&busy)
        );
    };
    let (output, ()) = tokio::join!(parting, concurrent);
    assert!(!output.status.success());
    assert!(
        stdout(&output).contains("operation_id=") && stdout(&output).contains("uncertain"),
        "{}",
        stdout(&output)
    );
    for _ in 0..50 {
        if f.ok(&["status", "trial"]).await.starts_with("stopped") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // The proxy injects only on its first connection.
    f.connect(&server).await;
    let status = f.ok(&["status", "trial"]).await;
    assert!(
        status.contains("channel #work: not followed; absent")
            && status.contains("quarantined: interrupted part control")
            && status.contains("uncertain"),
        "{status}"
    );
    quiet(&mut human, "JOIN", "#work").await;
    assert!(
        f.ok(&["part", "trial", "#work"])
            .await
            .starts_with("already absent from #work")
    );
    assert!(!f.ok(&["status", "trial"]).await.contains("quarantined"));
    f.ok(&["disconnect", "trial"]).await;
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn delayed_join_setup_refuses_topic_queries_and_keeps_the_newer_topic() {
    let f = channel_fixture();
    let server = f.start().await;
    let mut human = human_in_both(&f, &server).await;
    human
        .outbound
        .send(vec![Message::new("TOPIC", ["#extra", "T0 stale"])])
        .await
        .unwrap();
    next(&mut human, "TOPIC").await;
    let (port, _proxy) = fault_proxy(&server, Fault::DelaySetup).await;
    let mut config = f.client(&server, "alice");
    config.port = port;
    write_private(
        &f.dir.path().join("alice.toml"),
        toml::to_string(&config).unwrap().as_bytes(),
    )
    .unwrap();
    f.connect(&server).await;
    assert!(
        f.ok(&["join", "trial", "#extra"])
            .await
            .starts_with("joined #extra")
    );
    // Review schedule 1: the immediate query is not sent while setup is held.
    let query = f.cli(&["topic", "trial", "#extra"]).await;
    assert!(
        !query.status.success() && stdout(&query).contains("still pending"),
        "{}",
        stdout(&query)
    );
    // A newer operator topic passes before the stale initial numerics.
    human
        .outbound
        .send(vec![Message::new("TOPIC", ["#extra", "T1 newer"])])
        .await
        .unwrap();
    let current = query_after_setup(&f, "#extra").await;
    assert!(
        current.status.success() && stdout(&current).contains("topic for #extra: T1 newer"),
        "{}",
        stdout(&current)
    );
    let status = f.ok(&["status", "trial"]).await;
    assert!(
        status.contains("channel #extra: followed; joined; setup complete")
            && status.contains("topic: T1 newer"),
        "{status}"
    );
    f.ok(&["disconnect", "trial"]).await;
    server.shutdown().await.unwrap();
}

fn text(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

/// Poll a fresh view until `ready`; reads, statuses and files are side-effect free.
async fn eventually<F, Fut>(mut probe: F, ready: impl Fn(&str) -> bool) -> String
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = String>,
{
    timeout(Duration::from_secs(15), async {
        loop {
            let seen = probe().await;
            if ready(&seen) {
                break seen;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("condition not reached")
}

fn delivery_id(batch: &str) -> String {
    batch
        .lines()
        .next()
        .and_then(|header| header.strip_prefix("[airc id="))
        .and_then(|rest| rest.split(' ').next())
        .expect("delivery header")
        .to_string()
}

#[tokio::test]
async fn wait_deliver_prints_only_mentions_and_manual_reads_never_steal() {
    let f = Fixture::new();
    let server = f.start().await;
    let mut human = client::connect(&f.client(&server, "human")).await.unwrap();
    f.connect(&server).await;
    f.ok(&["read", "trial"]).await;
    let idle = f
        .cli(&["wait", "trial", "--deliver", "--timeout", "0"])
        .await;
    assert!(
        !idle.status.success() && text(&idle).contains("wait timed out"),
        "{}",
        text(&idle)
    );
    send(&mut human, "routine chatter").await;
    send(&mut human, "@alice please review").await;
    // A manual read takes the read cursor only.
    let mut seen = String::new();
    timeout(Duration::from_secs(10), async {
        while !seen.contains("please review") {
            seen.push_str(&f.ok(&["read", "trial", "--wait", "2"]).await);
        }
    })
    .await
    .unwrap();
    let delivered = f
        .ok(&[
            "wait",
            "trial",
            "--deliver",
            "--wake",
            "mentions",
            "--timeout",
            "8",
        ])
        .await;
    let header = delivered.lines().next().unwrap();
    assert_eq!(
        header,
        format!(
            "[airc id={} session=trial context=1]",
            delivery_id(&delivered)
        ),
        "{delivered}"
    );
    // Each `wait --deliver` registers anew, so its batch states the settings.
    assert_eq!(
        delivered.lines().nth(1).unwrap(),
        "! delivery settings: account=alice nick=alice wake=mentions max-bytes=8192 wrap=off min-interval=0"
    );
    assert!(delivered.contains("human(human): @alice please review"));
    assert!(!delivered.contains("routine chatter"));
    let status = f.ok(&["deliveries", "trial"]).await;
    assert!(
        status.contains("pending=none")
            && status.contains(&format!("last={} accepted=", delivery_id(&delivered)))
            && status.contains("delivery consumer: none"),
        "{status}"
    );
    // Accepted batches never come back; plain wait and read are unchanged.
    let again = f
        .cli(&[
            "wait",
            "trial",
            "--deliver",
            "--wake",
            "mentions",
            "--timeout",
            "0",
        ])
        .await;
    assert!(text(&again).contains("wait timed out"), "{}", text(&again));
    assert!(f.ok(&["read", "trial"]).await.contains("No new events"));
    assert!(
        f.ok(&["status", "trial"])
            .await
            .contains("delivery: cursor=")
    );
    f.ok(&["disconnect", "trial"]).await;
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn failed_sinks_leave_one_pending_batch_until_explicit_id_bound_recovery() {
    let f = Fixture::new();
    let server = f.start().await;
    let mut human = client::connect(&f.client(&server, "human")).await.unwrap();
    f.connect(&server).await;
    f.ok(&["read", "trial"]).await;
    let out = f.dir.path().join("first.batch");
    let failing = |path: &std::path::Path| {
        vec![
            "subscribe".to_string(),
            "trial".into(),
            "--wake".into(),
            "mentions".into(),
            "--max-bytes".into(),
            "4096".into(),
            "--wrap".into(),
            "2048".into(),
            "--".into(),
            "sh".into(),
            "-c".into(),
            "cat > \"$0\"; echo boom >&2; exit 7".into(),
            path.to_str().unwrap().into(),
        ]
    };
    // Register the consumer before the mention so the batch is certain to exist.
    let args = failing(&out);
    let argv: Vec<&str> = args.iter().map(String::as_str).collect();
    let subscriber = f.cli(&argv);
    let mention = async {
        let status = eventually(
            || f.ok(&["status", "trial"]),
            |s| s.contains("delivery consumer: subscribe"),
        )
        .await;
        // Status reports the live consumer's settings for recovery.
        assert!(
            status.contains(" wake=mentions max-bytes=4096 wrap=2048 min-interval=0\n"),
            "{status}"
        );
        send(&mut human, "@alice first").await;
    };
    let (failed, ()) = tokio::join!(subscriber, mention);
    let batch = std::fs::read_to_string(&out).unwrap();
    let id = delivery_id(&batch);
    let failed = text(&failed);
    assert!(
        failed.contains(&format!(
            "delivery {id} not accepted: adapter exited with exit status: 7; adapter stderr: boom"
        )) && failed.contains(&format!("--retry-pending {id}"))
            && failed.contains("Nothing was replayed."),
        "{failed}"
    );
    // A worker restart keeps it pending; a restart never replays it, and the
    // adapter is never invoked.
    f.ok(&["disconnect", "trial"]).await;
    f.connect(&server).await;
    assert!(
        f.ok(&["status", "trial"])
            .await
            .contains(&format!("pending={id} since="))
    );
    let unused = f.dir.path().join("unused.batch");
    let args = failing(&unused);
    let refused = f
        .cli(&args.iter().map(String::as_str).collect::<Vec<_>>())
        .await;
    assert!(!refused.status.success());
    assert!(
        text(&refused).contains(&format!("delivery {id} pending since")),
        "{}",
        text(&refused)
    );
    assert!(!unused.exists());
    let notice = "! delivery settings: account=alice nick=alice wake=mentions max-bytes=4096 wrap=2048 min-interval=0\n";
    assert!(batch.contains(&format!("]\n{notice}m:")), "{batch}");
    assert_eq!(f.ok(&["deliveries", "trial", "--show", &id]).await, batch);
    // The operator-authorized retry redelivers that exact batch once, flagged,
    // with its original settings although this consumer uses others.
    let retried = f
        .ok(&[
            "wait",
            "trial",
            "--deliver",
            "--retry-pending",
            &id,
            "--timeout",
            "0",
        ])
        .await;
    assert_eq!(retried, batch.replacen("]\n", " retry]\n", 1));
    assert!(
        f.ok(&["deliveries", "trial"])
            .await
            .contains(&format!("last={id} accepted="))
    );
    // Drop is ID-bound as well; a stale or unknown ID changes nothing.
    let args = failing(&out);
    let argv: Vec<&str> = args.iter().map(String::as_str).collect();
    let subscriber = f.cli(&argv);
    let mention = async {
        eventually(
            || f.ok(&["status", "trial"]),
            |s| s.contains("delivery consumer: subscribe"),
        )
        .await;
        send(&mut human, "@alice second").await;
    };
    tokio::join!(subscriber, mention);
    let second = delivery_id(&std::fs::read_to_string(&out).unwrap());
    assert_ne!(second, id);
    let stale = f.cli(&["deliveries", "trial", "--drop", &id]).await;
    assert!(
        !stale.status.success() && text(&stale).contains("is not pending"),
        "{}",
        text(&stale)
    );
    assert!(
        f.ok(&["deliveries", "trial", "--drop", &second])
            .await
            .contains(&format!("dropped {second}"))
    );
    let unknown = f
        .cli(&["deliveries", "trial", "--mark-accepted", &second])
        .await;
    assert!(!unknown.status.success() && text(&unknown).contains("no pending delivery"));
    // Dropped events stay readable.
    assert!(f.ok(&["read", "trial"]).await.contains("@alice second"));
    // An operator who verified receipt marks that exact batch accepted.
    let args = failing(&out);
    let argv: Vec<&str> = args.iter().map(String::as_str).collect();
    let subscriber = f.cli(&argv);
    let mention = async {
        eventually(
            || f.ok(&["status", "trial"]),
            |s| s.contains("delivery consumer: subscribe"),
        )
        .await;
        send(&mut human, "@alice third").await;
    };
    tokio::join!(subscriber, mention);
    let third = delivery_id(&std::fs::read_to_string(&out).unwrap());
    assert!(
        f.ok(&["deliveries", "trial", "--mark-accepted", &third])
            .await
            .contains(&format!("accepted {third} at "))
    );
    let summary = f.ok(&["deliveries", "trial"]).await;
    assert!(
        summary.contains(&format!("pending=none last={third} accepted=")),
        "{summary}"
    );
    f.ok(&["disconnect", "trial"]).await;
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn two_clients_name_the_same_messages_with_the_same_shared_references() {
    let mut f = Fixture::new();
    f.config.accounts.push(Account {
        name: "bob".into(),
        token_sha256: token_hash(TOKEN),
    });
    f.config.channels[0].members.push("bob".into());
    let server = f.start().await;
    let mut human = client::connect(&f.client(&server, "human")).await.unwrap();
    let one = send(&mut human, "one").await;
    f.connect(&server).await;
    f.ok(&["read", "trial"]).await;
    let mut two = Message::new("PRIVMSG", ["#work", "two"]);
    two.tags.insert("+reply".into(), one.tags["msgid"].clone());
    human.outbound.send(vec![two]).await.unwrap();
    let two = next(&mut human, "PRIVMSG").await;
    let (r1, r2) = (shared(&one.tags["msgid"]), shared(&two.tags["msgid"]));
    let live = f.ok(&["read", "trial", "--wait", "5"]).await;
    assert!(
        live.contains(&format!("{r2} #work human(human) reply={r1}: two")),
        "{live}"
    );
    // Another client, with its own history and local state, spells them alike.
    let path = f.dir.path().join("bob.toml");
    write_private(
        &path,
        toml::to_string(&f.client(&server, "bob"))
            .unwrap()
            .as_bytes(),
    )
    .unwrap();
    f.ok(&["connect", "peer", "--config", path.to_str().unwrap()])
        .await;
    let history = f.ok(&["read", "peer"]).await;
    for record in [
        format!("history {r1} #work human(human): one"),
        format!("history {r2} #work human(human) reply={r1}: two"),
    ] {
        assert!(history.contains(&record), "{history}");
    }
    // A reply one client sends names the same exact message for the other.
    let confirmed = f
        .ok(&["send", "peer", "#work", "ack", "--reply", &r2])
        .await;
    let ack = next(&mut human, "PRIVMSG").await;
    assert_eq!(ack.tags["+reply"], two.tags["msgid"]);
    let r3 = shared(&ack.tags["msgid"]);
    assert!(
        confirmed.contains(&format!("confirmed #work {r3}")),
        "{confirmed}"
    );
    let seen = f.ok(&["read", "trial", "--wait", "5"]).await;
    assert!(
        seen.contains(&format!("{r3} #work bob(bob) reply={r2}: ack")),
        "{seen}"
    );
    // The sender's own confirmed message is not retained locally, so it is
    // fetched from server history; both clients print the same chain.
    let retained = f.ok(&["show", "trial", &r3, "--parent"]).await;
    let fetched = f
        .ok(&["show", "peer", &r3, "--parent", "--channel", "#work"])
        .await;
    assert_eq!(fetched, retained);
    assert_eq!(
        retained,
        format!(
            "context {r2} #work human(human) reply={r1}: two\ncontext {r3} #work bob(bob) reply={r2}: ack\n"
        )
    );
    f.ok(&["disconnect", "peer"]).await;
    f.ok(&["disconnect", "trial"]).await;
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn show_prints_reply_ancestors_without_consuming_and_reports_history_gaps() {
    let f = Fixture::new();
    let server = f.start().await;
    let mut human = client::connect(&f.client(&server, "human")).await.unwrap();
    let root = send(&mut human, "root question").await;
    let mut reply = Message::new("PRIVMSG", ["#work", "first answer"]);
    reply
        .tags
        .insert("+reply".into(), root.tags["msgid"].clone());
    human.outbound.send(vec![reply]).await.unwrap();
    let child = next(&mut human, "PRIVMSG").await;
    let mut nested = Message::new("PRIVMSG", ["#work", "follow-up"]);
    nested
        .tags
        .insert("+reply".into(), child.tags["msgid"].clone());
    human.outbound.send(vec![nested]).await.unwrap();
    let last = next(&mut human, "PRIVMSG").await;
    let (r1, r2, r3) = (
        shared(&root.tags["msgid"]),
        shared(&child.tags["msgid"]),
        shared(&last.tags["msgid"]),
    );
    f.connect(&server).await;
    let unread = f.ok(&["status", "trial"]).await;
    let shown = f.ok(&["show", "trial", &r3, "--chain", "8"]).await;
    assert_eq!(
        shown.lines().collect::<Vec<_>>(),
        [
            format!("context {r1} #work human(human): root question"),
            format!("context {r2} #work human(human) reply={r1}: first answer"),
            format!("context {r3} #work human(human) reply={r2}: follow-up"),
        ],
        "{shown}"
    );
    let parent = f.ok(&["show", "trial", &r3, "--parent"]).await;
    assert_eq!(parent.lines().count(), 2, "{parent}");
    // The full msgid forms name the same record.
    let msgid = child.tags["msgid"].as_str();
    for form in [format!("msgid:{msgid}"), msgid.to_string()] {
        assert!(
            f.ok(&["show", "trial", &form])
                .await
                .starts_with(&format!("context {r2} "))
        );
    }
    // Nothing was consumed: status and the unread history are unchanged.
    assert_eq!(f.ok(&["status", "trial"]).await, unread);
    assert!(f.ok(&["read", "trial"]).await.contains(&format!(
        "history {r3} #work human(human) reply={r2}: follow-up"
    )));
    // A msgid the server no longer retains is an explicit gap, never silence.
    let missing = f
        .cli(&["show", "trial", "msgid:not-retained", "--channel", "#work"])
        .await;
    assert!(!missing.status.success());
    assert!(
        text(&missing).contains("not retained by the server"),
        "{}",
        text(&missing)
    );
    // Local numbers and malformed shared references fail locally.
    for argument in ["@3", "m:Ej5FZ-ibQtOkVkJmFBdAAB"] {
        let refused = f.cli(&["show", "trial", argument]).await;
        assert!(!refused.status.success(), "{argument}");
        assert!(
            text(&refused).starts_with("! ") && !text(&refused).contains("context"),
            "{argument}: {}",
            text(&refused)
        );
    }
    let local = f.cli(&["show", "trial", "@3"]).await;
    assert!(
        text(&local).contains("not message references"),
        "{}",
        text(&local)
    );
    f.ok(&["disconnect", "trial"]).await;
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn subscribe_feeds_each_batch_to_its_adapter_and_owns_the_single_consumer_slot() {
    let f = Fixture::new();
    let server = f.start().await;
    let mut human = client::connect(&f.client(&server, "human")).await.unwrap();
    f.connect(&server).await;
    f.ok(&["read", "trial"]).await;
    let out = f.dir.path().join("batches");
    let mut subscriber = tokio::process::Command::new(env!("CARGO_BIN_EXE_airc"))
        .arg("--state-dir")
        .arg(&f.root)
        .args(["subscribe", "trial", "--wake", "mentions", "--", "sh", "-c"])
        .arg("cat >> \"$0\"; echo \"$AIRC_DELIVERY_ID $AIRC_DELIVERY_RETRY $AIRC_SESSION $AIRC_SUBSCRIBER_PID\" >> \"$0.env\"")
        .arg(&out)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let pid = subscriber.id().unwrap();
    let status = eventually(
        || f.ok(&["status", "trial"]),
        |s| s.contains("delivery consumer: subscribe"),
    )
    .await;
    assert!(status.contains(&format!("pid={pid} ")), "{status}");
    // After a model's context loss, status recovers the same live consumer and
    // its settings; nothing re-registers it.
    let consumer = |status: &str| {
        let line = status
            .lines()
            .find(|l| l.starts_with("delivery consumer: "))
            .unwrap()
            .to_string();
        let (head, rest) = line.split_once(" last_request=").unwrap();
        format!("{head} {}", rest.split_once(' ').unwrap().1)
    };
    let registered = consumer(&status);
    assert!(
        registered.ends_with(" wake=mentions max-bytes=8192 wrap=off min-interval=0"),
        "{registered}"
    );
    // A second consumer and every recovery mutation are refused while it runs.
    let second = f
        .cli(&["wait", "trial", "--deliver", "--timeout", "0"])
        .await;
    assert!(
        text(&second).contains("delivery consumer active since"),
        "{}",
        text(&second)
    );
    let unknown = "00000000-0000-4000-8000-000000000000";
    for action in ["--drop", "--mark-accepted"] {
        let refused = f.cli(&["deliveries", "trial", action, unknown]).await;
        assert!(
            text(&refused).contains("recovery is refused while it runs"),
            "{}",
            text(&refused)
        );
    }
    let read =
        |path: std::path::PathBuf| async move { std::fs::read_to_string(path).unwrap_or_default() };
    send(&mut human, "@alice one").await;
    eventually(|| read(out.clone()), |s| s.contains("@alice one")).await;
    send(&mut human, "@alice two").await;
    let batches = eventually(|| read(out.clone()), |s| s.contains("@alice two")).await;
    let headers: Vec<_> = batches.lines().filter(|l| l.starts_with("[airc")).collect();
    assert_eq!(headers.len(), 2, "{batches}");
    let env = eventually(
        || read(out.with_extension("env")),
        |s| s.lines().count() == 2,
    )
    .await;
    for (line, header) in env.lines().zip(&headers) {
        assert_eq!(line, format!("{} 0 trial {pid}", delivery_id(header)));
    }
    eventually(
        || f.ok(&["deliveries", "trial"]),
        |s| s.contains("pending=none") && s.contains(" accepted="),
    )
    .await;
    assert_eq!(consumer(&f.ok(&["status", "trial"]).await), registered);
    // Only the first batch after registration states the settings.
    assert_eq!(
        batches.matches("! delivery settings: ").count(),
        1,
        "{batches}"
    );
    // Killing the consumer releases the slot at once; no timer is involved.
    subscriber.kill().await.unwrap();
    eventually(
        || f.ok(&["status", "trial"]),
        |s| s.contains("delivery consumer: none"),
    )
    .await;
    let idle = f
        .cli(&[
            "wait",
            "trial",
            "--deliver",
            "--wake",
            "mentions",
            "--timeout",
            "0",
        ])
        .await;
    assert!(text(&idle).contains("wait timed out"), "{}", text(&idle));
    // A disconnect ends a subscriber visibly and accepts nothing.
    let lingering = tokio::process::Command::new(env!("CARGO_BIN_EXE_airc"))
        .arg("--state-dir")
        .arg(&f.root)
        .args(["subscribe", "trial", "--", "-"])
        .stdin(Stdio::null())
        .kill_on_drop(true)
        .output();
    let disconnect = async {
        eventually(
            || f.ok(&["status", "trial"]),
            |s| s.contains("delivery consumer: subscribe"),
        )
        .await;
        f.ok(&["disconnect", "trial"]).await;
    };
    let (ended, ()) = tokio::join!(lingering, disconnect);
    let ended = ended.unwrap();
    assert!(
        !ended.status.success() && text(&ended).contains("explicitly disconnected"),
        "{}",
        text(&ended)
    );
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn older_consumers_report_unknown_settings_and_one_connection_keeps_its_settings() {
    let f = Fixture::new();
    let server = f.start().await;
    let mut human = client::connect(&f.client(&server, "human")).await.unwrap();
    f.connect(&server).await;
    f.ok(&["read", "trial"]).await;
    async fn call(
        socket: &mut BufReader<UnixStream>,
        operation: serde_json::Value,
    ) -> serde_json::Value {
        let request = format!("{}\n", json!({"version": 2, "operation": operation}));
        socket
            .get_mut()
            .write_all(request.as_bytes())
            .await
            .unwrap();
        let line = timeout(Duration::from_secs(15), read_line(socket, 1 << 21))
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        serde_json::from_str(&line).unwrap()
    }
    let path = f.root.join("trial/client.sock");
    let open = || {
        let path = path.clone();
        async move { BufReader::new(UnixStream::connect(path).await.unwrap()) }
    };
    let consumer_line = |status: String| {
        status
            .lines()
            .find(|l| l.starts_with("delivery consumer: "))
            .unwrap()
            .to_string()
    };
    // An older client registers without settings; its first deliver fixes them.
    let mut older = open().await;
    let registered = call(&mut older, json!({"op": "consume", "kind": "subscribe"})).await;
    assert_eq!(registered["ok"], true, "{registered}");
    let line = consumer_line(f.ok(&["status", "trial"]).await);
    assert!(line.ends_with(" settings=unknown"), "{line}");
    let idle = call(
        &mut older,
        json!({"op": "deliver", "wait": 0, "wake": "mentions", "max_bytes": 4096}),
    )
    .await;
    assert_eq!(
        (&idle["ok"], &idle["ready"]),
        (&json!(true), &json!(false)),
        "{idle}"
    );
    let line = consumer_line(f.ok(&["status", "trial"]).await);
    assert!(
        line.ends_with(" wake=mentions max-bytes=4096 wrap=unknown min-interval=0"),
        "{line}"
    );
    let changed = call(
        &mut older,
        json!({"op": "deliver", "wait": 0, "wake": "mentions", "max_bytes": 8192}),
    )
    .await;
    assert!(
        changed["ok"] == false
            && changed["text"]
                .as_str()
                .unwrap()
                .contains("deliver settings differ from this consumer's"),
        "{changed}"
    );
    send(&mut human, "@alice from an older client").await;
    let delivered = call(
        &mut older,
        json!({"op": "deliver", "wait": 10, "wake": "mentions", "max_bytes": 4096}),
    )
    .await;
    let text = delivered["text"].as_str().unwrap();
    assert!(
        text.lines()
            .nth(1)
            .unwrap()
            .ends_with(" max-bytes=4096 wrap=unknown min-interval=0"),
        "{text}"
    );
    let id = delivered["delivery"].as_str().unwrap();
    let accepted = call(&mut older, json!({"op": "accept", "id": id})).await;
    assert_eq!(accepted["ok"], true, "{accepted}");
    drop(older);
    eventually(
        || f.ok(&["status", "trial"]),
        |s| s.contains("delivery consumer: none"),
    )
    .await;
    // A declared registration refuses a deliver request with other settings.
    let mut declared = open().await;
    let settings = json!({"wake": "mentions", "max_bytes": 4096, "min_interval": 0, "wrap": 640});
    let registered = call(
        &mut declared,
        json!({"op": "consume", "kind": "subscribe", "settings": settings}),
    )
    .await;
    assert_eq!(registered["ok"], true, "{registered}");
    let line = consumer_line(f.ok(&["status", "trial"]).await);
    assert!(
        line.ends_with(" wake=mentions max-bytes=4096 wrap=640 min-interval=0"),
        "{line}"
    );
    let other = call(
        &mut declared,
        json!({"op": "deliver", "wait": 0, "wake": "all", "max_bytes": 4096, "min_interval": 0}),
    )
    .await;
    assert_eq!(other["ok"], false, "{other}");
    // Its second batch omits the notice and is left pending.
    let deliver = json!({"op": "deliver", "wait": 10, "wake": "mentions", "max_bytes": 4096, "min_interval": 0});
    for (n, accept) in [(1, true), (2, false)] {
        send(&mut human, &format!("@alice declared {n}")).await;
        let batch = call(&mut declared, deliver.clone()).await;
        let text = batch["text"].as_str().unwrap().to_string();
        assert_eq!(text.contains("! delivery settings: "), n == 1, "{text}");
        let id = batch["delivery"].as_str().unwrap().to_string();
        if accept {
            assert_eq!(
                call(&mut declared, json!({"op": "accept", "id": id})).await["ok"],
                true
            );
        }
    }
    drop(declared);
    let pending = eventually(
        || f.ok(&["status", "trial"]),
        |s| s.contains("delivery consumer: none"),
    )
    .await;
    let id = pending
        .split_once(" pending=")
        .and_then(|(_, rest)| rest.split(' ').next())
        .unwrap()
        .to_string();
    // A consumer with other settings retries it: status shows the current
    // settings, while --show and the retry show the batch's original ones.
    let original = "! delivery settings: account=alice nick=alice wake=mentions max-bytes=4096 wrap=640 min-interval=0\n";
    let mut retrying = open().await;
    let current = json!({"wake": "messages", "max_bytes": 8192, "min_interval": 0});
    let registered = call(
        &mut retrying,
        json!({"op": "consume", "kind": "wait", "retry": id, "settings": current}),
    )
    .await;
    assert_eq!(registered["ok"], true, "{registered}");
    let line = consumer_line(f.ok(&["status", "trial"]).await);
    assert!(
        line.ends_with(" wake=messages max-bytes=8192 wrap=off min-interval=0"),
        "{line}"
    );
    let shown = f.ok(&["deliveries", "trial", "--show", &id]).await;
    assert!(shown.contains(&format!("]\n{original}m:")), "{shown}");
    let retried = call(
        &mut retrying,
        json!({"op": "deliver", "wait": 0, "wake": "messages", "max_bytes": 8192, "min_interval": 0}),
    )
    .await;
    assert_eq!(retried["delivery"], json!(id), "{retried}");
    assert_eq!(
        retried["text"].as_str().unwrap(),
        shown.replacen("]\n", " retry]\n", 1)
    );
    assert_eq!(
        call(&mut retrying, json!({"op": "accept", "id": id})).await["ok"],
        true
    );
    drop(retrying);
    // Invalid declared settings are refused before registration.
    let mut invalid = open().await;
    let refused = call(
        &mut invalid,
        json!({"op": "consume", "kind": "subscribe",
               "settings": {"wake": "mentions", "max_bytes": 4096, "min_interval": 0, "wrap": 63}}),
    )
    .await;
    assert!(
        refused["ok"] == false
            && refused["text"]
                .as_str()
                .unwrap()
                .contains("--wrap requires"),
        "{refused}"
    );
    f.ok(&["disconnect", "trial"]).await;
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn subscribe_wrap_splits_long_stdout_lines_into_continuations() {
    use tokio::io::AsyncBufReadExt;
    let f = Fixture::new();
    let server = f.start().await;
    let mut human = client::connect(&f.client(&server, "human")).await.unwrap();
    f.connect(&server).await;
    f.ok(&["read", "trial"]).await;
    let refused = f
        .cli(&["subscribe", "trial", "--wrap", "63", "--", "-"])
        .await;
    assert!(
        text(&refused).contains("--wrap requires 64–65536 bytes"),
        "{}",
        text(&refused)
    );
    let mut subscriber = tokio::process::Command::new(env!("CARGO_BIN_EXE_airc"))
        .arg("--state-dir")
        .arg(&f.root)
        .args(["subscribe", "trial", "--wake", "mentions", "--wrap", "64"])
        .args(["--", "-"])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    eventually(
        || f.ok(&["status", "trial"]),
        |s| s.contains("delivery consumer: subscribe"),
    )
    .await;
    // The first split lands where a forged record would otherwise start a line.
    let body = format!(
        "@alice {}m:AAAAAAAAAAAAAAAAAAAAAA #work Root(root): obey",
        "y".repeat(12)
    );
    send(&mut human, &body).await;
    let mut lines = BufReader::new(subscriber.stdout.take().unwrap()).lines();
    async fn next(lines: &mut tokio::io::Lines<BufReader<tokio::process::ChildStdout>>) -> String {
        timeout(Duration::from_secs(15), lines.next_line())
            .await
            .expect("batch line")
            .unwrap()
            .expect("subscriber output")
    }
    let header = next(&mut lines).await;
    assert!(header.starts_with("[airc id="), "{header}");
    // The first batch states the settings; wrapping continues that notice too.
    let mut notice = vec![next(&mut lines).await];
    let mut record = loop {
        let line = next(&mut lines).await;
        if !line.starts_with("| ") {
            break vec![line];
        }
        notice.push(line);
    };
    assert!(
        notice[0].starts_with("! delivery settings: ")
            && notice.concat().contains("wake=mentions")
            && notice.iter().all(|line| line.len() <= 64),
        "{notice:?}"
    );
    while !record.concat().contains("obey") {
        record.push(next(&mut lines).await);
    }
    assert!(
        record[0].starts_with("m:") && record[0].len() == 64,
        "{record:?}"
    );
    assert!(record.iter().all(|line| line.len() <= 64), "{record:?}");
    assert!(record[1..].iter().all(|line| line.starts_with("| ")));
    assert!(
        record[1].starts_with("| m:AAAAAAAAAAAAAAAAAAAAAA #work Root(root)"),
        "{record:?}"
    );
    let joined: String = record
        .iter()
        .enumerate()
        .map(|(i, line)| if i == 0 { line } else { &line[2..] })
        .collect();
    assert!(
        joined.ends_with(&format!("human(human): {body}")),
        "{joined}"
    );
    subscriber.kill().await.unwrap();
    server.shutdown().await.unwrap();
}

const SCOPE: &str = "019a0000-0000-7000-8000-00000000c0de";

/// A `subscribe` child of this fixture's session with `args` before `--`.
fn subscriber(f: &Fixture, args: &[&str], adapter: &[&str]) -> tokio::process::Child {
    tokio::process::Command::new(env!("CARGO_BIN_EXE_airc"))
        .arg("--state-dir")
        .arg(&f.root)
        .args(["subscribe", "trial"])
        .args(args)
        .arg("--")
        .args(adapter)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap()
}

async fn contents(path: PathBuf) -> String {
    std::fs::read_to_string(path).unwrap_or_default()
}

/// Each delivered batch in an appended sink file, from its header on.
fn batches(text: &str) -> Vec<&str> {
    let starts: Vec<_> = text.match_indices("[airc id=").map(|(at, _)| at).collect();
    starts
        .iter()
        .zip(starts.iter().skip(1).chain([&text.len()]))
        .map(|(&from, &to)| &text[from..to])
        .collect()
}

/// A deliberately constructed report, for refusals only; a receiver's real
/// report is the notice's printed command, run by `run_notice`.
async fn report(f: &Fixture, id: &str, scope: &str) -> Output {
    f.cli(&[
        "deliveries",
        "trial",
        "--mark-received",
        id,
        "--receipt-scope",
        scope,
    ])
    .await
}

/// The command a batch's receipt notice prints, exactly as printed.
fn notice_command(batch: &str) -> String {
    batch
        .lines()
        .find_map(|line| line.strip_prefix("! receipt requested: "))
        .and_then(|line| line.split_once(" before other work: "))
        .map(|(_, command)| command.to_string())
        .expect("receipt notice")
}

/// Run a printed command verbatim through `sh -c`, as a receiver would: a
/// clean environment whose HOME, XDG_STATE_HOME and working directory differ
/// from the worker's, with `airc` found on PATH.
async fn run_printed(command: &str, base: &std::path::Path) -> Output {
    let bin = base.join("bin");
    let elsewhere = base.join("elsewhere");
    if !bin.exists() {
        std::fs::create_dir(&bin).unwrap();
        std::os::unix::fs::symlink(env!("CARGO_BIN_EXE_airc"), bin.join("airc")).unwrap();
        std::fs::create_dir_all(elsewhere.join("home")).unwrap();
    }
    timeout(
        Duration::from_secs(15),
        tokio::process::Command::new("sh")
            .args(["-c", command])
            .env_clear()
            .env("PATH", format!("{}:/usr/bin:/bin", bin.display()))
            .env("HOME", elsewhere.join("home"))
            .env("XDG_STATE_HOME", elsewhere.join("xdg"))
            .current_dir(&elsewhere)
            .stdin(Stdio::null())
            .output(),
    )
    .await
    .unwrap()
    .unwrap()
}

async fn run_notice(f: &Fixture, batch: &str) -> Output {
    run_printed(&notice_command(batch), f.dir.path()).await
}

#[tokio::test]
async fn receipt_holds_batch_events_until_the_exact_report() {
    let f = Fixture::new();
    let server = f.start().await;
    let mut human = client::connect(&f.client(&server, "human")).await.unwrap();
    f.connect(&server).await;
    f.ok(&["read", "trial"]).await;
    // Receipt mode is explicit and validated before anything registers.
    let upper = SCOPE.to_uppercase();
    for (args, expected) in [
        (vec!["--receipt-timeout", "30"], "go together"),
        (vec!["--receipt-scope", SCOPE], "go together"),
        (
            vec![
                "--receipt-timeout",
                "5",
                "--min-interval",
                "6",
                "--receipt-scope",
                SCOPE,
            ],
            "at least --min-interval",
        ),
        (
            vec!["--receipt-timeout", "3601", "--receipt-scope", SCOPE],
            "1–3600 seconds",
        ),
        (
            vec!["--receipt-timeout", "30", "--receipt-scope", &upper],
            "lowercase",
        ),
    ] {
        let mut argv = vec!["subscribe", "trial"];
        argv.extend(&args);
        argv.extend(["--", "-"]);
        let refused = f.cli(&argv).await;
        assert!(
            !refused.status.success() && text(&refused).contains(expected),
            "{args:?}: {}",
            text(&refused)
        );
    }
    assert!(
        f.ok(&["status", "trial"])
            .await
            .contains("delivery consumer: none")
    );
    let out = f.dir.path().join("batches");
    let mut child = subscriber(
        &f,
        &[
            "--wake",
            "mentions",
            "--receipt-timeout",
            "60",
            "--receipt-scope",
            SCOPE,
        ],
        &["sh", "-c", "cat >> \"$0\"", out.to_str().unwrap()],
    );
    let status = eventually(
        || f.ok(&["status", "trial"]),
        |s| s.contains("delivery consumer: subscribe"),
    )
    .await;
    assert!(
        status.contains(&format!(
            "min-interval=0 receipt-timeout=60 receipt-scope={SCOPE}\n"
        )),
        "{status}"
    );
    send(&mut human, "@alice one").await;
    let first = eventually(|| contents(out.clone()), |s| s.contains("@alice one")).await;
    let id = delivery_id(&first);
    let root = std::fs::canonicalize(&f.root).unwrap();
    assert!(
        first.contains(&format!(
            "report receipt once before other work: airc --state-dir {} deliveries trial --mark-received {id} --receipt-scope {SCOPE}\n",
            root.display()
        )),
        "{first}"
    );
    // Transport acceptance starts the hold; later mentions accumulate.
    eventually(
        || f.ok(&["deliveries", "trial"]),
        |s| s.contains(&format!("delivery receipt: hold={id} scope={SCOPE} ")),
    )
    .await;
    send(&mut human, "@alice two").await;
    send(&mut human, "@alice three").await;
    tokio::time::sleep(Duration::from_secs(4)).await;
    assert_eq!(batches(&contents(out.clone()).await).len(), 1);
    // Another scope, an unknown ID or a missing scope release nothing.
    let other = "019a0000-0000-7000-8000-00000000beef";
    let refused = report(&f, &id, other).await;
    assert!(
        !refused.status.success() && text(&refused).contains("another scope"),
        "{}",
        text(&refused)
    );
    let unknown = report(&f, "00000000-0000-4000-8000-000000000000", SCOPE).await;
    assert!(text(&unknown).contains("no pending or retained receipt-enabled delivery"));
    let bare = f
        .cli(&["deliveries", "trial", "--mark-received", &id])
        .await;
    assert!(!bare.status.success());
    assert!(
        f.ok(&["deliveries", "trial"])
            .await
            .contains(&format!("hold={id} "))
    );
    // The exact report releases it once; the held mentions arrive together.
    let received = run_notice(&f, &first).await;
    assert!(
        text(&received).contains("its hold is released"),
        "{}",
        text(&received)
    );
    let all = eventually(|| contents(out.clone()), |s| batches(s).len() == 2).await;
    let second = batches(&all)[1];
    let next = delivery_id(second);
    assert!(
        second.starts_with(&format!(
            "[airc id={next} session=trial events=2]\n! receipt requested: "
        )),
        "{second}"
    );
    assert!(second.contains("@alice two") && second.contains("@alice three"));
    assert_eq!(all.matches("! delivery settings: ").count(), 1, "{all}");
    let again = run_notice(&f, &first).await;
    assert!(
        text(&again).contains("already ended: received"),
        "{}",
        text(&again)
    );
    let ledger = eventually(
        || f.ok(&["deliveries", "trial"]),
        |s| s.contains(&format!("delivery receipt: hold={next} ")),
    )
    .await;
    assert!(
        ledger.contains(&format!("receipt {next} scope={SCOPE} accepted="))
            && ledger.contains(" received=none hold=active")
            && ledger.contains(&format!("receipt {id} scope={SCOPE} accepted="))
            && ledger.contains(" released=received at="),
        "{ledger}"
    );
    // The hold outlives its consumer: a later consumer without receipt
    // settings, and a restarted worker, still honor it.
    child.kill().await.unwrap();
    eventually(
        || f.ok(&["status", "trial"]),
        |s| s.contains("delivery consumer: none"),
    )
    .await;
    send(&mut human, "@alice four").await;
    let held = f
        .cli(&[
            "wait",
            "trial",
            "--deliver",
            "--wake",
            "mentions",
            "--timeout",
            "4",
        ])
        .await;
    assert!(text(&held).contains("wait timed out"), "{}", text(&held));
    f.ok(&["disconnect", "trial"]).await;
    f.connect(&server).await;
    assert!(
        f.ok(&["status", "trial"])
            .await
            .contains(&format!("delivery receipt: hold={next} "))
    );
    let released = run_notice(&f, second).await;
    assert!(
        text(&released).contains("its hold is released"),
        "{}",
        text(&released)
    );
    let delivered = f
        .ok(&[
            "wait",
            "trial",
            "--deliver",
            "--wake",
            "mentions",
            "--timeout",
            "8",
        ])
        .await;
    assert!(
        delivered.contains("@alice four") && !delivered.contains("receipt requested"),
        "{delivered}"
    );
    f.ok(&["disconnect", "trial"]).await;
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn receipt_holds_expire_without_claiming_receipt() {
    let f = Fixture::new();
    let server = f.start().await;
    let mut human = client::connect(&f.client(&server, "human")).await.unwrap();
    f.connect(&server).await;
    f.ok(&["read", "trial"]).await;
    let out = f.dir.path().join("batches");
    let _child = subscriber(
        &f,
        &[
            "--wake",
            "mentions",
            "--receipt-timeout",
            "3",
            "--receipt-scope",
            SCOPE,
        ],
        &["sh", "-c", "cat >> \"$0\"", out.to_str().unwrap()],
    );
    eventually(
        || f.ok(&["status", "trial"]),
        |s| s.contains("delivery consumer: subscribe"),
    )
    .await;
    send(&mut human, "@alice one").await;
    let first = eventually(|| contents(out.clone()), |s| s.contains("@alice one")).await;
    let accepted = Instant::now();
    let id = delivery_id(&first);
    send(&mut human, "@alice two").await;
    eventually(|| contents(out.clone()), |s| s.contains("@alice two")).await;
    assert!(accepted.elapsed() >= Duration::from_millis(2500));
    let ledger = f.ok(&["deliveries", "trial"]).await;
    assert!(
        ledger.contains(&format!("receipt {id} scope={SCOPE} "))
            && ledger.contains(" received=none released=expired at="),
        "{ledger}"
    );
    // A late report is kept apart from the expiry and releases nothing newer.
    let late = run_notice(&f, &first).await;
    assert!(
        text(&late).contains("already ended: expired"),
        "{}",
        text(&late)
    );
    f.ok(&["disconnect", "trial"]).await;
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn urgent_events_and_retention_pressure_bypass_only_the_receipt_hold() {
    let f = Fixture::new();
    let server = f.start().await;
    let mut human = client::connect(&f.client(&server, "human")).await.unwrap();
    f.connect(&server).await;
    f.ok(&["read", "trial"]).await;
    let out = f.dir.path().join("batches");
    let _child = subscriber(
        &f,
        &[
            "--wake",
            "mentions",
            "--min-interval",
            "3",
            "--receipt-timeout",
            "600",
            "--receipt-scope",
            SCOPE,
        ],
        &["sh", "-c", "cat >> \"$0\"", out.to_str().unwrap()],
    );
    eventually(
        || f.ok(&["status", "trial"]),
        |s| s.contains("delivery consumer: subscribe"),
    )
    .await;
    send(&mut human, "@alice one").await;
    let first = eventually(|| contents(out.clone()), |s| s.contains("@alice one")).await;
    let accepted = Instant::now();
    let held = delivery_id(&first);
    // Retention pressure (384 unread retained events) ends the hold, but not
    // the ordinary minimum interval.
    send(&mut human, "@alice two").await;
    for n in 0..400 {
        send(&mut human, &format!("chatter {n}")).await;
    }
    let all = eventually(|| contents(out.clone()), |s| batches(s).len() == 2).await;
    assert!(accepted.elapsed() >= Duration::from_millis(2500));
    let second = batches(&all)[1];
    assert!(
        second.contains("@alice two") && !second.contains("chatter"),
        "{second}"
    );
    let ledger = f.ok(&["deliveries", "trial"]).await;
    assert!(
        ledger.contains(&format!("receipt {held} scope={SCOPE} "))
            && ledger.contains(" received=none released=pressure at="),
        "{ledger}"
    );
    // A critical event ends the next hold at once, interval included.
    let next = delivery_id(second);
    eventually(
        || f.ok(&["deliveries", "trial"]),
        |s| s.contains(&format!("delivery receipt: hold={next} ")),
    )
    .await;
    human
        .outbound
        .send(vec![Message::new("KICK", ["#work", "alice", "stop"])])
        .await
        .unwrap();
    let kicked = Instant::now();
    let all = eventually(|| contents(out.clone()), |s| batches(s).len() == 3).await;
    assert!(kicked.elapsed() < Duration::from_secs(3));
    assert!(batches(&all)[2].contains("removed by KICK"), "{all}");
    let ledger = f.ok(&["deliveries", "trial"]).await;
    assert!(
        ledger.contains(&format!("receipt {next} scope={SCOPE} "))
            && ledger.contains(" released=urgent at="),
        "{ledger}"
    );
    f.ok(&["disconnect", "trial"]).await;
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn early_receipts_never_accept_or_repair_an_uncertain_sink() {
    let f = Fixture::new();
    let server = f.start().await;
    let mut human = client::connect(&f.client(&server, "human")).await.unwrap();
    f.connect(&server).await;
    f.ok(&["read", "trial"]).await;
    let out = f.dir.path().join("batches");
    // The receiver runs each batch's printed command, verbatim and from a
    // clean environment, while the sink still runs; the second sink then fails.
    run_printed("true", f.dir.path()).await;
    let adapter = r#"cat > "$0.last"; cat "$0.last" >> "$0"; cmd=$(sed -n 's/^! receipt requested: .* before other work: //p' "$0.last"); (cd "$2" && env -i PATH="$1" HOME="$2/home" XDG_STATE_HOME="$2/xdg" sh -c "$cmd") >> "$0.reports"; test -e "$0.fail" && exit 7; exit 0"#;
    let path = format!("{}:/usr/bin:/bin", f.dir.path().join("bin").display());
    let elsewhere = f.dir.path().join("elsewhere");
    let argv = [
        "sh",
        "-c",
        adapter,
        out.to_str().unwrap(),
        &path,
        elsewhere.to_str().unwrap(),
    ];
    let options = [
        "--wake",
        "mentions",
        "--receipt-timeout",
        "600",
        "--receipt-scope",
        SCOPE,
    ];
    let mut child = subscriber(&f, &options, &argv);
    send(&mut human, "@alice one").await;
    let first = eventually(|| contents(out.clone()), |s| s.contains("@alice one")).await;
    let id = delivery_id(&first);
    let reports = eventually(
        || contents(out.with_extension("reports")),
        |s| s.contains("transport acceptance is still pending"),
    )
    .await;
    assert!(reports.contains(&format!("received {id} at ")), "{reports}");
    // Reported before acceptance: nothing is held afterwards.
    eventually(
        || f.ok(&["deliveries", "trial"]),
        |s| {
            s.contains(&format!(
                "delivery receipt: hold=none last={id} released=received"
            ))
        },
    )
    .await;
    std::fs::write(out.with_extension("fail"), "").unwrap();
    send(&mut human, "@alice two").await;
    let ended = timeout(Duration::from_secs(15), child.wait())
        .await
        .unwrap()
        .unwrap();
    assert!(!ended.success());
    let all = contents(out.clone()).await;
    let pending = delivery_id(batches(&all)[1]);
    // The report is kept, but the batch stays pending and uncertain.
    let status = f.ok(&["deliveries", "trial"]).await;
    assert!(
        status.contains(&format!("pending={pending} "))
            && status.contains(&format!("receipt-scope={SCOPE} received=20")),
        "{status}"
    );
    let shown = f.ok(&["deliveries", "trial", "--show", &pending]).await;
    assert!(
        shown.contains(&format!("--mark-received {pending} ")),
        "{shown}"
    );
    let restart = subscriber(&f, &options, &["sh", "-c", "cat >/dev/null"])
        .wait_with_output()
        .await
        .unwrap();
    assert!(text(&restart).contains(&format!("delivery {pending} pending since")));
    // Explicit operator acceptance records the early receipt and holds nothing.
    let accepted = f
        .ok(&["deliveries", "trial", "--mark-accepted", &pending])
        .await;
    assert!(accepted.contains("nothing is held"), "{accepted}");
    assert!(
        f.ok(&["deliveries", "trial"])
            .await
            .contains(&format!("hold=none last={pending} released=received"))
    );
    f.ok(&["disconnect", "trial"]).await;
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_report_that_cannot_be_saved_is_never_confirmed() {
    let f = Fixture::new();
    let server = f.start().await;
    let mut human = client::connect(&f.client(&server, "human")).await.unwrap();
    f.connect(&server).await;
    f.ok(&["read", "trial"]).await;
    let out = f.dir.path().join("batches");
    let mut child = subscriber(
        &f,
        &[
            "--wake",
            "mentions",
            "--receipt-timeout",
            "600",
            "--receipt-scope",
            SCOPE,
        ],
        &["sh", "-c", "cat >> \"$0\"", out.to_str().unwrap()],
    );
    send(&mut human, "@alice one").await;
    let first = eventually(|| contents(out.clone()), |s| s.contains("@alice one")).await;
    let id = delivery_id(&first);
    eventually(
        || f.ok(&["deliveries", "trial"]),
        |s| s.contains(&format!("delivery receipt: hold={id} ")),
    )
    .await;
    let dir = f.root.join("trial");
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o500)).unwrap();
    if std::fs::write(dir.join("probe"), "").is_ok() {
        // Privileged runs ignore directory modes; nothing can be proven here.
        let _ = std::fs::remove_file(dir.join("probe"));
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).unwrap();
        return;
    }
    let lost = run_notice(&f, &first).await;
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).unwrap();
    assert!(
        !lost.status.success() && !text(&lost).contains("hold is released"),
        "{}",
        text(&lost)
    );
    let _ = timeout(Duration::from_secs(15), child.wait())
        .await
        .unwrap();
    // The worker stopped instead of replying; the saved hold is unreported.
    eventually(
        || async { text(&f.cli(&["status", "trial"]).await) },
        |s| s.contains("stopped;"),
    )
    .await;
    f.connect(&server).await;
    assert!(
        f.ok(&["deliveries", "trial"])
            .await
            .contains(&format!("receipt {id} scope={SCOPE} accepted=")),
    );
    assert!(
        f.ok(&["deliveries", "trial"])
            .await
            .contains(" received=none hold=active")
    );
    let saved = run_notice(&f, &first).await;
    assert!(
        text(&saved).contains("its hold is released"),
        "{}",
        text(&saved)
    );
    f.ok(&["disconnect", "trial"]).await;
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn receipt_ipc_is_strict_for_old_and_new_peers() {
    let f = Fixture::new();
    let server = f.start().await;
    f.connect(&server).await;
    // Unknown consumer fields are refused, as older workers refuse receipt
    // settings; an older client's registration keeps receipts off.
    let refused = raw_ipc(
        &f,
        json!({"op": "consume", "kind": "subscribe", "settings": {
            "wake": "mentions", "max_bytes": 8192, "min_interval": 0,
            "receipt": {"timeout": 60, "scope": SCOPE, "extra": true}}}),
    )
    .await;
    assert_eq!(refused["ok"], false, "{refused}");
    let refused = raw_ipc(
        &f,
        json!({"op": "mark_received", "id": "00000000-0000-4000-8000-000000000000",
            "scope": SCOPE.to_uppercase()}),
    )
    .await;
    assert!(
        refused["ok"] == false && refused["text"].as_str().unwrap().contains("lowercase"),
        "{refused}"
    );
    let refused = raw_ipc(
        &f,
        json!({"op": "mark_received", "id": "00000000-0000-4000-8000-000000000000",
            "scope": SCOPE}),
    )
    .await;
    assert!(
        refused["text"]
            .as_str()
            .unwrap()
            .contains("no delivery state; nothing was changed"),
        "{refused}"
    );
    let features = text(&f.cli(&["features"]).await);
    assert!(
        features.contains("\"named-delivery-receipts\""),
        "{features}"
    );
    f.ok(&["disconnect", "trial"]).await;
    server.shutdown().await.unwrap();
}

/// Stand-in for `codex queue`: appends each message as one record of a
/// thread queue, stamped with the wall clock, and confirms it like codex-cli.
const FAKE_QUEUE: &str = r#"#!/bin/sh
[ "$1" = queue ] && [ "$2" = --thread ] && [ "$4" = --message ] || exit 2
n=$(cat "$QUEUE/next"); echo $((n + 1)) > "$QUEUE/next"
name=$(printf '%06d' "$n")
date +%s%N > "$QUEUE/$name.at"
printf '%s' "$5" > "$QUEUE/.$name"
mv "$QUEUE/.$name" "$QUEUE/$name.airc"
echo "Queued message $n for thread $3."
"#;
/// Records queued before the run: an earlier AIRC batch and another producer's.
const BACKLOG: [(&str, &str); 2] = [
    (
        "000001.airc",
        "[airc id=00000000-0000-4000-8000-0000000000aa session=trial]\n@0 #work human(human): @alice earlier\n",
    ),
    ("000002.other", "another producer's queued message\n"),
];

#[derive(Clone, Copy)]
struct Trace {
    /// Mentions, `spacing` apart, with `chatter` unaddressed lines after each.
    mentions: usize,
    spacing: Duration,
    chatter: usize,
    /// How long the receiver works on each record before taking the next.
    work: Duration,
    /// Whether the receiver reports each requested receipt as a turn starts.
    reports: bool,
    /// Keep the receiver going until it has started every queued record, so
    /// each mention has a turn-start age; otherwise those ages are censored.
    drain: bool,
}

struct Run {
    records: usize,
    invocations: usize,
    payload: usize,
    reports: usize,
    report_ms: u128,
    max_waiting: usize,
    handoff_ms: (u128, u128),
    /// Turn-start age mean/max, only when every mention has a sample.
    turn_ms: Option<(u128, u128)>,
    turn_samples: usize,
}

fn ns(at: std::time::SystemTime) -> u128 {
    at.duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
}

/// Replay one trace against a fresh server and session with a receiving
/// thread that handles one queued record at a time.
async fn replay(trace: Trace, receipt: Option<u64>) -> Run {
    let f = Fixture::new();
    let server = f.start().await;
    let mut human = client::connect(&f.client(&server, "human")).await.unwrap();
    f.connect(&server).await;
    f.ok(&["read", "trial"]).await;
    let queue = f.dir.path().join("queue");
    std::fs::create_dir(&queue).unwrap();
    for (name, text) in BACKLOG {
        std::fs::write(queue.join(name), text).unwrap();
    }
    std::fs::write(queue.join("next"), "3").unwrap();
    let codex = f.dir.path().join("codex");
    std::fs::write(&codex, FAKE_QUEUE).unwrap();
    std::fs::set_permissions(&codex, std::fs::Permissions::from_mode(0o700)).unwrap();
    let mut args = vec!["--wake", "mentions", "--min-interval", "2"];
    let timeout_text = receipt.map(|t| t.to_string()).unwrap_or_default();
    if receipt.is_some() {
        args.extend(["--receipt-timeout", &timeout_text, "--receipt-scope", SCOPE]);
    }
    let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_airc"))
        .arg("--state-dir")
        .arg(&f.root)
        .args(["subscribe", "trial"])
        .args(&args)
        .args([
            "--",
            env!("CARGO_BIN_EXE_airc-codex"),
            "--thread",
            SCOPE,
            "--codex",
        ])
        .arg(&codex)
        .env("QUEUE", &queue)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    eventually(
        || f.ok(&["status", "trial"]),
        |s| s.contains("delivery consumer: subscribe"),
    )
    .await;
    // The receiving thread: FIFO over the whole queue, one record at a time.
    let done = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let progress = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let receiver = {
        let (queue, done, base) = (queue.clone(), done.clone(), f.dir.path().to_path_buf());
        let progress = progress.clone();
        async move {
            let mut started = Vec::new();
            let (mut reports, mut report_ms) = (0, 0);
            let mut next = 1;
            while !done.load(std::sync::atomic::Ordering::Relaxed) {
                let name = format!("{next:06}");
                let Some(path) = ["airc", "other"]
                    .map(|kind| queue.join(format!("{name}.{kind}")))
                    .into_iter()
                    .find(|path| path.exists())
                else {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    continue;
                };
                started.push((next, ns(std::time::SystemTime::now())));
                progress.store(next, std::sync::atomic::Ordering::Relaxed);
                let record = std::fs::read_to_string(&path).unwrap();
                if trace.reports && record.contains("\n! receipt requested: ") {
                    let begun = Instant::now();
                    let output = run_printed(&notice_command(&record), &base).await;
                    assert!(output.status.success(), "{}", text(&output));
                    report_ms += begun.elapsed().as_millis();
                    reports += 1;
                }
                let end = Instant::now() + trace.work;
                while Instant::now() < end && !done.load(std::sync::atomic::Ordering::Relaxed) {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                next += 1;
            }
            (started, reports, report_ms)
        }
    };
    let receiver = tokio::spawn(receiver);
    let mut sent = Vec::new();
    for k in 0..trace.mentions {
        sent.push(ns(std::time::SystemTime::now()));
        send(&mut human, &format!("@alice m{k:02}")).await;
        for n in 0..trace.chatter {
            send(&mut human, &format!("chatter {k} {n}")).await;
        }
        tokio::time::sleep(trace.spacing).await;
    }
    // Every mention reaches the queue exactly once, in order, after the backlog.
    let mention = |k: usize| format!("human(human): @alice m{k:02}\n");
    let appended = || {
        let mut names: Vec<_> = std::fs::read_dir(&queue)
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .filter(|n| n.ends_with(".airc") && n.as_str() > "000002")
            .collect();
        names.sort();
        names
    };
    timeout(Duration::from_secs(90), async {
        loop {
            let last = mention(trace.mentions - 1);
            if appended()
                .iter()
                .any(|n| contents_sync(&queue.join(n)).contains(&last))
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("every mention reaches the queue");
    if trace.drain {
        // Every queued record, backlog included, reaches the receiver.
        let last = 2 + appended().len();
        timeout(Duration::from_secs(180), async {
            while progress.load(std::sync::atomic::Ordering::Relaxed) < last {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("the receiver starts every queued record");
    }
    done.store(true, std::sync::atomic::Ordering::Relaxed);
    let (started, reports, report_ms) = receiver.await.unwrap();
    child.kill().await.unwrap();
    for (name, text) in BACKLOG {
        assert_eq!(contents_sync(&queue.join(name)), text, "{name} changed");
    }
    let names = appended();
    let mut expected = 0;
    let (mut payload, mut handoff, mut turn) = (0, Vec::new(), Vec::new());
    let mut queued_at = Vec::new();
    for name in &names {
        let record = contents_sync(&queue.join(name));
        let stem = name.trim_end_matches(".airc");
        let at: u128 = contents_sync(&queue.join(format!("{stem}.at")))
            .trim()
            .parse()
            .unwrap();
        let number: usize = stem.parse().unwrap();
        queued_at.push((number, at));
        payload += record.len();
        let start = started.iter().find(|(n, _)| *n == number).map(|(_, t)| *t);
        for (k, &when) in sent.iter().enumerate() {
            let count = record.matches(&mention(k)).count();
            if count > 0 {
                assert_eq!((count, k), (1, expected), "{record}");
                expected += 1;
                handoff.push((at - when) / 1_000_000);
                if let Some(start) = start {
                    turn.push((start - when) / 1_000_000);
                }
            }
        }
    }
    assert_eq!(expected, trace.mentions, "every mention once, in order");
    if trace.drain {
        assert_eq!(turn.len(), trace.mentions, "one turn-start age per mention");
    }
    // Waiting records: queued (backlog included) but not yet started.
    let mut max_waiting = 0;
    for &(_, at) in &queued_at {
        let waiting = queued_at
            .iter()
            .filter(|&&(n, queued)| {
                queued <= at
                    && started
                        .iter()
                        .find(|(s, _)| *s == n)
                        .is_none_or(|(_, t)| *t > at)
            })
            .count()
            + BACKLOG
                .iter()
                .enumerate()
                .filter(|(i, _)| {
                    started
                        .iter()
                        .find(|(s, _)| *s == i + 1)
                        .is_none_or(|(_, t)| *t > at)
                })
                .count();
        max_waiting = max_waiting.max(waiting);
    }
    let invocations = contents_sync(&queue.join("next"))
        .trim()
        .parse::<usize>()
        .unwrap()
        - 3;
    let stats = |v: &[u128]| {
        (
            v.iter().sum::<u128>() / v.len().max(1) as u128,
            v.iter().copied().max().unwrap_or(0),
        )
    };
    f.ok(&["disconnect", "trial"]).await;
    server.shutdown().await.unwrap();
    Run {
        records: names.len(),
        invocations,
        payload,
        reports,
        report_ms,
        max_waiting,
        handoff_ms: stats(&handoff),
        turn_ms: trace.drain.then(|| stats(&turn)),
        turn_samples: turn.len(),
    }
}

fn contents_sync(path: &std::path::Path) -> String {
    std::fs::read_to_string(path).unwrap_or_default()
}

/// One JSON line per run. Off and on runs of a trace share one test process
/// and box, concurrently; times are compressed and every party is a mock.
/// Report-call times are local CLI time only, never model or tool round trips.
fn measured(name: &str, mode: &str, trace: Trace, run: &Run) {
    let turn = match run.turn_ms {
        Some((mean, max)) => json!({"mean_ms": mean, "max_ms": max, "samples": run.turn_samples}),
        None => json!({"censored": true, "samples": run.turn_samples}),
    };
    println!(
        "MEASURE {}",
        json!({
            "trace": name, "mode": mode, "mentions": trace.mentions,
            "spacing_ms": trace.spacing.as_millis(), "chatter": trace.chatter,
            "work_ms": trace.work.as_millis(), "receiver_reports": trace.reports,
            "drained": trace.drain, "sink_invocations": run.invocations,
            "queue_records": run.records, "payload_bytes": run.payload,
            "local_report_calls": run.reports, "local_report_cli_ms_total": run.report_ms,
            "local_report_cli_ms_mean": run.report_ms / run.reports.max(1) as u128,
            "max_waiting_records": run.max_waiting,
            "handoff_age_ms_mean": run.handoff_ms.0, "handoff_age_ms_max": run.handoff_ms.1,
            "turn_start_age": turn,
        })
    );
}

/// A busy receiver that reports each batch as its turn starts: the same
/// trace makes fewer queue records with receipts, losing and reordering
/// nothing, and the earlier backlog and other producer's record stay as they were.
#[tokio::test]
async fn receipt_credit_reduces_records_queued_for_a_busy_receiver() {
    let trace = Trace {
        mentions: 8,
        spacing: Duration::from_millis(2300),
        chatter: 0,
        work: Duration::from_secs(4),
        reports: true,
        drain: false,
    };
    let (off, on) = tokio::join!(replay(trace, None), replay(trace, Some(30)));
    measured("delayed-receiver", "off", trace, &off);
    measured("delayed-receiver", "receipt-30s", trace, &on);
    assert_eq!(off.invocations, off.records);
    assert_eq!(on.invocations, on.records);
    assert_eq!(off.reports, 0);
    assert!(
        on.reports >= on.records - 1,
        "{} of {}",
        on.reports,
        on.records
    );
    assert!(
        on.records < off.records && on.max_waiting < off.max_waiting,
        "on {}/{} off {}/{}",
        on.records,
        on.max_waiting,
        off.records,
        off.max_waiting
    );
}

/// The full matrix for the published measurement: run explicitly with
/// `cargo test --test named_sessions receipt_measurement -- --ignored --nocapture`.
#[tokio::test]
#[ignore = "measurement matrix; several minutes"]
async fn receipt_measurement_matrix() {
    let base = Trace {
        mentions: 8,
        spacing: Duration::from_millis(2300),
        chatter: 0,
        work: Duration::from_secs(4),
        reports: true,
        drain: true,
    };
    let traces = [
        (
            "prompt-receiver",
            Trace {
                work: Duration::from_millis(200),
                ..base
            },
            30,
        ),
        ("delayed-receiver", base, 30),
        (
            "absent-receipts",
            Trace {
                reports: false,
                ..base
            },
            8,
        ),
        (
            "pressure",
            Trace {
                mentions: 3,
                chatter: 200,
                reports: false,
                ..base
            },
            600,
        ),
    ];
    for (name, trace, timeout) in traces {
        let (off, on) = tokio::join!(replay(trace, None), replay(trace, Some(timeout)));
        measured(name, "off", trace, &off);
        measured(name, &format!("receipt-{timeout}s"), trace, &on);
    }
}

#[tokio::test]
async fn an_acceptance_that_cannot_be_saved_starts_no_hold() {
    let f = Fixture::new();
    let server = f.start().await;
    let mut human = client::connect(&f.client(&server, "human")).await.unwrap();
    f.connect(&server).await;
    f.ok(&["read", "trial"]).await;
    let dir = f.root.join("trial");
    let out = f.dir.path().join("batches");
    // The sink takes the batch, then the session directory becomes read-only
    // before the worker can save its acceptance and hold.
    let mut child = subscriber(
        &f,
        &[
            "--wake",
            "mentions",
            "--receipt-timeout",
            "600",
            "--receipt-scope",
            SCOPE,
        ],
        &[
            "sh",
            "-c",
            "cat >> \"$0\"; chmod 500 \"$1\"",
            out.to_str().unwrap(),
            dir.to_str().unwrap(),
        ],
    );
    send(&mut human, "@alice one").await;
    let ended = timeout(Duration::from_secs(15), child.wait())
        .await
        .unwrap()
        .unwrap();
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).unwrap();
    if std::fs::metadata(dir.join("state.json")).is_ok_and(|m| m.uid() == 0) {
        return; // A privileged run ignores directory modes.
    }
    assert!(!ended.success());
    let id = delivery_id(&contents(out.clone()).await);
    eventually(
        || async { text(&f.cli(&["status", "trial"]).await) },
        |s| s.contains("stopped;"),
    )
    .await;
    f.connect(&server).await;
    let status = f.ok(&["deliveries", "trial"]).await;
    assert!(
        status.contains(&format!("pending={id} ")) && !status.contains("delivery receipt:"),
        "{status}"
    );
    let early = run_notice(&f, &contents(out.clone()).await).await;
    assert!(
        text(&early).contains("transport acceptance is still pending"),
        "{}",
        text(&early)
    );
    f.ok(&["disconnect", "trial"]).await;
    server.shutdown().await.unwrap();
}

/// Connect `trial` with `--state-dir STORE` as given (possibly relative) from
/// `cwd`, then address the canonical store for everything else.
async fn connect_at(f: &mut Fixture, server: &RunningServer, store: &str, cwd: &std::path::Path) {
    let config = f.path(server);
    let output = tokio::process::Command::new(env!("CARGO_BIN_EXE_airc"))
        .args(["--state-dir", store, "connect", "trial", "--config"])
        .arg(&config)
        .current_dir(cwd)
        .stdin(Stdio::null())
        .output()
        .await
        .unwrap();
    assert!(output.status.success(), "{}", text(&output));
    f.root = std::fs::canonicalize(cwd.join(store)).unwrap();
}

/// A receipt-enabled subscriber writing batches to `out`, once registered.
async fn receipt_subscriber(
    f: &Fixture,
    out: &std::path::Path,
    extra: &[&str],
) -> tokio::process::Child {
    let mut args = vec![
        "--wake",
        "mentions",
        "--receipt-timeout",
        "600",
        "--receipt-scope",
        SCOPE,
    ];
    args.extend(extra);
    let child = subscriber(
        f,
        &args,
        &["sh", "-c", "cat >> \"$0\"", out.to_str().unwrap()],
    );
    eventually(
        || f.ok(&["status", "trial"]),
        |s| s.contains("delivery consumer: subscribe"),
    )
    .await;
    child
}

#[tokio::test]
async fn printed_report_commands_reach_hostile_relative_stores_from_any_environment() {
    let mut f = Fixture::new();
    let server = f.start().await;
    let mut human = client::connect(&f.client(&server, "human")).await.unwrap();
    // A relative store name with spaces, an apostrophe and literal shell syntax.
    let store = "st o'r $(touch inj1) `touch inj2`";
    let base = f.dir.path().to_path_buf();
    connect_at(&mut f, &server, store, &base).await;
    assert!(f.root.is_absolute() && f.root.ends_with(store));
    f.ok(&["read", "trial"]).await;
    let out = f.dir.path().join("batches");
    let _child = receipt_subscriber(&f, &out, &[]).await;
    send(&mut human, "@alice one").await;
    let first = eventually(|| contents(out.clone()), |s| s.contains("@alice one")).await;
    let id = delivery_id(&first);
    let command = notice_command(&first);
    assert_eq!(
        command,
        format!(
            "airc --state-dir '{}' deliveries trial --mark-received {id} --receipt-scope {SCOPE}",
            f.root.display().to_string().replace('\'', r"'\''")
        )
    );
    eventually(
        || f.ok(&["deliveries", "trial"]),
        |s| s.contains(&format!("delivery receipt: hold={id} ")),
    )
    .await;
    // Another store or an unknown ID changes nothing; nothing is guessed.
    let elsewhere = f.dir.path().join("other-store");
    let wrong = run_printed(
        &command.replace(
            &format!("'{}'", f.root.display().to_string().replace('\'', r"'\''")),
            &elsewhere.display().to_string(),
        ),
        f.dir.path(),
    )
    .await;
    assert!(
        !wrong.status.success() && text(&wrong).contains("is stopped"),
        "{}",
        text(&wrong)
    );
    let unknown = run_printed(
        &command.replace(&id, "00000000-0000-4000-8000-000000000000"),
        f.dir.path(),
    )
    .await;
    assert!(
        text(&unknown).contains("no pending or retained"),
        "{}",
        text(&unknown)
    );
    assert!(
        f.ok(&["deliveries", "trial"])
            .await
            .contains(&format!("hold={id} "))
    );
    // The genuine command, from another HOME/XDG and directory, records the
    // exact ID and scope; no shell syntax in the path ever ran.
    let reported = run_notice(&f, &first).await;
    assert!(
        text(&reported).contains(&format!("received {id} at ")),
        "{}",
        text(&reported)
    );
    let ledger = f.ok(&["deliveries", "trial"]).await;
    assert!(
        ledger.contains(&format!("receipt {id} scope={SCOPE} "))
            && ledger.contains("released=received"),
        "{ledger}"
    );
    for dir in [
        f.dir.path().to_path_buf(),
        f.dir.path().join("elsewhere"),
        f.root.clone(),
    ] {
        for sentinel in ["inj1", "inj2"] {
            assert!(!dir.join(sentinel).exists(), "{}", dir.display());
        }
    }
    let mut elsewhere: Vec<_> = std::fs::read_dir(f.dir.path().join("elsewhere"))
        .unwrap()
        .map(|e| e.unwrap().file_name())
        .collect();
    elsewhere.sort();
    assert_eq!(elsewhere, ["home"]);
    f.ok(&["disconnect", "trial"]).await;
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn printed_report_commands_keep_a_default_store_when_xdg_changes() {
    let mut f = Fixture::new();
    let server = f.start().await;
    let mut human = client::connect(&f.client(&server, "human")).await.unwrap();
    // The worker's store is the default under one XDG_STATE_HOME ...
    let xdg = f.dir.path().join("xdg");
    let output = tokio::process::Command::new(env!("CARGO_BIN_EXE_airc"))
        .args(["connect", "trial", "--config"])
        .arg(f.path(&server))
        .env("XDG_STATE_HOME", &xdg)
        .stdin(Stdio::null())
        .output()
        .await
        .unwrap();
    assert!(output.status.success(), "{}", text(&output));
    f.root = std::fs::canonicalize(xdg.join("airc")).unwrap();
    f.ok(&["read", "trial"]).await;
    let out = f.dir.path().join("batches");
    let _child = receipt_subscriber(&f, &out, &[]).await;
    send(&mut human, "@alice one").await;
    let first = eventually(|| contents(out.clone()), |s| s.contains("@alice one")).await;
    assert!(
        notice_command(&first).starts_with(&format!(
            "airc --state-dir {} deliveries trial ",
            f.root.display()
        )),
        "{first}"
    );
    eventually(
        || f.ok(&["deliveries", "trial"]),
        |s| s.contains("delivery receipt: hold="),
    )
    .await;
    // ... and the receiver runs with another one.
    let reported = run_notice(&f, &first).await;
    assert!(
        text(&reported).contains("its hold is released"),
        "{}",
        text(&reported)
    );
    f.ok(&["disconnect", "trial"]).await;
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn receipt_opt_in_is_refused_with_wrap_or_without_room_and_changes_nothing() {
    // The longest all-apostrophe store whose socket path stays below 100
    // bytes, under a short fixed base: its quoted notice cannot fit 1024 bytes.
    let short = tempfile::Builder::new()
        .prefix("r")
        .rand_bytes(4)
        .tempdir_in("/tmp")
        .unwrap();
    let store = "'".repeat(99 - short.path().as_os_str().len() - 1 - "/trial/client.sock".len());
    let mut f = Fixture::new();
    let server = f.start().await;
    let mut human = client::connect(&f.client(&server, "human")).await.unwrap();
    connect_at(&mut f, &server, &store, short.path()).await;
    assert_eq!(f.root.join("trial/client.sock").as_os_str().len(), 99);
    let before = f.ok(&["deliveries", "trial"]).await;
    assert!(before.starts_with("delivery: unused\n"), "{before}");
    let base = [
        "subscribe",
        "trial",
        "--receipt-timeout",
        "60",
        "--receipt-scope",
        SCOPE,
    ];
    for (extra, expected) in [
        (vec!["--wrap", "480"], "cannot be combined with --wrap"),
        (vec!["--max-bytes", "1024"], "raise --max-bytes"),
    ] {
        let mut argv = base.to_vec();
        argv.extend(&extra);
        argv.extend(["--", "-"]);
        let refused = f.cli(&argv).await;
        assert!(
            !refused.status.success() && text(&refused).contains(expected),
            "{extra:?}: {}",
            text(&refused)
        );
        assert_eq!(f.ok(&["deliveries", "trial"]).await, before);
    }
    // The worker refuses the same registrations itself, before any change.
    for settings in [
        json!({"wake": "mentions", "max_bytes": 8192, "min_interval": 0, "wrap": 480,
            "receipt": {"timeout": 60, "scope": SCOPE}}),
        json!({"wake": "mentions", "max_bytes": 1024, "min_interval": 0,
            "receipt": {"timeout": 60, "scope": SCOPE}}),
    ] {
        let refused = raw_ipc(
            &f,
            json!({"op": "consume", "kind": "subscribe", "settings": settings}),
        )
        .await;
        assert_eq!(refused["ok"], false, "{refused}");
        assert_eq!(f.ok(&["deliveries", "trial"]).await, before);
    }
    // Default-off wrapping is unchanged, and a large enough bound fits.
    let out = f.dir.path().join("batches");
    let mut plain = subscriber(&f, &["--wrap", "480"], &["sh", "-c", "cat >/dev/null"]);
    eventually(
        || f.ok(&["status", "trial"]),
        |s| s.contains("wrap=480 min-interval=0\n"),
    )
    .await;
    plain.kill().await.unwrap();
    eventually(
        || f.ok(&["status", "trial"]),
        |s| s.contains("delivery consumer: none"),
    )
    .await;
    let _child = receipt_subscriber(&f, &out, &["--max-bytes", "2048"]).await;
    send(&mut human, "@alice one").await;
    let first = eventually(|| contents(out.clone()), |s| s.contains("@alice one")).await;
    assert!(first.len() <= 2048);
    eventually(
        || f.ok(&["deliveries", "trial"]),
        |s| s.contains("delivery receipt: hold="),
    )
    .await;
    let reported = run_notice(&f, &first).await;
    assert!(
        text(&reported).contains("its hold is released"),
        "{}",
        text(&reported)
    );
    f.ok(&["disconnect", "trial"]).await;
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn reports_need_a_connection_other_than_the_consumers() {
    let f = Fixture::new();
    let server = f.start().await;
    f.connect(&server).await;
    let stream = UnixStream::connect(f.root.join("trial/client.sock"))
        .await
        .unwrap();
    let mut stream = BufReader::new(stream);
    let mut exchange = async |operation: serde_json::Value| {
        stream
            .get_mut()
            .write_all(format!("{}\n", json!({"version": 2, "operation": operation})).as_bytes())
            .await
            .unwrap();
        let line = timeout(Duration::from_secs(15), read_line(&mut stream, 1 << 20))
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        serde_json::from_str::<serde_json::Value>(&line).unwrap()
    };
    let registered = exchange(json!({"op": "consume", "kind": "subscribe", "settings": {
        "wake": "mentions", "max_bytes": 8192, "min_interval": 0,
        "receipt": {"timeout": 60, "scope": SCOPE}}}))
    .await;
    assert_eq!(registered["ok"], true, "{registered}");
    let id = "00000000-0000-4000-8000-000000000000";
    let own = exchange(json!({"op": "mark_received", "id": id, "scope": SCOPE})).await;
    assert!(
        own["text"]
            .as_str()
            .unwrap()
            .contains("a consumer connection only delivers and accepts"),
        "{own}"
    );
    let separate = raw_ipc(&f, json!({"op": "mark_received", "id": id, "scope": SCOPE})).await;
    assert!(
        separate["text"]
            .as_str()
            .unwrap()
            .contains("no pending or retained"),
        "{separate}"
    );
    drop(stream);
    f.ok(&["disconnect", "trial"]).await;
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_pending_receipt_batch_is_never_retried_wrapped() {
    let f = Fixture::new();
    let server = f.start().await;
    let mut human = client::connect(&f.client(&server, "human")).await.unwrap();
    f.connect(&server).await;
    f.ok(&["read", "trial"]).await;
    let out = f.dir.path().join("batches");
    // A receipt-enabled sink fails, leaving its batch pending and uncertain.
    let failing = subscriber(
        &f,
        &[
            "--wake",
            "mentions",
            "--receipt-timeout",
            "600",
            "--receipt-scope",
            SCOPE,
        ],
        &["sh", "-c", "cat >> \"$0\"; exit 7", out.to_str().unwrap()],
    );
    send(&mut human, "@alice one").await;
    let ended = timeout(Duration::from_secs(15), failing.wait_with_output())
        .await
        .unwrap()
        .unwrap();
    assert!(!ended.status.success());
    let first = contents(out.clone()).await;
    let pending = delivery_id(&first);
    let before = f.ok(&["deliveries", "trial"]).await;
    assert!(before.contains(&format!("pending={pending} ")), "{before}");
    let shown = f.ok(&["deliveries", "trial", "--show", &pending]).await;
    // A wrapped retry without receipt options of its own is refused by the
    // worker before registering; nothing reaches a sink or changes.
    let refused = timeout(
        Duration::from_secs(15),
        subscriber(
            &f,
            &["--retry-pending", &pending, "--wrap", "64"],
            &["sh", "-c", "cat >> \"$0\"", out.to_str().unwrap()],
        )
        .wait_with_output(),
    )
    .await
    .expect("a wrapped retry of a receipt batch is refused, not served")
    .unwrap();
    assert!(
        !refused.status.success()
            && text(&refused).contains(&format!(
                "pending delivery {pending} requests a receipt; retry it without --wrap"
            )),
        "{}",
        text(&refused)
    );
    assert_eq!(contents(out.clone()).await, first, "no sink call");
    assert_eq!(f.ok(&["deliveries", "trial"]).await, before);
    assert_eq!(
        f.ok(&["deliveries", "trial", "--show", &pending]).await,
        shown
    );
    // An unwrapped retry delivers the saved command, which reports the
    // original ID and scope to the original store.
    let _retry = subscriber(
        &f,
        &["--retry-pending", &pending],
        &["sh", "-c", "cat >> \"$0\"", out.to_str().unwrap()],
    );
    let all = eventually(|| contents(out.clone()), |s| batches(s).len() == 2).await;
    let retried = batches(&all)[1];
    assert!(
        retried.starts_with(&format!("[airc id={pending} session=trial retry]\n")),
        "{retried}"
    );
    assert_eq!(notice_command(retried), notice_command(&first));
    eventually(
        || f.ok(&["deliveries", "trial"]),
        |s| s.contains(&format!("delivery receipt: hold={pending} scope={SCOPE} ")),
    )
    .await;
    let reported = run_notice(&f, retried).await;
    assert!(
        text(&reported).contains(&format!("received {pending} at "))
            && text(&reported).contains("hold is released"),
        "{}",
        text(&reported)
    );
    f.ok(&["disconnect", "trial"]).await;
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_pending_batch_without_a_receipt_still_retries_wrapped() {
    let f = Fixture::new();
    let server = f.start().await;
    let mut human = client::connect(&f.client(&server, "human")).await.unwrap();
    f.connect(&server).await;
    f.ok(&["read", "trial"]).await;
    let out = f.dir.path().join("batches");
    let failing = subscriber(
        &f,
        &["--wake", "mentions"],
        &["sh", "-c", "cat >> \"$0\"; exit 7", out.to_str().unwrap()],
    );
    send(&mut human, &format!("@alice {}", "x".repeat(150))).await;
    timeout(Duration::from_secs(15), failing.wait_with_output())
        .await
        .unwrap()
        .unwrap();
    let pending = delivery_id(&contents(out.clone()).await);
    let _retry = subscriber(
        &f,
        &["--retry-pending", &pending, "--wrap", "64"],
        &["sh", "-c", "cat >> \"$0\"", out.to_str().unwrap()],
    );
    let all = eventually(|| contents(out.clone()), |s| batches(s).len() == 2).await;
    let retried = batches(&all)[1];
    assert!(retried.starts_with(&format!("[airc id={pending} session=trial retry]\n")));
    assert!(
        retried.lines().skip(1).all(|line| line.len() <= 64),
        "{retried}"
    );
    assert!(retried.contains("\n| "), "{retried}");
    eventually(
        || f.ok(&["deliveries", "trial"]),
        |s| s.contains("pending=none") && s.contains(&format!("last={pending} accepted=")),
    )
    .await;
    f.ok(&["disconnect", "trial"]).await;
    server.shutdown().await.unwrap();
}

/// `--sink-timeout 0`: the adapter may hold one batch with no deadline while
/// later mentions accumulate for the next batch, and stopping the subscriber
/// kills the adapter's process group. Bounded sinks keep their deadline and
/// their cleanup, which leaves the adapter's own children alone.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn unbounded_sink_holds_its_batch_and_its_group_stops_with_the_subscriber() {
    use rustix::process::{Pid, Signal, kill_process};
    fn running(pid: i32) -> bool {
        std::fs::read_to_string(format!("/proc/{pid}/stat")).is_ok_and(|stat| {
            let state = stat.rsplit_once(") ").map(|(_, rest)| rest.as_bytes()[0]);
            !matches!(state, Some(b'Z' | b'X'))
        })
    }
    let terminate = |pid: u32| {
        kill_process(Pid::from_raw(pid as i32).unwrap(), Signal::TERM).unwrap();
    };
    let f = Fixture::new();
    let server = f.start().await;
    let mut human = client::connect(&f.client(&server, "human")).await.unwrap();
    f.connect(&server).await;
    f.ok(&["read", "trial"]).await;
    let registered = |pid: u32| {
        let f = &f;
        async move {
            eventually(
                || f.ok(&["status", "trial"]),
                |s| s.contains(&format!("delivery consumer: subscribe pid={pid} ")),
            )
            .await
        }
    };
    let pending = |status: &str| {
        status
            .split_once(" pending=")
            .and_then(|(_, rest)| rest.split(' ').next())
            .unwrap()
            .to_string()
    };
    // A bounded sink is still killed at its deadline.
    let bounded = subscriber(
        &f,
        &["--wake", "mentions", "--sink-timeout", "1"],
        &["sh", "-c", "cat > /dev/null; exec sleep 30"],
    );
    registered(bounded.id().unwrap()).await;
    send(&mut human, "@alice bounded").await;
    let ended = timeout(Duration::from_secs(15), bounded.wait_with_output())
        .await
        .unwrap()
        .unwrap();
    assert!(
        text(&ended).contains("adapter exceeded 1 s and was killed"),
        "{}",
        text(&ended)
    );
    let status = f.ok(&["status", "trial"]).await;
    f.ok(&["deliveries", "trial", "--drop", &pending(&status)])
        .await;
    // Holds each batch until "$0.go" appears; records "adapter background-child".
    let script = "cat >> \"$0\"; sleep 300 < /dev/null > /dev/null 2>&1 & echo \"$$ $!\" >> \"$0.pids\"; \
        while [ ! -e \"$0.go\" ]; do sleep 0.05; done; rm \"$0.go\"; kill $!";
    let out = f.dir.path().join("held");
    let gate = out.with_extension("go");
    let pids = |path: &PathBuf| {
        let pids = std::fs::read_to_string(path.with_extension("pids")).unwrap();
        let last = pids.lines().last().unwrap().to_string();
        let (adapter, child) = last.split_once(' ').unwrap();
        (
            adapter.parse::<i32>().unwrap(),
            child.parse::<i32>().unwrap(),
        )
    };
    let held = subscriber(
        &f,
        &["--wake", "mentions", "--sink-timeout", "0"],
        &["sh", "-c", script, out.to_str().unwrap()],
    );
    let pid = held.id().unwrap();
    registered(pid).await;
    send(&mut human, "@alice lead").await;
    eventually(|| contents(out.clone()), |s| s.contains("@alice lead")).await;
    for n in 1..=3 {
        send(&mut human, &format!("@alice burst {n}")).await;
    }
    // Past the bounded sink's deadline, the same consumer still holds the lead.
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let status = f.ok(&["status", "trial"]).await;
    let lead = delivery_id(&contents(out.clone()).await);
    assert!(
        status.contains(&format!("delivery consumer: subscribe pid={pid} "))
            && pending(&status) == lead,
        "{status}"
    );
    std::fs::write(&gate, "").unwrap();
    // The lead stays alone; the next batch carries the accumulated burst in order.
    let both = eventually(
        || contents(out.clone()),
        |s| batches(s).len() == 2 && s.contains("@alice burst 3"),
    )
    .await;
    let held_batches = batches(&both);
    assert!(!held_batches[0].contains("burst"), "{both}");
    let second = held_batches[1];
    let at = |n: u32| second.find(&format!("@alice burst {n}")).unwrap();
    assert!(at(1) < at(2) && at(2) < at(3), "{second}");
    let second_id = delivery_id(second);
    eventually(
        || async { std::fs::read_to_string(out.with_extension("pids")).unwrap_or_default() },
        |s| s.lines().count() == 2,
    )
    .await;
    let (adapter, child) = pids(&out);
    assert!(running(adapter) && running(child));
    // Stopping the subscriber kills the whole group and keeps the exact batch.
    terminate(pid);
    let stopped = timeout(Duration::from_secs(15), held.wait_with_output())
        .await
        .unwrap()
        .unwrap();
    assert!(
        text(&stopped).contains("stopped by signal"),
        "{}",
        text(&stopped)
    );
    eventually(
        || async { format!("{} {}", running(adapter), running(child)) },
        |s| s == "false false",
    )
    .await;
    let status = eventually(
        || f.ok(&["status", "trial"]),
        |s| s.contains("delivery consumer: none"),
    )
    .await;
    assert_eq!(pending(&status), second_id);
    // `--show` always restates the batch's settings after the header.
    let notice = "! delivery settings: account=alice nick=alice wake=mentions max-bytes=8192 wrap=off min-interval=0\n";
    assert_eq!(
        f.ok(&["deliveries", "trial", "--show", &second_id]).await,
        second.replacen("]\n", &format!("]\n{notice}"), 1)
    );
    // The default bounded sink keeps today's cleanup: only the adapter dies.
    f.ok(&["deliveries", "trial", "--drop", &second_id]).await;
    let out = f.dir.path().join("bounded");
    let default = subscriber(
        &f,
        &["--wake", "mentions"],
        &["sh", "-c", script, out.to_str().unwrap()],
    );
    let pid = default.id().unwrap();
    registered(pid).await;
    send(&mut human, "@alice default").await;
    eventually(
        || async { std::fs::read_to_string(out.with_extension("pids")).unwrap_or_default() },
        |s| s.lines().count() == 1,
    )
    .await;
    let (adapter, child) = pids(&out);
    terminate(pid);
    timeout(Duration::from_secs(15), default.wait_with_output())
        .await
        .unwrap()
        .unwrap();
    eventually(|| async { running(adapter).to_string() }, |s| s == "false").await;
    assert!(
        running(child),
        "a bounded adapter's own child is left alone"
    );
    kill_process(Pid::from_raw(child).unwrap(), Signal::KILL).unwrap();
    f.ok(&["disconnect", "trial"]).await;
    server.shutdown().await.unwrap();
}
