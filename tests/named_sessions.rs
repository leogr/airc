#![cfg(unix)]

use airc::{
    client::{self, ClientConfig},
    config::{Account, Channel, Config, Limits, token_hash, write_private},
    protocol::{Message, read_line},
    server::{self, RunningServer},
};
use serde_json::json;
use std::{
    os::unix::fs::PermissionsExt,
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
            listen: "127.0.0.1:0".parse().unwrap(),
            database: dir.path().join("state.sqlite3"),
            allow_insecure: false,
            allow_private_messages: false,
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
            port: server.address.port(),
            tls: false,
            allow_insecure: false,
            ca_certificate: None,
            proxy: None,
            account: name.into(),
            nick: name.into(),
            token_file: token,
            channels: vec!["#work".into()],
            artifact_url: None,
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
        // Explicitly close the disposable child even when an assertion panics.
        let _ = std::process::Command::new(env!("CARGO_BIN_EXE_airc"))
            .arg("--state-dir")
            .arg(&self.root)
            .args(["disconnect", "trial"])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
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
        .send(Message::new("PRIVMSG", ["#work", text]))
        .await
        .unwrap();
    next(connection, "PRIVMSG").await
}

#[tokio::test]
async fn named_connection_send_reply_read_replay_and_stopped_lifecycle() {
    let f = Fixture::new();
    let server = f.start().await;
    let mut human = client::connect(&f.client(&server, "human")).await.unwrap();
    send(&mut human, "before connect").await;
    f.connect(&server).await;
    let output = f.ok(&["read", "trial"]).await;
    assert!(
        output.contains("history @1 #work human(human): before connect"),
        "{output}"
    );
    assert_eq!(f.ok(&["read", "trial", "--again"]).await, output);
    assert!(
        f.ok(&["send", "trial", "#work", "identical", "--reply", "@1"])
            .await
            .contains("confirmed #work @2")
    );
    let first = next(&mut human, "PRIVMSG").await;
    assert_eq!(first.params[1], "identical");
    assert!(first.tags.contains_key("+reply"));
    assert!(
        f.ok(&["send", "trial", "#work", "identical"])
            .await
            .contains("confirmed #work @3")
    );
    let second = next(&mut human, "PRIVMSG").await;
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
    // Start a real IPC wait and synchronize acceptance through a second read.
    let mut socket = BufReader::new(
        UnixStream::connect(f.root.join("trial/client.sock"))
            .await
            .unwrap(),
    );
    socket.get_mut().write_all(b"{\"version\":1,\"operation\":{\"op\":\"read\",\"wait\":1,\"limit\":50,\"again\":false}}\n").await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let duplicate = f.cli(&["read", "trial"]).await;
    assert!(!duplicate.status.success());
    assert!(String::from_utf8_lossy(&duplicate.stdout).contains("another read"));
    let start = Instant::now();
    let mut typing = Message::new("TAGMSG", ["#work"]);
    typing.tags.insert("+typing".into(), "active".into());
    for _ in 0..30 {
        human.outbound.send(typing.clone()).await.unwrap();
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
    abandoned.write_all(b"{\"version\":1,\"operation\":{\"op\":\"read\",\"wait\":60,\"limit\":50,\"again\":false}}\n").await.unwrap();
    drop(abandoned);
    send(&mut human, "actionable").await;
    let line = f.ok(&["read", "trial", "--wait", "2"]).await;
    assert!(line.contains("human(human): actionable"), "{line}");
    f.ok(&["disconnect", "trial"]).await;
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
    stream.get_mut().write_all(b"{\"version\":1,\"operation\":{\"op\":\"read\",\"wait\":2,\"limit\":50,\"again\":false}}\n").await.unwrap();
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
    let target = server.address;
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
                            while let Ok(Some(line)) = read_line(&mut remote, 4608).await {
                                let message = Message::parse(&line).unwrap();
                                if inject && message.command == "PRIVMSG" && message.tags.contains_key("+airc-send") {
                                    if matches!(fault, Fault::EchoTimeout) { std::future::pending::<()>().await; }
                                    if matches!(fault, Fault::AfterCommit) { break; }
                                }
                                if write.write_all(line.as_bytes()).await.is_err() { break; }
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
        .send(Message::new("CHATHISTORY", ["LATEST", "#work", "*", "20"]))
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
