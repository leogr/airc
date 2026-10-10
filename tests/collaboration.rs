use airc::{
    client::{self, ClientConfig},
    config::{Account, Artifacts, Channel, Config, Limits, token_hash, write_private},
    protocol::{Message, read_line},
    server::{self, RunningServer},
};
use std::time::Duration;
use tokio::{
    io::{AsyncWriteExt, BufReader},
    net::{
        TcpStream,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
};

const TOKEN: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

struct Fixture {
    dir: tempfile::TempDir,
    config: Config,
}
impl Fixture {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let config = Config {
            server_name: "test.airc".into(),
            listen: "127.0.0.1:0".parse().unwrap(),
            database: dir.path().join("state.db"),
            allow_insecure: false,
            allow_private_messages: false,
            tls: None,
            limits: Limits {
                history_per_channel: 4,
                ..Limits::default()
            },
            accounts: ["human", "alice", "bob", "outsider"]
                .map(|name| Account {
                    name: name.into(),
                    token_sha256: token_hash(TOKEN),
                })
                .to_vec(),
            channels: vec![Channel {
                name: "#work".into(),
                members: vec!["human".into(), "alice".into(), "bob".into()],
                operators: vec!["human".into()],
                moderated: false,
            }],
            artifacts: Some(Artifacts {
                listen: "127.0.0.1:0".parse().unwrap(),
                public_url: "http://127.0.0.1:0".into(),
                max_bytes: 32,
                per_channel: 2,
            }),
        };
        Self { dir, config }
    }
    async fn start(&self) -> RunningServer {
        server::start(self.config.clone()).await.unwrap()
    }
    fn client_config(&self, server: &RunningServer, name: &str) -> ClientConfig {
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
            account: name.into(),
            token_file: token,
            nick: name.into(),
            channels: vec!["#work".into()],
            artifact_url: Some(format!("http://{}", server.artifact_address.unwrap())),
        }
    }
}

struct Peer {
    reader: BufReader<OwnedReadHalf>,
    writer: OwnedWriteHalf,
}
impl Peer {
    async fn open(server: &RunningServer) -> Self {
        let (read, writer) = TcpStream::connect(server.address)
            .await
            .unwrap()
            .into_split();
        Self {
            reader: BufReader::new(read),
            writer,
        }
    }
    async fn login(server: &RunningServer, name: &str, extensions: bool) -> Self {
        let mut peer = Self::open(server).await;
        if extensions {
            peer.send("CAP LS 302").await;
            peer.until("CAP").await;
            peer.send("CAP REQ :message-tags server-time batch echo-message account-tag draft/chathistory").await;
            peer.until("CAP").await;
        }
        peer.send(&format!("PASS {name}:{TOKEN}")).await;
        peer.send(&format!("NICK {name}")).await;
        peer.send(&format!("USER {name} 0 * :Test participant"))
            .await;
        if extensions {
            peer.send("CAP END").await;
        }
        peer.until("001").await;
        peer
    }
    async fn send(&mut self, line: &str) {
        self.writer
            .write_all(format!("{line}\r\n").as_bytes())
            .await
            .unwrap();
    }
    async fn next(&mut self) -> Message {
        let line = tokio::time::timeout(Duration::from_secs(5), read_line(&mut self.reader, 4608))
            .await
            .expect("response timeout")
            .unwrap()
            .expect("server disconnected");
        Message::parse(&line).unwrap()
    }
    async fn until(&mut self, command: &str) -> Message {
        for _ in 0..600 {
            let m = self.next().await;
            if m.command == command {
                return m;
            }
        }
        panic!("no {command}");
    }
    async fn join(&mut self) {
        self.send("JOIN #work").await;
        self.until("366").await;
    }
    async fn history(&mut self, request: &str) -> Vec<Message> {
        self.send(request).await;
        let start = self.until("BATCH").await;
        assert!(start.params[0].starts_with('+'));
        let mut messages = Vec::new();
        loop {
            let m = self.next().await;
            if m.command == "BATCH" {
                break;
            }
            messages.push(m);
        }
        messages
    }
}

#[tokio::test]
async fn humans_agents_moderation_and_private_message_policy() {
    let fixture = Fixture::new();
    let server = fixture.start().await;
    let mut human = Peer::login(&server, "human", false).await;
    let mut alice = Peer::login(&server, "alice", true).await;
    let mut bob = Peer::login(&server, "bob", true).await;
    human.join().await;
    alice.join().await;
    bob.join().await;
    alice
        .send("@time=forged;account=human;msgid=forged;+reply=reference PRIVMSG #work :hello human")
        .await;
    let human_message = human.until("PRIVMSG").await;
    assert!(human_message.tags.is_empty());
    assert_eq!(human_message.params[1], "hello human");
    let echo = alice.until("PRIVMSG").await;
    assert_eq!(echo.tags["account"], "alice");
    assert_ne!(echo.tags["msgid"], "forged");
    assert_ne!(echo.tags["time"], "forged");
    assert_eq!(echo.tags["+reply"], "reference");
    bob.until("PRIVMSG").await;
    for command in [
        "PRIVMSG bob :hidden",
        "PRIVMSG bob :\u{1}DCC SEND file\u{1}",
        "TAGMSG bob",
    ] {
        alice.send(command).await;
        alice.until("404").await;
    }
    alice.send("NOTICE bob :hidden notice").await;
    alice.send("PRIVMSG #work :visible marker").await;
    let visible = bob.until("PRIVMSG").await;
    assert_eq!(visible.params[1], "visible marker");
    // NOTICE must neither leak to the recipient nor generate an error reply.
    alice.until("PRIVMSG").await;
    human.send("MODE #work +m").await;
    human.until("MODE").await;
    bob.send("PRIVMSG #work :blocked").await;
    bob.until("404").await;
    bob.send("MODE #work +v bob").await;
    bob.until("482").await;
    human.send("MODE #work +v bob").await;
    human.until("MODE").await;
    bob.send("PRIVMSG #work :voiced").await;
    assert_eq!(bob.until("PRIVMSG").await.params[1], "voiced");
    // A nickname that looks like an account never grants that account's rights.
    alice.send("NICK operator").await;
    alice.until("NICK").await;
    alice.send("MODE #work -m").await;
    alice.until("482").await;
    let mut outsider = Peer::login(&server, "outsider", true).await;
    outsider.send("JOIN #work").await;
    outsider.until("403").await;
    outsider.send("CHATHISTORY LATEST #work * 10").await;
    assert_eq!(outsider.until("FAIL").await.params[1], "INVALID_TARGET");
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn history_survives_restart_and_reports_lost_references() {
    let fixture = Fixture::new();
    let server = fixture.start().await;
    let mut alice = Peer::login(&server, "alice", true).await;
    alice.join().await;
    let mut ids = Vec::new();
    for n in 0..6 {
        alice.send(&format!("PRIVMSG #work :message {n}")).await;
        ids.push(alice.until("PRIVMSG").await.tags["msgid"].clone());
    }
    let recent = alice.history("CHATHISTORY LATEST #work * 100").await;
    assert_eq!(recent.len(), 4);
    assert_eq!(recent[0].params[1], "message 2");
    alice
        .send(&format!("CHATHISTORY AFTER #work msgid={} 100", ids[0]))
        .await;
    assert_eq!(alice.until("FAIL").await.params[1], "HISTORY_LOST");
    let after = alice
        .history(&format!("CHATHISTORY AFTER #work msgid={} 1", ids[2]))
        .await;
    assert_eq!(after[0].tags["msgid"], ids[3]);
    let before = alice
        .history(&format!("CHATHISTORY BEFORE #work msgid={} 2", ids[5]))
        .await;
    assert_eq!(
        before
            .iter()
            .map(|m| m.tags["msgid"].clone())
            .collect::<Vec<_>>(),
        ids[3..5]
    );
    let reverse = alice
        .history(&format!(
            "CHATHISTORY BETWEEN #work msgid={} msgid={} 1",
            ids[5], ids[2]
        ))
        .await;
    assert_eq!(reverse[0].tags["msgid"], ids[4]);
    server.shutdown().await.unwrap();
    drop(alice);
    let server = fixture.start().await;
    let mut bob = Peer::login(&server, "bob", true).await;
    bob.join().await;
    let restored = bob.history("CHATHISTORY LATEST #work * 100").await;
    assert_eq!(
        restored
            .iter()
            .map(|m| m.tags["msgid"].clone())
            .collect::<Vec<_>>(),
        ids[2..]
    );
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn artifact_access_limits_and_moderation() {
    let fixture = Fixture::new();
    let server = fixture.start().await;
    let mut alice = Peer::login(&server, "alice", true).await;
    alice.join().await;
    let http = reqwest::Client::new();
    let base = format!("http://{}", server.artifact_address.unwrap());
    let endpoint = format!("{base}/v1/channels/%23work/artifacts");
    let upload = http
        .post(&endpoint)
        .basic_auth("alice", Some(TOKEN))
        .header("x-airc-filename", "result.txt")
        .body("test result")
        .send()
        .await
        .unwrap();
    assert_eq!(upload.status(), 201);
    let value: serde_json::Value = upload.json().await.unwrap();
    let url = value["url"].as_str().unwrap();
    assert!(alice.until("PRIVMSG").await.params[1].contains(url));
    assert_eq!(http.get(url).send().await.unwrap().status(), 401);
    assert_eq!(
        http.get(url)
            .basic_auth("outsider", Some(TOKEN))
            .send()
            .await
            .unwrap()
            .status(),
        404
    );
    let download = http
        .get(url)
        .basic_auth("bob", Some(TOKEN))
        .send()
        .await
        .unwrap();
    assert_eq!(download.status(), 200);
    assert_eq!(download.bytes().await.unwrap(), "test result");
    assert_eq!(
        http.post(&endpoint)
            .basic_auth("alice", Some(TOKEN))
            .header("x-airc-filename", "big.txt")
            .body(vec![0; 33])
            .send()
            .await
            .unwrap()
            .status(),
        413
    );
    let mut human = Peer::login(&server, "human", false).await;
    human.join().await;
    human.send("MODE #work +m").await;
    human.until("MODE").await;
    assert_eq!(
        http.post(&endpoint)
            .basic_auth("alice", Some(TOKEN))
            .header("x-airc-filename", "x.txt")
            .body("blocked")
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn sasl_client_and_jsonl_frontend() {
    let fixture = Fixture::new();
    let server = fixture.start().await;
    let config = fixture.client_config(&server, "alice");
    let mut connection = client::connect(&config).await.unwrap();
    loop {
        if connection.inbound.recv().await.unwrap().command == "366" {
            break;
        }
    }
    connection
        .outbound
        .send(Message::new("PRIVMSG", ["#work", "from agent"]))
        .await
        .unwrap();
    loop {
        let m = connection.inbound.recv().await.unwrap();
        if m.command == "PRIVMSG" {
            assert_eq!(m.tags["account"], "alice");
            break;
        }
    }
    drop(connection);
    let config = fixture.client_config(&server, "bob");
    let (mut requests, input) = tokio::io::duplex(65536);
    let (output, responses) = tokio::io::duplex(65536);
    let task = tokio::spawn(client::frontend(config, BufReader::new(input), output));
    let mut responses = BufReader::new(responses);
    let ready = read_line(&mut responses, 65536).await.unwrap().unwrap();
    assert!(ready.contains("ready"));
    requests
        .write_all(b"{\"id\":1,\"op\":\"read\",\"after\":0,\"wait_ms\":1000}\n")
        .await
        .unwrap();
    let value: serde_json::Value =
        serde_json::from_str(&read_line(&mut responses, 65536).await.unwrap().unwrap()).unwrap();
    assert_eq!(value["id"], 1);
    assert_eq!(value["ok"], true, "{value}");
    requests
        .write_all(
            b"{\"id\":2,\"op\":\"send\",\"target\":\"#work\",\"text\":\"hi\\r\\nOPER root\"}\n",
        )
        .await
        .unwrap();
    let value: serde_json::Value =
        serde_json::from_str(&read_line(&mut responses, 65536).await.unwrap().unwrap()).unwrap();
    assert_eq!(value["ok"], false);
    let source = fixture.dir.path().join("report.txt");
    std::fs::write(&source, "client artifact").unwrap();
    let upload = serde_json::json!({"id":"upload","op":"upload","channel":"#work","path":source});
    requests
        .write_all(format!("{upload}\n").as_bytes())
        .await
        .unwrap();
    let value: serde_json::Value =
        serde_json::from_str(&read_line(&mut responses, 65536).await.unwrap().unwrap()).unwrap();
    assert_eq!(value["ok"], true, "{value}");
    let destination = fixture.dir.path().join("download.txt");
    let download = serde_json::json!({"id":"download","op":"download","artifact":value["result"]["id"],"path":destination});
    requests
        .write_all(format!("{download}\n").as_bytes())
        .await
        .unwrap();
    let value: serde_json::Value =
        serde_json::from_str(&read_line(&mut responses, 65536).await.unwrap().unwrap()).unwrap();
    assert_eq!(value["ok"], true, "{value}");
    assert_eq!(
        std::fs::read_to_string(&destination).unwrap(),
        "client artifact"
    );
    requests
        .write_all(format!("{download}\n").as_bytes())
        .await
        .unwrap();
    let value: serde_json::Value =
        serde_json::from_str(&read_line(&mut responses, 65536).await.unwrap().unwrap()).unwrap();
    assert_eq!(
        value["ok"], false,
        "existing downloads must not be overwritten"
    );
    requests
        .write_all(b"{\"id\":3,\"op\":\"quit\"}\n")
        .await
        .unwrap();
    task.await.unwrap().unwrap();
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn auth_failures_cap_atomicity_and_malformed_frames() {
    let fixture = Fixture::new();
    let server = fixture.start().await;
    let mut peer = Peer::open(&server).await;
    peer.send("CAP LS 303").await;
    assert!(peer.until("CAP").await.params[2].contains("sasl=PLAIN"));
    peer.send("CAP REQ :--sasl").await;
    assert_eq!(peer.until("CAP").await.params[1], "NAK");
    peer.send("CAP REQ :message-tags unknown-capability").await;
    let m = peer.until("CAP").await;
    assert_eq!(m.params[1], "NAK");
    peer.send("CAP LIST").await;
    let m = peer.until("CAP").await;
    assert!(!m.params[2].contains("message-tags"));
    peer.send("PASS alice:wrong").await;
    peer.until("904").await;
    peer.send("NICK alice").await;
    peer.send("USER alice 0 * :Alice").await;
    peer.send("CAP END").await;
    peer.until("464").await;
    peer.send("JOIN #work").await;
    peer.until("451").await;
    peer.writer.write_all(&vec![b'x'; 5000]).await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(2), read_line(&mut peer.reader, 4608))
            .await
            .unwrap()
            .unwrap()
            .is_none()
    );
    server.shutdown().await.unwrap();
}

#[test]
fn configuration_rejects_unsafe_or_ambiguous_settings() {
    let mut f = Fixture::new();
    f.config.listen = "0.0.0.0:6667".parse().unwrap();
    assert!(f.config.validate().is_err());
    f.config.allow_insecure = true;
    f.config.validate().unwrap();
    f.config.accounts.push(f.config.accounts[0].clone());
    assert!(f.config.validate().is_err());
    let bad = r#"host = "127.0.0.1"
port = 6667
tls = false
account = "alice"
nick = "alice"
token_file = "token"
unknown = true
"#;
    assert!(toml::from_str::<ClientConfig>(bad).is_err());
}

#[tokio::test]
async fn fragmented_input_survives_concurrent_output_and_oversized_relays() {
    let fixture = Fixture::new();
    let server = fixture.start().await;
    let mut alice = Peer::login(&server, "alice", true).await;
    alice.join().await;
    let mut bob = Peer::login(&server, "bob", true).await;
    bob.join().await;
    alice.writer.write_all(b"PRIVMSG #wo").await.unwrap();
    bob.send("PRIVMSG #work :while alice frame is partial")
        .await;
    assert_eq!(
        alice.until("PRIVMSG").await.params[1],
        "while alice frame is partial"
    );
    alice
        .writer
        .write_all(b"rk :completed frame\r\n")
        .await
        .unwrap();
    assert_eq!(alice.until("PRIVMSG").await.params[1], "completed frame");
    alice
        .send(&format!("PRIVMSG #work :{}", "a".repeat(490)))
        .await;
    alice.until("417").await;
    alice.send("").await;
    alice.send("PING :still-alive").await;
    assert_eq!(
        alice.until("PONG").await.params.last().unwrap(),
        "still-alive"
    );
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn topic_and_moderation_persist_but_dynamic_voice_does_not() {
    let fixture = Fixture::new();
    let server = fixture.start().await;
    let mut human = Peer::login(&server, "human", false).await;
    human.join().await;
    let mut alice = Peer::login(&server, "alice", true).await;
    alice.join().await;
    human.send("MODE #work +mv alice").await;
    human.until("MODE").await;
    human.until("MODE").await;
    human.send("TOPIC #work :Shared objective").await;
    human.until("TOPIC").await;
    server.shutdown().await.unwrap();
    drop(human);
    drop(alice);
    let server = fixture.start().await;
    let mut alice = Peer::login(&server, "alice", true).await;
    alice.send("JOIN #work").await;
    assert_eq!(alice.until("332").await.params[2], "Shared objective");
    alice.send("MODE #work").await;
    assert_eq!(alice.until("324").await.params[2], "+mnt");
    alice.send("PRIVMSG #work :no voice after reconnect").await;
    alice.until("404").await;
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn optional_direct_messages_are_live_only() {
    let mut fixture = Fixture::new();
    fixture.config.allow_private_messages = true;
    let server = fixture.start().await;
    let mut alice = Peer::login(&server, "alice", true).await;
    let mut bob = Peer::login(&server, "bob", true).await;
    alice.send("PRIVMSG bob :permitted direct message").await;
    assert_eq!(
        bob.until("PRIVMSG").await.params[1],
        "permitted direct message"
    );
    bob.send("CHATHISTORY LATEST alice * 10").await;
    assert_eq!(bob.until("FAIL").await.params[1], "INVALID_TARGET");
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn tls_requires_trust_and_checks_hostname() {
    let mut fixture = Fixture::new();
    let certificate = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let cert_path = fixture.dir.path().join("certificate.pem");
    let key_path = fixture.dir.path().join("private-key.pem");
    write_private(&cert_path, certificate.cert.pem().as_bytes()).unwrap();
    write_private(
        &key_path,
        certificate.signing_key.serialize_pem().as_bytes(),
    )
    .unwrap();
    fixture.config.tls = Some(airc::config::Tls {
        certificate: cert_path.clone(),
        private_key: key_path,
    });
    let server = fixture.start().await;
    let mut config = fixture.client_config(&server, "alice");
    config.tls = true;
    config.host = "localhost".into();
    assert!(
        client::connect(&config).await.is_err(),
        "untrusted certificate was accepted"
    );
    config.ca_certificate = Some(cert_path);
    let connection = client::connect(&config).await.unwrap();
    drop(connection);
    config.host = "127.0.0.1".into();
    assert!(
        client::connect(&config).await.is_err(),
        "hostname mismatch was accepted"
    );
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn client_quit_exits_with_stdin_still_open() {
    use std::process::Stdio;
    let fixture = Fixture::new();
    let server = fixture.start().await;
    let config = fixture.client_config(&server, "alice");
    let path = fixture.dir.path().join("client.toml");
    write_private(&path, toml::to_string(&config).unwrap().as_bytes()).unwrap();
    let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_airc"))
        .args(["--config", path.to_str().unwrap()])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut output = BufReader::new(child.stdout.take().unwrap());
    assert!(
        read_line(&mut output, 32768)
            .await
            .unwrap()
            .unwrap()
            .contains("ready")
    );
    let mut input = child.stdin.take().unwrap();
    input
        .write_all(b"{\"id\":1,\"op\":\"quit\"}\n")
        .await
        .unwrap();
    assert!(
        read_line(&mut output, 32768)
            .await
            .unwrap()
            .unwrap()
            .contains("true")
    );
    assert!(
        tokio::time::timeout(Duration::from_secs(3), child.wait())
            .await
            .unwrap()
            .unwrap()
            .success()
    );
    drop(input);
    server.shutdown().await.unwrap();
}
