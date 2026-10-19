use airc::{
    client::{self, ClientConfig},
    config::{Account, Artifacts, Channel, Config, Limits, token_hash, write_private},
    protocol::{Message, read_line},
    server::{self, RunningServer},
};
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufReader},
    net::{
        TcpStream,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
};

const TOKEN: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

// Only forwards to this test's two listeners. No Internet access or credentials
// in diagnostic output. Drop cancels the listener and every tunnel it owns.
struct Proxy(tokio::task::JoinHandle<()>);
impl Drop for Proxy {
    fn drop(&mut self) {
        self.0.abort();
    }
}
async fn proxy(server: &RunningServer) -> (String, Proxy) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let irc = server.address.unwrap();
    let http = server.artifact_address.unwrap();
    let task = tokio::spawn(async move {
        let mut tunnels = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                accepted = listener.accept() => {
                    let (mut incoming, _) = accepted.unwrap();
                    tunnels.spawn(async move {
                        let mut header = Vec::new();
                        while !header.ends_with(b"\r\n\r\n") {
                            header.push(incoming.read_u8().await.unwrap());
                            assert!(header.len() < 8192);
                        }
                        let text = std::str::from_utf8(&header).unwrap();
                        let mut lines = text.splitn(2, "\r\n");
                        let request: Vec<_> = lines.next().unwrap().split_whitespace().collect();
                        let is_connect = request[0] == "CONNECT";
                        let url = reqwest::Url::parse(&if is_connect {
                            format!("http://{}", request[1])
                        } else {
                            request[1].to_owned()
                        }).unwrap();
                        let destination = if is_connect { irc } else { http };
                        assert_eq!(url.port(), Some(destination.port()));
                        let mut outgoing = TcpStream::connect(destination).await.unwrap();
                        if is_connect {
                            incoming.write_all(b"HTTP/1.1 200 Connected\r\n\r\n").await.unwrap();
                        } else {
                            let path = url.path();
                            let rewritten = format!("{} {} HTTP/1.1\r\n{}", request[0], path, lines.next().unwrap());
                            outgoing.write_all(rewritten.as_bytes()).await.unwrap();
                        }
                        let _ = tokio::io::copy_bidirectional(&mut incoming, &mut outgoing).await;
                    });
                }
                Some(result) = tunnels.join_next(), if !tunnels.is_empty() => { result.unwrap(); }
            }
        }
    });
    (url, Proxy(task))
}

struct Fixture {
    dir: tempfile::TempDir,
    config: Config,
}
impl Fixture {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let config = Config {
            server_name: "test.airc".into(),
            listen: Some("127.0.0.1:0".parse().unwrap()),
            listen_unix: None,
            control_unix: None,
            membership_generation: None,
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
                listen: Some("127.0.0.1:0".parse().unwrap()),
                listen_unix: None,
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
            port: server.address.unwrap().port(),
            tls: false,
            allow_insecure: false,
            ca_certificate: None,
            proxy: None,
            account: name.into(),
            token_file: token,
            nick: name.into(),
            channels: vec!["#work".into()],
            artifact_url: Some(format!("http://{}", server.artifact_address.unwrap())),
            unix: None,
            artifact_unix: None,
        }
    }
}

struct Peer {
    reader: BufReader<OwnedReadHalf>,
    writer: OwnedWriteHalf,
}
impl Peer {
    async fn open(server: &RunningServer) -> Self {
        let (read, writer) = TcpStream::connect(server.address.unwrap())
            .await
            .unwrap()
            .into_split();
        Self {
            reader: BufReader::new(read),
            writer,
        }
    }
    async fn login(server: &RunningServer, name: &str, extensions: bool) -> Self {
        Self::login_caps(
            server,
            name,
            extensions.then_some(
                "message-tags server-time batch echo-message account-tag draft/chathistory",
            ),
        )
        .await
    }
    async fn login_caps(server: &RunningServer, name: &str, caps: Option<&str>) -> Self {
        let mut peer = Self::open(server).await;
        let extensions = caps.is_some();
        if let Some(caps) = caps {
            peer.send("CAP LS 302").await;
            peer.until("CAP").await;
            peer.send(&format!("CAP REQ :{caps}")).await;
            assert_eq!(peer.until("CAP").await.params[1], "ACK");
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
async fn userhost_reports_authenticated_identity_without_channel_operator_star() {
    let fixture = Fixture::new();
    let server = fixture.start().await;
    let mut human = Peer::login(&server, "human", false).await;
    let mut alice = Peer::login(&server, "alice", true).await;
    human.join().await;
    alice.send("NICK a[ice").await;
    alice.until("NICK").await;
    let mut unfinished = Peer::open(&server).await;
    unfinished.send("NICK unfinished").await;
    unfinished.send("USERHOST human").await;
    assert_eq!(unfinished.until("451").await.params[0], "unfinished");
    human.send("USERHOST").await;
    assert_eq!(human.until("461").await.params[1], "USERHOST");
    human.send("USERHOST HUMAN A{ICE missing unfinished").await;
    assert_eq!(
        human.until("302").await.params,
        ["human", "human=+human@airc a[ice=+alice@airc"]
    );
    human.send("USERHOST missing unfinished").await;
    assert_eq!(human.until("302").await.params, ["human", ""]);
    human
        .send("USERHOST human human human human human a[ice")
        .await;
    let reply = human.until("302").await;
    assert_eq!(reply.params[1].split_whitespace().count(), 5);
    assert!(!reply.params[1].contains("alice"));
    server.shutdown().await.unwrap();
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
    frontend_scenario(false).await;
}

#[tokio::test]
async fn proxy_sasl_and_artifact_round_trip() {
    frontend_scenario(true).await;
}

async fn frontend_scenario(proxied: bool) {
    let fixture = Fixture::new();
    let server = fixture.start().await;
    let (proxy_url, _proxy) = proxy(&server).await;
    let mut config = fixture.client_config(&server, "alice");
    config.proxy = proxied.then(|| proxy_url.clone());
    let mut connection = client::connect(&config).await.unwrap();
    loop {
        if connection.inbound.recv().await.unwrap().command == "366" {
            break;
        }
    }
    connection
        .outbound
        .send(vec![Message::new("PRIVMSG", ["#work", "from agent"])])
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
    let mut config = fixture.client_config(&server, "bob");
    config.proxy = proxied.then_some(proxy_url);
    if proxied {
        // No local DNS route to this hostname: both IRC and artifact transport
        // must use the configured proxy. The fixture maps only these ports.
        config.host = "unresolvable.invalid".into();
        config.allow_insecure = true;
        config.artifact_url = Some(format!(
            "http://unresolvable.invalid:{}",
            server.artifact_address.unwrap().port()
        ));
    }
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
    f.config.listen = Some("0.0.0.0:6667".parse().unwrap());
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
    tls_scenario(false).await;
}

#[tokio::test]
async fn proxy_tls_requires_trust_and_checks_hostname() {
    tls_scenario(true).await;
}

async fn tls_scenario(proxied: bool) {
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
    let (proxy_url, _proxy) = proxy(&server).await;
    let mut config = fixture.client_config(&server, "alice");
    config.proxy = proxied.then_some(proxy_url);
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

async fn ban_list(peer: &mut Peer, mode: &str) -> Vec<Message> {
    peer.send(&format!("MODE #work {mode}")).await;
    let mut entries = Vec::new();
    loop {
        let message = peer.next().await;
        match message.command.as_str() {
            "367" => entries.push(message),
            "368" => return entries,
            error if error.starts_with(['4', '5', '6']) => {
                panic!("ban query failed: {message:?}");
            }
            _ => {}
        }
        assert!(entries.len() <= 100);
    }
}

#[tokio::test]
async fn ban_lists_are_member_readable_operator_mutable_and_bounded() {
    let fixture = Fixture::new();
    let server = fixture.start().await;
    let mut human = Peer::login(&server, "human", false).await;
    let support = human.until("005").await;
    assert!(support.params.contains(&"CHANMODES=b,,,mnt".into()));
    assert!(support.params.contains(&"MAXLIST=b:100".into()));
    human.join().await;
    let mut alice = Peer::login(&server, "alice", false).await;
    alice.send("MODE #work b").await;
    alice.until("442").await; // Even an authorized account must join first.
    alice.join().await;
    for mode in ["b", "+b", "-b"] {
        assert!(ban_list(&mut alice, mode).await.is_empty());
    }
    for mode in ["+b", "-b"] {
        alice.send(&format!("MODE #work {mode} *!bob@airc")).await;
        alice.until("482").await;
    }
    human.send("MODE #work +b BoB").await;
    assert_eq!(human.until("MODE").await.params, ["#work", "+b", "bob!*@*"]);
    let entries = ban_list(&mut alice, "b").await;
    assert_eq!(entries.len(), 1);
    assert_eq!(
        &entries[0].params[1..4],
        ["#work", "bob!*@*", "human!human@airc"]
    );
    assert!(entries[0].params[4].parse::<i64>().unwrap() > 0);
    human.send("MODE #work +b BOB!*@*").await; // Duplicate, preserving setter/time.
    let unchanged = ban_list(&mut human, "b").await;
    assert_eq!(unchanged[0].params[1..], entries[0].params[1..]);
    human.send("MODE #work -b BOB").await;
    human.until("MODE").await;
    assert!(ban_list(&mut human, "b").await.is_empty());

    for mask in ["$a:alice".to_owned(), "a".repeat(201), "a!b!c".to_owned()] {
        human.send(&format!("MODE #work +b {mask}")).await;
        human.until("696").await;
    }
    for n in 0..100 {
        human.send(&format!("MODE #work +b absent{n}")).await;
        human.until("MODE").await;
    }
    human.send("MODE #work +b overflow").await;
    human.until("478").await;
    assert_eq!(ban_list(&mut human, "b").await.len(), 100);
    human.send("MODE #work -b absent0").await;
    human.until("MODE").await;
    human.send("MODE #work +b replacement").await;
    human.until("MODE").await;
    assert_eq!(ban_list(&mut human, "b").await.len(), 100);
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn bans_enforce_join_speech_and_upload_without_revoking_account_reads() {
    let fixture = Fixture::new();
    let server = fixture.start().await;
    let mut human = Peer::login(&server, "human", true).await;
    human.join().await;
    let mut alice = Peer::login(&server, "alice", true).await;
    alice.join().await;
    let http = reqwest::Client::new();
    let endpoint = format!(
        "http://{}/v1/channels/%23work/artifacts",
        server.artifact_address.unwrap()
    );
    let upload = || {
        http.post(&endpoint)
            .basic_auth("alice", Some(TOKEN))
            .header("x-airc-filename", "result.txt")
            .body("test result")
            .send()
    };
    human.send("MODE #work +b *!alice@airc").await;
    human.until("MODE").await;
    alice.until("MODE").await;
    alice.send("NICK renamed").await;
    alice.until("NICK").await;
    for line in ["PRIVMSG #work :blocked", "@+typing=active TAGMSG #work"] {
        alice.send(line).await;
        alice.until("404").await;
    }
    alice.send("NOTICE #work :silent denial").await;
    alice.send("PING :notice barrier").await;
    assert_eq!(alice.next().await.command, "PONG");
    assert_eq!(upload().await.unwrap().status(), 403);
    assert!(
        human
            .history("CHATHISTORY LATEST #work * 100")
            .await
            .is_empty()
    );

    // A list ban does not kick existing members. Voice/operator exemptions
    // follow IRC, and the same speaking decision gates HTTP announcements.
    human.send("MODE #work +v renamed").await;
    human.until("MODE").await;
    alice.until("MODE").await;
    alice.send("PRIVMSG #work :voiced").await;
    assert_eq!(alice.until("PRIVMSG").await.params[1], "voiced");
    let response = upload().await.unwrap();
    assert_eq!(response.status(), 201);
    let value: serde_json::Value = response.json().await.unwrap();
    let url = value["url"].as_str().unwrap();
    human.send("MODE #work -v+o renamed renamed").await;
    human.until("MODE").await;
    human.until("MODE").await;
    alice.until("MODE").await;
    alice.until("MODE").await;
    alice.send("PRIVMSG #work :operator").await;
    loop {
        if alice.until("PRIVMSG").await.params[1] == "operator" {
            break;
        }
    }
    human.send("KICK #work renamed :leave").await;
    human.until("KICK").await;
    alice.until("KICK").await;
    alice.send("JOIN #work").await;
    alice.until("474").await;
    assert_eq!(upload().await.unwrap().status(), 403);
    assert_eq!(
        http.get(url)
            .basic_auth("alice", Some(TOKEN))
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    human.send("MODE #work -b *!ALICE@airc").await;
    human.until("MODE").await;
    alice.join().await;
    alice.send("PRIVMSG #work :allowed again").await;
    assert_eq!(alice.until("PRIVMSG").await.params[1], "allowed again");
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn ban_metadata_and_removal_survive_restart() {
    let fixture = Fixture::new();
    let server = fixture.start().await;
    let mut human = Peer::login(&server, "human", false).await;
    human.join().await;
    human.send("MODE #work +b *!alice@airc").await;
    human.until("MODE").await;
    let entries = ban_list(&mut human, "b").await;
    server.shutdown().await.unwrap();
    let server = fixture.start().await;
    let mut human = Peer::login(&server, "human", false).await;
    human.join().await;
    assert_eq!(ban_list(&mut human, "b").await[0].params, entries[0].params);
    let mut alice = Peer::login(&server, "alice", false).await;
    alice.send("JOIN #work").await;
    alice.until("474").await;
    human.send("MODE #work -b *!alice@airc").await;
    human.until("MODE").await;
    server.shutdown().await.unwrap();
    let server = fixture.start().await;
    let mut alice = Peer::login(&server, "alice", false).await;
    alice.join().await;
    assert!(ban_list(&mut alice, "b").await.is_empty());
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn schema_one_migrates_without_losing_state_and_newer_versions_are_refused() {
    let fixture = Fixture::new();
    let database = &fixture.config.database;
    {
        // Original schema, before the ban table existed.
        let db = rusqlite::Connection::open(database).unwrap();
        db.execute_batch(
            "CREATE TABLE messages (seq INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT NOT NULL UNIQUE, channel TEXT NOT NULL, time TEXT NOT NULL, message TEXT NOT NULL);
             CREATE TABLE channels (name TEXT PRIMARY KEY, moderated INTEGER NOT NULL, topic TEXT NOT NULL DEFAULT '');
             CREATE TABLE artifacts (id TEXT PRIMARY KEY, channel TEXT NOT NULL, filename TEXT NOT NULL, data BLOB NOT NULL, time TEXT NOT NULL);
             INSERT INTO channels VALUES ('#work', 1, 'retained topic');
             PRAGMA user_version=1;"
        ).unwrap();
        let time = chrono::Utc::now().to_rfc3339();
        let mut message = Message::new("PRIVMSG", ["#work", "retained message"]);
        message.prefix = Some("alice!alice@airc".into());
        message.tags.insert("msgid".into(), "old-message".into());
        message.tags.insert("time".into(), time.clone());
        db.execute(
            "INSERT INTO messages(id,channel,time,message) VALUES ('old-message','#work',?1,?2)",
            rusqlite::params![time, serde_json::to_string(&message).unwrap()],
        )
        .unwrap();
        db.execute("INSERT INTO artifacts VALUES ('12345678-1234-4234-8234-123456789012','#work','old.txt',?1,?2)", rusqlite::params![b"retained bytes", time]).unwrap();
    }
    let server = fixture.start().await;
    let mut human = Peer::login(&server, "human", true).await;
    human.send("JOIN #work").await;
    assert_eq!(human.until("332").await.params[2], "retained topic");
    human.until("366").await;
    human.send("MODE #work").await;
    assert_eq!(human.until("324").await.params[2], "+mnt");
    let messages = human.history("CHATHISTORY LATEST #work * 100").await;
    assert_eq!(messages[0].params[1], "retained message");
    assert_eq!(messages[0].tags["msgid"], "old-message");
    let url = format!(
        "http://{}/v1/artifacts/12345678-1234-4234-8234-123456789012",
        server.artifact_address.unwrap()
    );
    let artifact = reqwest::Client::new()
        .get(url)
        .basic_auth("alice", Some(TOKEN))
        .send()
        .await
        .unwrap();
    assert_eq!(artifact.status(), 200);
    assert_eq!(artifact.bytes().await.unwrap(), "retained bytes");
    assert!(ban_list(&mut human, "b").await.is_empty());
    server.shutdown().await.unwrap();
    {
        let db = rusqlite::Connection::open(database).unwrap();
        assert_eq!(
            db.query_row("PRAGMA user_version", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            3
        );
        db.execute_batch("PRAGMA user_version=4;").unwrap();
    }
    let error = server::start(fixture.config.clone())
        .await
        .err()
        .expect("newer schema was accepted");
    assert!(error.to_string().contains("schema is newer"), "{error:#}");
    let db = rusqlite::Connection::open(database).unwrap();
    assert_eq!(
        db.query_row("PRAGMA user_version", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        4
    );
    assert_eq!(
        db.query_row("SELECT COUNT(*) FROM messages", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        1
    );
}

const MULTILINE_CAPS: &str =
    "message-tags server-time batch echo-message account-tag draft/chathistory draft/multiline";

/// Collect one draft/multiline batch after its opening BATCH, returning the
/// opener and the combined text with each line's concat flag.
async fn multiline_batch(peer: &mut Peer, open: Message) -> (Message, String, Vec<bool>) {
    assert_eq!(open.params[1], "draft/multiline", "{open:?}");
    let reference = open.params[0].strip_prefix('+').unwrap().to_owned();
    let mut text = String::new();
    let mut concat = Vec::new();
    loop {
        let m = peer.next().await;
        if m.command == "BATCH" {
            assert_eq!(m.params[0], format!("-{reference}"));
            return (open, text, concat);
        }
        assert_eq!(m.tags["batch"], reference);
        assert!(!m.tags.contains_key("msgid"), "lines carry no msgid");
        let joined = m.tags.contains_key("draft/multiline-concat");
        if !concat.is_empty() && !joined {
            text.push('\n');
        }
        text.push_str(&m.params[1]);
        concat.push(joined);
    }
}

async fn send_batch(peer: &mut Peer, reference: &str, lines: &[&str]) {
    peer.send(&format!(
        "@+reply=r1 BATCH +{reference} draft/multiline #work"
    ))
    .await;
    for line in lines {
        peer.send(line).await;
    }
    peer.send(&format!("BATCH -{reference}")).await;
}

#[tokio::test]
async fn multiline_batches_deliver_one_message_with_fallback_history_and_restart() {
    let mut fixture = Fixture::new();
    fixture.config.limits.multiline_lines = 8;
    let server = fixture.start().await;
    let mut alice = Peer::open(&server).await;
    alice.send("CAP LS 302").await;
    let offered = alice.until("CAP").await;
    assert!(
        offered.params[2]
            .split(' ')
            .any(|cap| cap == "draft/multiline=max-bytes=65536,max-lines=8"),
        "{offered:?}"
    );
    drop(alice);
    let mut alice = Peer::login_caps(&server, "alice", Some(MULTILINE_CAPS)).await;
    let mut bob = Peer::login(&server, "bob", true).await;
    alice.join().await;
    bob.join().await;
    send_batch(
        &mut alice,
        "a1",
        &[
            "@batch=a1 PRIVMSG #work :hello",
            "@batch=a1 PRIVMSG #work :",
            "@batch=a1 PRIVMSG #work :how is ",
            "@batch=a1;draft/multiline-concat PRIVMSG #work :everyone? ```code```",
        ],
    )
    .await;
    let expected = "hello\n\nhow is everyone? ```code```";
    let open = alice.until("BATCH").await;
    let (open, text, concat) = multiline_batch(&mut alice, open).await;
    assert_eq!(text, expected);
    assert_eq!(concat, [false, false, false, true]);
    assert_eq!(open.tags["account"], "alice");
    assert_eq!(open.tags["+reply"], "r1");
    assert_eq!(open.prefix.as_deref(), Some("alice!alice@airc"));
    let msgid = open.tags["msgid"].clone();
    // Fallback: component lines without blanks; only the first carries the msgid.
    let first = bob.until("PRIVMSG").await;
    assert_eq!(first.params[1], "hello");
    assert_eq!(first.tags["msgid"], msgid);
    assert_eq!(first.tags["+reply"], "r1");
    for part in ["how is ", "everyone? ```code```"] {
        let next = bob.next().await;
        assert_eq!(next.params[1], part);
        assert_eq!(next.tags["account"], "alice");
        assert!(!next.tags.contains_key("msgid") && !next.tags.contains_key("+reply"));
    }
    // History keeps one logical record: nested for multiline clients, lines otherwise.
    alice.send("CHATHISTORY LATEST #work * 10").await;
    let outer = alice.until("BATCH").await;
    assert_eq!(outer.params[1], "chathistory");
    let parent = outer.params[0].strip_prefix('+').unwrap().to_owned();
    let nested = alice.next().await;
    assert_eq!(nested.tags["batch"], parent);
    let (nested, text, _) = multiline_batch(&mut alice, nested).await;
    assert_eq!(
        (nested.tags["msgid"].as_str(), text.as_str()),
        (msgid.as_str(), expected)
    );
    assert_eq!(alice.next().await.params[0], format!("-{parent}"));
    // Stored history is re-split into relay-sized lines, not the live composition.
    let history = bob.history("CHATHISTORY LATEST #work * 10").await;
    assert_eq!(
        history
            .iter()
            .map(|m| m.params[1].as_str())
            .collect::<Vec<_>>(),
        ["hello", "how is everyone? ```code```"]
    );
    assert_eq!(history[0].tags["msgid"], msgid);
    assert!(!history[1].tags.contains_key("msgid"));
    server.shutdown().await.unwrap();
    drop((alice, bob));
    let server = fixture.start().await;
    let mut alice = Peer::login_caps(&server, "alice", Some(MULTILINE_CAPS)).await;
    alice.join().await;
    alice.send("CHATHISTORY LATEST #work * 10").await;
    alice.until("BATCH").await;
    let nested = alice.next().await;
    let (_, text, _) = multiline_batch(&mut alice, nested).await;
    assert_eq!(text, expected, "restart keeps the logical message");
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn invalid_interrupted_or_unauthorized_batches_are_never_delivered() {
    let mut fixture = Fixture::new();
    fixture.config.limits.multiline_lines = 3;
    fixture.config.limits.multiline_bytes = 1024;
    let server = fixture.start().await;
    let mut alice = Peer::login_caps(&server, "alice", Some(MULTILINE_CAPS)).await;
    let mut bob = Peer::login(&server, "bob", true).await;
    let mut human = Peer::login(&server, "human", true).await;
    for peer in [&mut alice, &mut bob, &mut human] {
        peer.join().await;
    }
    let long = "x".repeat(400);
    let cases: Vec<(Vec<String>, &str)> = vec![
        (
            vec!["@batch=b PRIVMSG #other :x".into()],
            "MULTILINE_INVALID_TARGET #work #other",
        ),
        (
            vec![
                "@batch=b PRIVMSG #work :x".into(),
                "@batch=b NOTICE #work :y".into(),
            ],
            "MULTILINE_INVALID",
        ),
        (
            vec![
                "@batch=b PRIVMSG #work :x".into(),
                "@batch=b;draft/multiline-concat PRIVMSG #work :".into(),
            ],
            "MULTILINE_INVALID",
        ),
        (
            vec![
                "@batch=b PRIVMSG #work :".into(),
                "@batch=b PRIVMSG #work :".into(),
            ],
            "MULTILINE_INVALID",
        ),
        (
            vec!["@batch=b;+reply=x PRIVMSG #work :x".into()],
            "MULTILINE_INVALID",
        ),
        (
            (0..4)
                .map(|n| format!("@batch=b PRIVMSG #work :{n}"))
                .collect(),
            "MULTILINE_MAX_LINES 3",
        ),
        (
            (0..3)
                .map(|_| format!("@batch=b PRIVMSG #work :{long}"))
                .collect(),
            "MULTILINE_MAX_BYTES 1024",
        ),
        (
            vec!["@batch=b PRIVMSG #work :x".into(), "JOIN #work".into()],
            "MULTILINE_INVALID",
        ),
        (
            vec![
                "@batch=b PRIVMSG #work :x".into(),
                "BATCH +c draft/multiline #work".into(),
                "@batch=c PRIVMSG #work :y".into(),
                "BATCH -c".into(),
            ],
            "MULTILINE_INVALID",
        ),
    ];
    for (n, (lines, expected)) in cases.iter().enumerate() {
        alice.send("BATCH +b draft/multiline #work").await;
        for line in lines {
            alice.send(line).await;
        }
        alice.send("BATCH -b").await;
        alice.send(&format!("PING :case{n}")).await;
        let fail = alice.until("FAIL").await;
        assert_eq!(
            fail.params[1..fail.params.len() - 1].join(" "),
            *expected,
            "case {n}: {fail:?}"
        );
        let pong = alice.until("PONG").await;
        assert_eq!(
            pong.params[1],
            format!("case{n}"),
            "one FAIL per invalid batch"
        );
    }
    for (line, expected) in [
        ("@batch=zz PRIVMSG #work :orphan", "MULTILINE_INVALID"),
        ("BATCH +t vendor/other #work", "UNKNOWN_TYPE t vendor/other"),
        ("BATCH +bad_ref draft/multiline #work", "INVALID_REFTAG *"),
        (
            "@batch=p BATCH +n draft/multiline #work",
            "MULTILINE_INVALID",
        ),
    ] {
        alice.send(line).await;
        let fail = alice.until("FAIL").await;
        assert_eq!(fail.params[1..fail.params.len() - 1].join(" "), expected);
    }
    // Opener metadata over the relay budget is a batch error, even for NOTICE.
    let tags = "a".repeat(1100);
    for command in ["PRIVMSG", "NOTICE"] {
        alice
            .send(&format!("@+x={tags} BATCH +big draft/multiline #work"))
            .await;
        alice
            .send(&format!("@batch=big {command} #work :first"))
            .await;
        alice
            .send(&format!("@batch=big {command} #work :second"))
            .await;
        alice.send("BATCH -big").await;
        let fail = alice.until("FAIL").await;
        assert_eq!(
            fail.params[1..].join(" "),
            "MULTILINE_INVALID Tags too long",
            "{command}"
        );
    }
    // Lines of a rejected batch, including a non-negotiated sender's, stay silent.
    bob.send("BATCH +q draft/multiline #work").await;
    assert_eq!(bob.until("FAIL").await.params[1], "MULTILINE_INVALID");
    bob.send("@batch=q PRIVMSG #work :not delivered").await;
    bob.send("BATCH -q").await;
    bob.send("PING :quiet").await;
    assert_eq!(bob.until("PONG").await.params[1], "quiet");
    // Permission is checked when the batch closes; PING may interleave.
    alice.send("BATCH +m draft/multiline #work").await;
    alice.send("@batch=m PRIVMSG #work :line one").await;
    human.send("MODE #work +m").await;
    human.until("MODE").await;
    alice.until("MODE").await;
    alice.send("PING :kept").await;
    assert_eq!(alice.until("PONG").await.params[1], "kept");
    alice.send("@batch=m PRIVMSG #work :line two").await;
    alice.send("BATCH -m").await;
    let fail = alice.until("FAIL").await;
    assert_eq!(
        fail.params[1..].join(" "),
        "MULTILINE_INVALID Cannot send to channel",
        "close-time denial keeps batch context"
    );
    human.send("MODE #work -m").await;
    alice.until("MODE").await;
    send_batch(
        &mut alice,
        "ok",
        &[
            "@batch=ok PRIVMSG #work :first",
            "@batch=ok PRIVMSG #work :second",
        ],
    )
    .await;
    let open = alice.until("BATCH").await;
    assert_eq!(multiline_batch(&mut alice, open).await.1, "first\nsecond");
    // The observer saw only the mode changes and the one valid message.
    let mut seen = Vec::new();
    while seen.len() < 2 {
        let m = bob.next().await;
        if m.command == "PRIVMSG" {
            seen.push(m.params[1].clone());
        }
    }
    assert_eq!(seen, ["first", "second"]);
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn history_pages_are_byte_bounded_and_page_on_without_loss() {
    let mut fixture = Fixture::new();
    fixture.config.limits.multiline_lines = 200;
    fixture.config.limits.history_page_bytes = 64 * 1024;
    let server = fixture.start().await;
    let mut alice = Peer::login_caps(&server, "alice", Some(MULTILINE_CAPS)).await;
    alice.join().await;
    let line = "y".repeat(300);
    let mut ids = Vec::new();
    for reference in ["l1", "l2", "l3"] {
        alice
            .send(&format!("BATCH +{reference} draft/multiline #work"))
            .await;
        for _ in 0..140 {
            alice
                .send(&format!("@batch={reference} PRIVMSG #work :{line}"))
                .await;
        }
        alice.send(&format!("BATCH -{reference}")).await;
        let open = alice.until("BATCH").await;
        let (open, text, _) = multiline_batch(&mut alice, open).await;
        assert_eq!(text.len(), 140 * 300 + 139);
        ids.push(open.tags["msgid"].clone());
    }
    // One 42 KB message fits a 64 KiB page: each page keeps the row nearest its
    // reference, including the AROUND anchor, so paging on never skips one.
    for (request, expected) in [
        ("CHATHISTORY LATEST #work * 10".to_string(), &ids[2]),
        (
            format!("CHATHISTORY BEFORE #work msgid={} 10", ids[2]),
            &ids[1],
        ),
        (
            format!("CHATHISTORY AFTER #work msgid={} 10", ids[0]),
            &ids[1],
        ),
        (
            format!("CHATHISTORY AROUND #work msgid={} 3", ids[1]),
            &ids[1],
        ),
        (
            format!(
                "CHATHISTORY BETWEEN #work msgid={} msgid={} 10",
                ids[0], ids[2]
            ),
            &ids[1],
        ),
        (
            format!(
                "CHATHISTORY BETWEEN #work msgid={} msgid={} 10",
                ids[2], ids[0]
            ),
            &ids[1],
        ),
    ] {
        alice.send(&request).await;
        alice.until("BATCH").await;
        let mut found = Vec::new();
        loop {
            let m = alice.next().await;
            if m.command == "BATCH" && m.params[0].starts_with('-') {
                break;
            }
            if m.command == "BATCH" && m.params[0].starts_with('+') {
                found.push(m.tags["msgid"].clone());
                multiline_batch(&mut alice, m).await;
            }
        }
        assert_eq!(
            found.as_slice(),
            std::slice::from_ref(expected),
            "{request}"
        );
    }
    server.shutdown().await.unwrap();
}

impl Peer {
    // An ordered protocol barrier, without timing-based absence assertions.
    async fn through_pong(&mut self, label: &str) -> Vec<Message> {
        self.send(&format!("PING :{label}")).await;
        let mut messages = Vec::new();
        loop {
            let message = self.next().await;
            if message.command == "PONG" && message.params.last().is_some_and(|text| text == label)
            {
                return messages;
            }
            messages.push(message);
        }
    }
}

#[tokio::test]
async fn explicit_away_notifies_shared_capable_peers_and_supplies_roster_facts() {
    let mut f = Fixture::new();
    f.config.allow_private_messages = true;
    f.config.channels.push(Channel {
        name: "#second".into(),
        members: vec!["human".into(), "alice".into()],
        operators: vec!["human".into()],
        moderated: false,
    });
    let server = f.start().await;
    let mut human = Peer::login_caps(&server, "human", Some("away-notify")).await;
    let mut alice = Peer::login_caps(&server, "alice", Some("away-notify")).await;
    let mut bob = Peer::login(&server, "bob", false).await;
    let mut outsider = Peer::login_caps(&server, "outsider", Some("away-notify")).await;
    human.join().await;
    bob.join().await;
    alice.send("AWAY :explicit before joining").await;
    alice.until("306").await;
    assert!(
        !human
            .through_pong("before-join")
            .await
            .iter()
            .any(|m| m.command == "AWAY")
    );
    alice.join().await;
    let joined = human.through_pong("joined").await;
    let away = joined.iter().find(|m| m.command == "AWAY").unwrap();
    assert_eq!(away.params, ["explicit before joining"]);
    assert_eq!(away.prefix.as_deref(), Some("alice!alice@airc"));
    assert!(
        !alice
            .through_pong("not-self")
            .await
            .iter()
            .any(|m| m.command == "AWAY")
    );
    assert!(
        !bob.through_pong("legacy")
            .await
            .iter()
            .any(|m| m.command == "AWAY")
    );
    assert!(
        !outsider
            .through_pong("outsider")
            .await
            .iter()
            .any(|m| m.command == "AWAY")
    );
    human.send("JOIN #second").await;
    human.until("366").await;
    alice.send("JOIN #second").await;
    alice.until("366").await;
    human.through_pong("drain-second").await;

    // Bound/truncate a UTF-8 reason without splitting a scalar; shared-channel
    // overlap delivers one change, not one per common channel.
    alice.send(&format!("AWAY :a{}", "é".repeat(100))).await;
    alice.until("306").await;
    let updates = human.through_pong("changed").await;
    let updates: Vec<_> = updates.iter().filter(|m| m.command == "AWAY").collect();
    assert_eq!(updates.len(), 1);
    let reason = format!("a{}", "é".repeat(99));
    assert_eq!(updates[0].params.as_slice(), std::slice::from_ref(&reason));
    alice.send(&format!("AWAY :{reason}")).await;
    alice.until("306").await;
    assert!(
        !human
            .through_pong("unchanged")
            .await
            .iter()
            .any(|m| m.command == "AWAY")
    );
    human.send("MODE #work +v alice").await;
    human.until("MODE").await;
    human.send("WHO #work").await;
    let roster = human.through_pong("who").await;
    assert!(
        roster
            .iter()
            .any(|m| m.command == "352" && m.params[5] == "alice" && m.params[6] == "G+")
    );
    assert!(
        roster
            .iter()
            .any(|m| m.command == "352" && m.params[5] == "human" && m.params[6] == "H@")
    );
    outsider.send("WHO #work").await;
    assert!(
        !outsider
            .through_pong("hidden")
            .await
            .iter()
            .any(|m| m.command == "352")
    );
    human.send("WHOIS ALICE").await;
    assert_eq!(human.until("301").await.params, ["human", "alice", &reason]);
    human.until("318").await;
    human.send("USERHOST ALICE").await;
    assert_eq!(human.until("302").await.params[1], "alice=-alice@airc");
    human.send("PRIVMSG alice :still delivers while away").await;
    human.until("301").await;
    assert_eq!(
        alice.until("PRIVMSG").await.params[1],
        "still delivers while away"
    );
    human.send("NOTICE alice :quiet notice").await;
    assert_eq!(alice.until("NOTICE").await.params[1], "quiet notice");
    assert!(
        !human
            .through_pong("notice-silent")
            .await
            .iter()
            .any(|m| m.command == "301")
    );

    alice.send("AWAY").await;
    alice.until("305").await;
    assert!(human.until("AWAY").await.params.is_empty());
    human.send("WHO #work").await;
    assert!(
        human
            .through_pong("back-who")
            .await
            .iter()
            .any(|m| m.command == "352" && m.params[5] == "alice" && m.params[6] == "H+")
    );
    human.send("USERHOST alice").await;
    assert_eq!(human.until("302").await.params[1], "alice=+alice@airc");
    alice.send("AWAY :").await;
    alice.until("305").await;
    assert!(
        !human
            .through_pong("already-back")
            .await
            .iter()
            .any(|m| m.command == "AWAY")
    );
    // Reconnects are new sessions, never restoration of an account's claim.
    alice.send("AWAY :before disconnect").await;
    alice.until("306").await;
    alice.send("QUIT :done").await;
    human.until("QUIT").await;
    let mut alice = Peer::login(&server, "alice", false).await;
    alice.join().await;
    human.send("WHOIS alice").await;
    assert!(
        !human
            .through_pong("new-session")
            .await
            .iter()
            .any(|m| m.command == "301")
    );
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn away_keeps_registration_private_message_policy_and_notice_silence() {
    let f = Fixture::new();
    let server = f.start().await;
    let mut unauthenticated = Peer::open(&server).await;
    unauthenticated.send("AWAY :not registered").await;
    unauthenticated.until("451").await;
    let mut alice = Peer::login(&server, "alice", false).await;
    alice.send("AWAY :explicit claim").await;
    alice.until("306").await;
    let mut human = Peer::login(&server, "human", false).await;
    human.send("PRIVMSG alice :denied").await;
    let denied = human.through_pong("private-denied").await;
    assert!(denied.iter().any(|m| m.command == "404"));
    assert!(!denied.iter().any(|m| m.command == "301"));
    human.send("NOTICE alice :also denied").await;
    assert!(
        !human
            .through_pong("denied")
            .await
            .iter()
            .any(|m| m.command == "301" || m.command == "404")
    );
    server.shutdown().await.unwrap();
}
