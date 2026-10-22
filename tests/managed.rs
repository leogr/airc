//! Managed-deployment transports: Unix-only listeners, client endpoints and probes.
#![cfg(unix)]

use airc::{
    client::{self, ClientConfig},
    config::{Account, Artifacts, Channel, Config, Limits, token_hash, write_private},
    protocol::{Message, read_line},
    server,
};
use std::{
    os::unix::fs::{FileTypeExt, PermissionsExt},
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::io::{AsyncWriteExt, BufReader};

const TOKEN: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

fn unix_config(dir: &Path) -> Config {
    let endpoint = dir.join("endpoint");
    std::fs::create_dir_all(&endpoint).unwrap();
    Config {
        server_name: "test.airc".into(),
        listen: None,
        listen_unix: Some(endpoint.join("irc.sock")),
        control_unix: None,
        membership_generation: None,
        database: dir.join("state.db"),
        allow_insecure: false,
        allow_private_messages: false,
        tls: None,
        limits: Limits::default(),
        accounts: ["alice", "bob"]
            .map(|name| Account {
                name: name.into(),
                token_sha256: token_hash(TOKEN),
            })
            .to_vec(),
        channels: vec![Channel {
            name: "#space".into(),
            members: vec!["alice".into(), "bob".into()],
            operators: vec![],
            moderated: false,
        }],
        artifacts: Some(Artifacts {
            listen: None,
            listen_unix: Some(endpoint.join("artifacts.sock")),
            public_url: "http://airc.local".into(),
            max_bytes: 1024,
            per_channel: 4,
        }),
    }
}

fn member(dir: &Path, config: &Config, name: &str) -> ClientConfig {
    let token = dir.join(format!("{name}.token"));
    if !token.exists() {
        write_private(&token, TOKEN.as_bytes()).unwrap();
    }
    ClientConfig {
        host: String::new(),
        port: 0,
        tls: false,
        allow_insecure: false,
        ca_certificate: None,
        proxy: None,
        account: name.into(),
        token_file: token,
        nick: name.into(),
        channels: vec!["#space".into()],
        artifact_url: None,
        unix: config.listen_unix.clone(),
        artifact_unix: config.artifacts.as_ref().unwrap().listen_unix.clone(),
    }
}

fn mode(path: &Path) -> u32 {
    std::fs::symlink_metadata(path)
        .unwrap()
        .permissions()
        .mode()
        & 0o777
}

#[tokio::test]
async fn unix_mode_serves_irc_and_artifacts_without_tcp() {
    let dir = tempfile::tempdir().unwrap();
    let config = unix_config(dir.path());
    let irc = config.listen_unix.clone().unwrap();
    let http = config
        .artifacts
        .as_ref()
        .unwrap()
        .listen_unix
        .clone()
        .unwrap();
    let server = server::start(config.clone()).await.unwrap();
    assert!(server.address.is_none() && server.artifact_address.is_none());
    assert_eq!(server.unix.as_deref(), Some(irc.as_path()));
    for socket in [&irc, &http] {
        assert!(
            std::fs::symlink_metadata(socket)
                .unwrap()
                .file_type()
                .is_socket()
        );
        assert_eq!(mode(socket), 0o600, "{}", socket.display());
    }
    let mut bob = client::connect(&member(dir.path(), &config, "bob"))
        .await
        .unwrap();
    // Artifacts travel over the Unix endpoint through the JSONL frontend.
    let source = dir.path().join("note.txt");
    std::fs::write(&source, "unix artifact").unwrap();
    let (mut requests, input) = tokio::io::duplex(65536);
    let (output, responses) = tokio::io::duplex(65536);
    let task = tokio::spawn(client::frontend(
        member(dir.path(), &config, "alice"),
        BufReader::new(input),
        output,
    ));
    let mut responses = BufReader::new(responses);
    let mut reply = async || -> serde_json::Value {
        serde_json::from_str(&read_line(&mut responses, 65536).await.unwrap().unwrap()).unwrap()
    };
    assert_eq!(reply().await["event"], "ready");
    let upload = serde_json::json!({"id":1,"op":"upload","channel":"#space","path":source});
    requests
        .write_all(format!("{upload}\n").as_bytes())
        .await
        .unwrap();
    let uploaded = reply().await;
    assert_eq!(uploaded["ok"], true, "{uploaded}");
    let url = uploaded["result"]["url"].as_str().unwrap().to_owned();
    assert!(url.starts_with("http://airc.local/v1/artifacts/"), "{url}");
    let destination = dir.path().join("copy.txt");
    let download = serde_json::json!({"id":2,"op":"download","artifact":uploaded["result"]["id"],"path":destination});
    requests
        .write_all(format!("{download}\n").as_bytes())
        .await
        .unwrap();
    assert_eq!(reply().await["ok"], true);
    assert_eq!(
        std::fs::read_to_string(&destination).unwrap(),
        "unix artifact"
    );
    let announced = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let m = bob.inbound.recv().await.unwrap();
            if m.command == "PRIVMSG" {
                break m;
            }
        }
    })
    .await
    .unwrap();
    assert!(announced.params[1].contains(&url), "{announced:?}");
    assert_eq!(announced.tags["account"], "alice");
    // The standalone CLI uses the same Unix-only authenticated HTTP path,
    // while alice's original IRC connection remains active.
    let client_path = dir.path().join("alice.toml");
    std::fs::write(
        &client_path,
        toml::to_string(&member(dir.path(), &config, "alice")).unwrap(),
    )
    .unwrap();
    let cli_destination = dir.path().join("cli-copy.txt");
    let output = tokio::time::timeout(
        Duration::from_secs(10),
        tokio::process::Command::new(env!("CARGO_BIN_EXE_airc"))
            .args(["artifact", "download", "--config"])
            .arg(&client_path)
            .arg(uploaded["result"]["id"].as_str().unwrap())
            .arg(&cli_destination)
            .kill_on_drop(true)
            .output(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        std::fs::read_to_string(&cli_destination).unwrap(),
        "unix artifact"
    );
    requests
        .write_all(b"{\"id\":3,\"op\":\"quit\"}\n")
        .await
        .unwrap();
    task.await.unwrap().unwrap();
    bob.outbound
        .send(vec![Message::new("QUIT", ["bye"])])
        .await
        .unwrap();
    server.shutdown().await.unwrap();
    for socket in [&irc, &http] {
        assert!(
            !socket.exists(),
            "owned socket removed: {}",
            socket.display()
        );
    }
}

#[tokio::test]
async fn unix_listeners_replace_only_stale_sockets() {
    let dir = tempfile::tempdir().unwrap();
    let config = unix_config(dir.path());
    let irc = config.listen_unix.clone().unwrap();
    // A socket whose listener exited is stale and replaced.
    drop(std::os::unix::net::UnixListener::bind(&irc).unwrap());
    let server = server::start(config.clone()).await.unwrap();
    let error = server::start(config.clone())
        .await
        .err()
        .expect("second server");
    // The stable ownership lock refuses a second server before any probe.
    assert!(
        error.to_string().contains("another server owns"),
        "{error:#}"
    );
    assert!(irc.exists(), "live socket kept");
    server.shutdown().await.unwrap();
    // A regular file or symlink is never removed.
    std::fs::write(&irc, "not a socket").unwrap();
    let error = server::start(config.clone())
        .await
        .err()
        .expect("file refused");
    assert!(error.to_string().contains("not a socket"), "{error:#}");
    assert_eq!(std::fs::read_to_string(&irc).unwrap(), "not a socket");
    std::fs::remove_file(&irc).unwrap();
    std::os::unix::fs::symlink(dir.path().join("elsewhere"), &irc).unwrap();
    assert!(server::start(config).await.is_err());
    assert!(
        std::fs::symlink_metadata(&irc)
            .unwrap()
            .file_type()
            .is_symlink()
    );
}

#[test]
fn unix_mode_configuration_is_exclusive_and_keeps_control_private() {
    let dir = tempfile::tempdir().unwrap();
    let base = unix_config(dir.path());
    let invalid = |change: &dyn Fn(&mut Config)| {
        let mut config = base.clone();
        change(&mut config);
        config.validate().is_err()
    };
    assert!(base.validate().is_ok());
    assert!(invalid(&|c| c.listen = Some("127.0.0.1:0".parse().unwrap())));
    assert!(invalid(&|c| c.listen_unix = None));
    assert!(invalid(&|c| c.allow_insecure = true));
    assert!(invalid(&|c| {
        let a = c.artifacts.as_mut().unwrap();
        a.listen_unix = None;
        a.listen = Some("127.0.0.1:0".parse().unwrap());
    }));
    assert!(invalid(
        &|c| c.listen_unix = Some(PathBuf::from("relative.sock"))
    ));
    assert!(invalid(
        &|c| c.listen_unix = Some(PathBuf::from(format!("/{}", "x".repeat(120))))
    ));
    let control = dir.path().join("control/control.sock");
    // Control requires a generation and never shares/nests with a listener directory.
    assert!(invalid(&|c| c.control_unix = Some(control.clone())));
    let with_control = |path: PathBuf| {
        let mut config = base.clone();
        config.control_unix = Some(path);
        config.membership_generation = Some(1);
        config.validate()
    };
    assert!(with_control(control.clone()).is_ok());
    assert!(with_control(dir.path().join("endpoint/control.sock")).is_err());
    assert!(with_control(dir.path().join("endpoint/sub/control.sock")).is_err());
    assert!(
        with_control(dir.path().join("control.sock")).is_err(),
        "parent contains endpoint"
    );
    // A TCP-mode server cannot expose Unix artifacts (no mixed modes).
    let mut tcp = base.clone();
    tcp.listen_unix = None;
    tcp.listen = Some("127.0.0.1:0".parse().unwrap());
    assert!(tcp.validate().is_err());
    let client = member(dir.path(), &base, "alice");
    assert!(client.validate().is_ok());
    for change in [
        &(|c: &mut ClientConfig| c.host = "127.0.0.1".into()) as &dyn Fn(&mut ClientConfig),
        &|c| c.port = 6667,
        &|c| c.proxy = Some("http://127.0.0.1:3128".into()),
        &|c| c.allow_insecure = true,
        &|c| c.artifact_url = Some("http://127.0.0.1:8080".into()),
        &|c| c.unix = None,
    ] {
        let mut config = client.clone();
        change(&mut config);
        assert!(config.validate().is_err());
    }
}

#[test]
fn both_binaries_print_the_same_feature_probe() {
    let mut probes = Vec::new();
    for binary in [env!("CARGO_BIN_EXE_aircd"), env!("CARGO_BIN_EXE_airc")] {
        let output = std::process::Command::new(binary)
            .arg("features")
            .output()
            .unwrap();
        assert!(output.status.success());
        let probe: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(probe["protocol"], 1);
        probes.push(probe);
    }
    assert_eq!(probes[0], probes[1]);
    let features = probes[0]["features"].as_array().unwrap();
    for feature in [
        "client-presence",
        "server-presence",
        "unix-listeners",
        "unix-artifacts",
        "client-unix",
        "client-artifact-unix",
        "channel-add-reload",
        "client-topic",
        "named-channels",
        "named-delivery",
        "named-show",
    ] {
        assert!(features.iter().any(|f| f == feature), "{feature}");
    }
}

async fn control(socket: &Path, request: &str) -> serde_json::Value {
    let stream = tokio::net::UnixStream::connect(socket).await.unwrap();
    let mut stream = BufReader::new(stream);
    stream
        .get_mut()
        .write_all(format!("{request}\n").as_bytes())
        .await
        .unwrap();
    let line = tokio::time::timeout(Duration::from_secs(5), read_line(&mut stream, 65536))
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    serde_json::from_str(&line).unwrap()
}

fn sha256(bytes: &[u8]) -> String {
    airc::config::token_hash_bytes(bytes)
}

/// Write the managed server file; returns the exact bytes for digest checks.
fn render(path: &Path, config: &Config) -> Vec<u8> {
    let bytes = toml::to_string(config).unwrap().into_bytes();
    std::fs::write(path, &bytes).unwrap();
    bytes
}

async fn closed_with(connection: &mut client::Connection, reason: &str) {
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            match connection.inbound.recv().await {
                Some(m) if m.command == "ERROR" => {
                    assert_eq!(m.params[0], reason);
                }
                Some(_) => {}
                None => break,
            }
        }
    })
    .await
    .expect("revoked session closes within one second");
}

#[tokio::test]
async fn control_reload_applies_generations_and_revokes_live_identities() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = unix_config(dir.path());
    let control_dir = dir.path().join("control");
    std::fs::create_dir(&control_dir).unwrap();
    let socket = control_dir.join("control.sock");
    config.control_unix = Some(socket.clone());
    config.membership_generation = Some(1);
    let file = dir.path().join("server.toml");
    let bytes = render(&file, &config);
    let server = server::start_file(&file).await.unwrap();
    assert_eq!(mode(&socket), 0o600);
    let status = control(&socket, r#"{"op":"status"}"#).await;
    assert_eq!(status["ok"], true, "{status}");
    let result = &status["result"];
    assert_eq!(result["protocol"], 1);
    assert!(
        result["features"]
            .as_array()
            .unwrap()
            .iter()
            .any(|feature| feature == "server-presence")
    );
    assert_eq!(result["applied_generation"], 1);
    assert_eq!(result["config_sha256"], sha256(&bytes));
    assert_eq!(result["listeners"]["irc"], "unix");
    let incarnation = result["incarnation"].as_str().unwrap().to_owned();
    assert!(
        result["features"]
            .as_array()
            .unwrap()
            .iter()
            .any(|f| f == "acl-revoke")
    );
    let mut alice = client::connect(&member(dir.path(), &config, "alice"))
        .await
        .unwrap();
    let mut bob = client::connect(&member(dir.path(), &config, "bob"))
        .await
        .unwrap();
    let status = control(&socket, r#"{"op":"status"}"#).await;
    assert_eq!(status["result"]["accounts"]["bob"]["connections"], 1);
    assert_eq!(status["result"]["configured_accounts"], 2);
    // Same bytes at the same generation are idempotent; different bytes conflict.
    let same = control(&socket, r#"{"op":"reload"}"#).await;
    assert_eq!(
        same["result"]["disconnected"],
        serde_json::json!([]),
        "{same}"
    );
    let mut changed = config.clone();
    changed.channels[0].members.retain(|m| m != "bob");
    render(&file, &changed);
    let conflict = control(&socket, r#"{"op":"reload"}"#).await;
    assert_eq!(conflict["code"], "GENERATION_CONFLICT", "{conflict}");
    changed.membership_generation = Some(0);
    render(&file, &changed);
    assert_eq!(
        control(&socket, r#"{"op":"reload"}"#).await["code"],
        "GENERATION_STALE"
    );
    let mut restart = config.clone();
    restart.membership_generation = Some(9);
    restart.limits.history_page = 50;
    render(&file, &restart);
    assert_eq!(
        control(&socket, r#"{"op":"reload"}"#).await["code"],
        "RESTART_REQUIRED"
    );
    let status = control(&socket, r#"{"op":"status"}"#).await;
    assert_eq!(
        status["result"]["applied_generation"], 1,
        "refusals change nothing"
    );
    assert_eq!(status["result"]["config_sha256"], sha256(&bytes));
    // Generation 2: bob leaves the channel but keeps the account (KICK).
    changed.membership_generation = Some(2);
    let bytes = render(&file, &changed);
    let applied = control(&socket, r#"{"op":"reload"}"#).await;
    assert_eq!(applied["result"]["applied_generation"], 2, "{applied}");
    assert_eq!(applied["result"]["config_sha256"], sha256(&bytes));
    assert_eq!(applied["result"]["incarnation"], incarnation);
    assert_eq!(applied["result"]["kicked"][0]["account"], "bob");
    let kick = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let m = bob.inbound.recv().await.unwrap();
            if m.command == "KICK" {
                break m;
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(
        (kick.params[0].as_str(), kick.params[1].as_str()),
        ("#space", "bob")
    );
    bob.outbound
        .send(vec![Message::new("JOIN", ["#space"])])
        .await
        .unwrap();
    // Generation 3: bob's account is removed; its live session is revoked.
    let mut removed = changed.clone();
    removed.accounts.retain(|a| a.name != "bob");
    removed.membership_generation = Some(3);
    render(&file, &removed);
    let applied = control(&socket, r#"{"op":"reload"}"#).await;
    assert_eq!(
        applied["result"]["disconnected"],
        serde_json::json!(["bob"]),
        "{applied}"
    );
    closed_with(&mut bob, "Membership revoked").await;
    assert!(
        client::connect(&member(dir.path(), &config, "bob"))
            .await
            .is_err()
    );
    // Generation 4: alice's token rotates; the old token fails everywhere.
    let rotated = "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210";
    let mut rotation = removed.clone();
    rotation.accounts[0].token_sha256 = token_hash(rotated);
    rotation.membership_generation = Some(4);
    render(&file, &rotation);
    let applied = control(&socket, r#"{"op":"reload"}"#).await;
    assert_eq!(
        applied["result"]["disconnected"],
        serde_json::json!(["alice"])
    );
    closed_with(&mut alice, "Membership revoked").await;
    assert!(
        client::connect(&member(dir.path(), &config, "alice"))
            .await
            .is_err()
    );
    let artifacts = config
        .artifacts
        .as_ref()
        .unwrap()
        .listen_unix
        .clone()
        .unwrap();
    let http = reqwest::Client::builder()
        .unix_socket(artifacts)
        .build()
        .unwrap();
    let denied = http
        .post("http://airc.local/v1/channels/%23space/artifacts")
        .basic_auth("alice", Some(TOKEN))
        .header("x-airc-filename", "a.txt")
        .body("x")
        .send()
        .await
        .unwrap();
    assert_eq!(denied.status(), 401, "old token rejected by artifact HTTP");
    let token = dir.path().join("rotated.token");
    write_private(&token, rotated.as_bytes()).unwrap();
    let mut fresh = member(dir.path(), &config, "alice");
    fresh.token_file = token;
    let alice = client::connect(&fresh).await.unwrap();
    alice
        .outbound
        .send(vec![Message::new("QUIT", ["done"])])
        .await
        .unwrap();
    server.shutdown().await.unwrap();
    assert!(!socket.exists(), "control socket removed with the server");
}

async fn jsonl(
    config: ClientConfig,
    version: u8,
) -> (
    tokio::io::DuplexStream,
    BufReader<tokio::io::DuplexStream>,
    tokio::task::JoinHandle<anyhow::Result<()>>,
) {
    let (requests, input) = tokio::io::duplex(1 << 20);
    let (output, responses) = tokio::io::duplex(1 << 20);
    let task = tokio::spawn(client::frontend_with(
        config,
        version,
        BufReader::new(input),
        output,
    ));
    (requests, BufReader::new(responses), task)
}

async fn json_line(reader: &mut BufReader<tokio::io::DuplexStream>) -> serde_json::Value {
    let line = tokio::time::timeout(Duration::from_secs(10), read_line(reader, 1 << 21))
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    serde_json::from_str(&line).unwrap()
}

/// Read v2 events, keeping all of them, until `predicate` matches one.
async fn until_event(
    requests: &mut tokio::io::DuplexStream,
    responses: &mut BufReader<tokio::io::DuplexStream>,
    seen: &mut Vec<serde_json::Value>,
    predicate: impl Fn(&serde_json::Value) -> bool,
) -> serde_json::Value {
    for _ in 0..20 {
        if let Some(event) = seen.iter().find(|e| predicate(e)) {
            return event.clone();
        }
        let after = seen.last().map_or(0, |e| e["cursor"].as_u64().unwrap());
        let read = serde_json::json!({"id":"r","op":"read","after":after,"wait_ms":1000});
        requests
            .write_all(format!("{read}\n").as_bytes())
            .await
            .unwrap();
        let page = json_line(responses).await;
        seen.extend(page["result"]["events"].as_array().unwrap().iter().cloned());
    }
    panic!("event not observed");
}

#[tokio::test]
async fn jsonl_v2_assembles_multiline_and_reports_correlated_receipts() {
    let dir = tempfile::tempdir().unwrap();
    let config = unix_config(dir.path());
    let server = server::start(config.clone()).await.unwrap();
    let (mut requests, mut responses, task) = jsonl(member(dir.path(), &config, "alice"), 2).await;
    let ready = json_line(&mut responses).await;
    assert_eq!(ready["version"], 2);
    assert_eq!(ready["multiline"]["max_bytes"], 65536);
    // Version 1 is unchanged: no multiline negotiation, raw fallback lines.
    let (mut v1_requests, mut v1, v1_task) = jsonl(member(dir.path(), &config, "bob"), 1).await;
    let v1_ready = json_line(&mut v1).await;
    assert_eq!(v1_ready["version"], 1);
    assert!(v1_ready.get("multiline").is_none());
    let text = "line one\n\n```\nfenced\n```";
    let send = serde_json::json!({"id":1,"op":"send","target":"#space","text":text});
    requests
        .write_all(format!("{send}\n").as_bytes())
        .await
        .unwrap();
    let pending = json_line(&mut responses).await;
    assert_eq!(pending["result"]["state"], "pending", "{pending}");
    let receipt = pending["result"]["receipt"].as_str().unwrap().to_owned();
    let mut seen = Vec::new();
    let echo = until_event(&mut requests, &mut responses, &mut seen, |e| {
        e["command"] == "PRIVMSG"
    })
    .await;
    assert_eq!(echo["params"][1], text);
    assert_eq!(echo["lines"], 5);
    let confirmed = until_event(&mut requests, &mut responses, &mut seen, |e| {
        e["event"] == "receipt"
    })
    .await;
    let position = |e: &serde_json::Value| seen.iter().position(|s| s == e).unwrap();
    assert!(
        position(&echo) < position(&confirmed),
        "echo precedes its receipt"
    );
    assert_eq!(confirmed["receipt"], receipt);
    assert_eq!(confirmed["state"], "confirmed");
    assert_eq!(confirmed["msgid"], echo["tags"]["msgid"]);
    // A rejection is correlated too.
    let send = serde_json::json!({"id":2,"op":"send","target":"#nowhere","text":"x\ny"});
    requests
        .write_all(format!("{send}\n").as_bytes())
        .await
        .unwrap();
    let pending = json_line(&mut responses).await;
    let receipt = pending["result"]["receipt"].clone();
    let rejected = until_event(&mut requests, &mut responses, &mut seen, |e| {
        e["event"] == "receipt" && e["receipt"] == receipt
    })
    .await;
    assert_eq!(
        (rejected["receipt"].clone(), rejected["state"].clone()),
        (receipt, "rejected".into())
    );
    // CR is refused locally without a receipt.
    let send = serde_json::json!({"id":3,"op":"send","target":"#space","text":"a\r\nb"});
    requests
        .write_all(format!("{send}\n").as_bytes())
        .await
        .unwrap();
    assert_eq!(json_line(&mut responses).await["ok"], false);
    // The v1 client saw the component lines of the first message.
    let mut lines = Vec::new();
    let mut after = 0;
    while lines.len() < 4 {
        let read = serde_json::json!({"id":"r","op":"read","after":after,"wait_ms":1000});
        v1_requests
            .write_all(format!("{read}\n").as_bytes())
            .await
            .unwrap();
        let page = json_line(&mut v1).await;
        after = page["result"]["next"].as_u64().unwrap();
        for event in page["result"]["events"].as_array().unwrap() {
            if event["command"] == "PRIVMSG" {
                lines.push(event["params"][1].as_str().unwrap().to_owned());
            }
        }
    }
    assert_eq!(
        lines,
        ["line one", "```", "fenced", "```"],
        "blank line skipped"
    );
    for (requests, task) in [(&mut requests, task), (&mut v1_requests, v1_task)] {
        requests
            .write_all(b"{\"id\":9,\"op\":\"quit\"}\n")
            .await
            .unwrap();
        task.await.unwrap().unwrap();
    }
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn jsonl_v2_publishes_staged_attachments_as_one_confirmed_message() {
    let dir = tempfile::tempdir().unwrap();
    let config = unix_config(dir.path());
    let server = server::start(config.clone()).await.unwrap();
    let alice = member(dir.path(), &config, "alice");
    let (mut requests, mut responses, task) = jsonl(alice.clone(), 2).await;
    assert_eq!(json_line(&mut responses).await["version"], 2);
    let mut seen = Vec::new();
    let staged = client::stage_bytes(&alice, "#space", "paste-1.png", b"png".to_vec())
        .await
        .unwrap();
    assert_eq!(staged["staged"], true, "{staged}");
    let id = staged["id"].as_str().unwrap().to_owned();
    // A marker away from its offset is refused locally; nothing is sent.
    let send = |n: u8, text: &str, at: usize| {
        let send = serde_json::json!({"id":n,"op":"send","target":"#space","text":text,"attach":[{"id":id,"at":at}]});
        format!("{send}\n")
    };
    requests
        .write_all(send(1, "see [Image #1]", 0).as_bytes())
        .await
        .unwrap();
    let refused = json_line(&mut responses).await;
    assert_eq!(refused["ok"], false);
    assert!(
        refused["error"]
            .as_str()
            .unwrap()
            .contains("invalid attachments"),
        "{refused}"
    );
    let text = "see\n[Image #1] here";
    requests
        .write_all(send(2, text, 4).as_bytes())
        .await
        .unwrap();
    let receipt = json_line(&mut responses).await["result"]["receipt"].clone();
    let echo = until_event(&mut requests, &mut responses, &mut seen, |e| {
        e["command"] == "PRIVMSG" && e["params"][1] == text
    })
    .await;
    assert_eq!(
        echo["tags"]["airc-artifacts"],
        format!(
            "{id}/3/{}/4/paste-1.png",
            staged["sha256"].as_str().unwrap()
        )
    );
    assert!(echo["tags"].get("+airc-attach").is_none());
    let confirmed = until_event(&mut requests, &mut responses, &mut seen, |e| {
        e["event"] == "receipt" && e["receipt"] == receipt
    })
    .await;
    assert_eq!(confirmed["state"], "confirmed");
    assert_eq!(confirmed["msgid"], echo["tags"]["msgid"]);
    // Resending the same submission is a correlated, definite rejection that
    // names the message which already published it.
    requests
        .write_all(send(3, text, 4).as_bytes())
        .await
        .unwrap();
    let receipt = json_line(&mut responses).await["result"]["receipt"].clone();
    let rejected = until_event(&mut requests, &mut responses, &mut seen, |e| {
        e["event"] == "receipt" && e["receipt"] == receipt
    })
    .await;
    assert_eq!(rejected["state"], "rejected");
    let consumed = format!(
        "attachments: consumed k=1 msgid={}",
        echo["tags"]["msgid"].as_str().unwrap()
    );
    assert!(
        rejected["reason"].as_str().unwrap().ends_with(&consumed),
        "{rejected}"
    );
    requests
        .write_all(b"{\"id\":9,\"op\":\"quit\"}\n")
        .await
        .unwrap();
    task.await.unwrap().unwrap();
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn jsonl_v2_replays_a_multiline_attachment_message_from_history() {
    let dir = tempfile::tempdir().unwrap();
    let config = unix_config(dir.path());
    let server = server::start(config.clone()).await.unwrap();
    let alice = member(dir.path(), &config, "alice");
    let (mut requests, mut responses, task) = jsonl(alice.clone(), 2).await;
    assert_eq!(json_line(&mut responses).await["version"], 2);
    let mut ids = Vec::new();
    for name in ["paste-1.png", "paste-2.png"] {
        let staged = client::stage_bytes(&alice, "#space", name, b"png".to_vec())
            .await
            .unwrap();
        ids.push(staged["id"].as_str().unwrap().to_owned());
    }
    let text = "first [Image #1]\nsecond [Image #2]";
    let second = text.find("[Image #2]").unwrap();
    let send = serde_json::json!({"id":1,"op":"send","target":"#space","text":text,
        "attach":[{"id":ids[0],"at":6},{"id":ids[1],"at":second}]});
    requests
        .write_all(format!("{send}\n").as_bytes())
        .await
        .unwrap();
    let receipt = json_line(&mut responses).await["result"]["receipt"].clone();
    let mut seen = Vec::new();
    let confirmed = until_event(&mut requests, &mut responses, &mut seen, |e| {
        e["event"] == "receipt" && e["receipt"] == receipt
    })
    .await;
    assert_eq!(confirmed["state"], "confirmed", "{confirmed}");
    // A fresh client replays it from history as one assembled message whose
    // offsets still hold their markers.
    let (mut bob_requests, mut bob, bob_task) = jsonl(member(dir.path(), &config, "bob"), 2).await;
    json_line(&mut bob).await;
    let history = serde_json::json!({"id":"h","op":"history","channel":"#space","direction":"LATEST","reference":"*","limit":10});
    bob_requests
        .write_all(format!("{history}\n").as_bytes())
        .await
        .unwrap();
    assert_eq!(json_line(&mut bob).await["ok"], true);
    let mut replayed = Vec::new();
    let replay = until_event(&mut bob_requests, &mut bob, &mut replayed, |e| {
        e["command"] == "PRIVMSG" && e["params"][1] == text
    })
    .await;
    assert!(replay["tags"].get("batch").is_some(), "{replay}");
    let offsets: Vec<usize> = replay["tags"]["airc-artifacts"]
        .as_str()
        .unwrap()
        .split(',')
        .map(|item| item.split('/').nth(3).unwrap().parse().unwrap())
        .collect();
    assert_eq!(offsets, [6, second]);
    for (requests, task) in [(&mut requests, task), (&mut bob_requests, bob_task)] {
        requests
            .write_all(b"{\"id\":9,\"op\":\"quit\"}\n")
            .await
            .unwrap();
        task.await.unwrap().unwrap();
    }
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn relative_configuration_names_resolve_unix_endpoints_from_the_file() {
    let dir = tempfile::tempdir().unwrap();
    let room = dir.path().join("room");
    std::fs::create_dir_all(room.join("endpoint")).unwrap();
    let mut config = unix_config(&room);
    config.listen_unix = Some("endpoint/irc.sock".into());
    config.artifacts.as_mut().unwrap().listen_unix = Some("endpoint/artifacts.sock".into());
    config.database = "state.db".into();
    std::fs::write(room.join("server.toml"), toml::to_string(&config).unwrap()).unwrap();
    // Run from a different working directory with a relative file name.
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_aircd"))
        .args(["check", "--config", "room/server.toml"])
        .current_dir(dir.path())
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let server = server::start_file(&room.join("server.toml")).await.unwrap();
    write_private(&room.join("alice.token"), TOKEN.as_bytes()).unwrap();
    let client = "unix = \"endpoint/irc.sock\"\nartifact_unix = \"endpoint/artifacts.sock\"\naccount = \"alice\"\nnick = \"alice\"\ntoken_file = \"alice.token\"\n";
    std::fs::write(room.join("client.toml"), client).unwrap();
    let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_airc"))
        .args(["--config", "room/client.toml", "--jsonl", "2"])
        .current_dir(dir.path())
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut stdout = BufReader::new(child.stdout.take().unwrap());
    let ready = tokio::time::timeout(Duration::from_secs(10), read_line(&mut stdout, 65536))
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(ready.contains("\"ready\""), "{ready}");
    child.kill().await.unwrap();
    server.shutdown().await.unwrap();
}

async fn command_on(connection: &mut client::Connection, command: &str) -> Message {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let m = connection.inbound.recv().await.unwrap();
            if m.command == command {
                break m;
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("no {command}"))
}

#[tokio::test]
async fn reload_adds_channels_and_removes_one_channel_acl_without_revoking_the_account() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = unix_config(dir.path());
    let control_dir = dir.path().join("control");
    std::fs::create_dir(&control_dir).unwrap();
    let socket = control_dir.join("control.sock");
    config.control_unix = Some(socket.clone());
    config.membership_generation = Some(1);
    let file = dir.path().join("server.toml");
    render(&file, &config);
    let server = server::start_file(&file).await.unwrap();
    let incarnation = control(&socket, r#"{"op":"status"}"#).await["result"]["incarnation"].clone();
    let mut bob = client::connect(&member(dir.path(), &config, "bob"))
        .await
        .unwrap();
    // Generation 2 adds #ops; the applied #space catalog stays.
    let mut added = config.clone();
    added.channels.push(Channel {
        name: "#ops".into(),
        members: vec!["alice".into(), "bob".into()],
        operators: vec![],
        moderated: false,
    });
    added.membership_generation = Some(2);
    let bytes = render(&file, &added);
    let applied = control(&socket, r#"{"op":"reload"}"#).await;
    assert_eq!(applied["result"]["applied_generation"], 2, "{applied}");
    assert_eq!(applied["result"]["config_sha256"], sha256(&bytes));
    assert_eq!(applied["result"]["incarnation"], incarnation);
    bob.outbound
        .send(vec![Message::new("JOIN", ["#ops"])])
        .await
        .unwrap();
    // The startup JOIN #space may still be queued ahead of it.
    while command_on(&mut bob, "JOIN").await.params[0] != "#ops" {}
    // Generation 3 removes bob from #ops only: a KICK, not account revocation.
    let mut removed = added.clone();
    removed.channels[1].members.retain(|m| m != "bob");
    removed.membership_generation = Some(3);
    render(&file, &removed);
    let applied = control(&socket, r#"{"op":"reload"}"#).await;
    assert_eq!(
        applied["result"]["disconnected"],
        serde_json::json!([]),
        "{applied}"
    );
    assert_eq!(
        applied["result"]["kicked"],
        serde_json::json!([{"channel": "#ops", "account": "bob"}])
    );
    let kick = command_on(&mut bob, "KICK").await;
    assert_eq!(
        (kick.params[0].as_str(), kick.params[1].as_str()),
        ("#ops", "bob")
    );
    bob.outbound
        .send(vec![Message::new("PRIVMSG", ["#space", "still here"])])
        .await
        .unwrap();
    assert_eq!(
        command_on(&mut bob, "PRIVMSG").await.params[1],
        "still here"
    );
    bob.outbound
        .send(vec![Message::new("JOIN", ["#ops"])])
        .await
        .unwrap();
    assert_eq!(command_on(&mut bob, "403").await.params[1], "#ops");
    // A -> A+B -> A+C: removing the applied #ops is refused; nothing changes.
    let mut replaced = removed.clone();
    replaced.channels[1].name = "#other".into();
    replaced.membership_generation = Some(4);
    render(&file, &replaced);
    let refused = control(&socket, r#"{"op":"reload"}"#).await;
    assert_eq!(refused["code"], "RESTART_REQUIRED", "{refused}");
    let status = control(&socket, r#"{"op":"status"}"#).await;
    assert_eq!(status["result"]["applied_generation"], 3);
    assert_eq!(status["result"]["accounts"]["bob"]["connections"], 1);
    server.shutdown().await.unwrap();
}
