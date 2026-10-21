#![cfg(unix)]

use std::{
    io::{BufRead, BufReader, Read},
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::{Command, Output, Stdio},
};

/// Stand-in for `airc`: `status` reports stopped for FAKE_STOPPED_POLLS polls,
/// then connected with FAKE_CONSUMER (default none); `subscribe` records argv
/// and acts per FAKE_MODE.
const FAKE_AIRC: &str = r#"#!/bin/sh
case "$1" in
status)
  n=$(cat "$FAKE_DIR/polls" 2>/dev/null || echo 0)
  n=$((n + 1))
  echo "$n" > "$FAKE_DIR/polls"
  if [ "$n" -le "${FAKE_STOPPED_POLLS:-0}" ]; then
    echo "! session $2 is stopped; run airc connect $2 --config FILE. No request was sent."
    exit 1
  fi
  echo "connected; worker(acct); epoch=e; unread=0"
  echo "connected; no automatic reconnect or resend"
  echo "delivery consumer: ${FAKE_CONSUMER:-none}" ;;
subscribe)
  printf '%s\0' "$@" > "$FAKE_DIR/argv"
  case "$FAKE_MODE" in
  batch)
    printf '[airc delivery id=1 session=%s]\n@1 #work bob(bob): @worker hi\n' "$2"
    echo "! session closed before replying; a batch may be pending"
    exit 1 ;;
  usage) echo "error: invalid value 'loud' for '--wake <WAKE>'" >&2; exit 2 ;;
  hold)
    trap 'echo term > "$FAKE_DIR/term"; exit 1' TERM
    echo ready
    i=0
    while [ "$i" -lt 20 ]; do sleep 1; i=$((i + 1)); done
    exit 3 ;;
  esac ;;
esac
"#;

fn plugin() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("integrations/claude-plugin")
}

struct Fake {
    dir: tempfile::TempDir,
}

impl Fake {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let airc = dir.path().join("airc");
        std::fs::write(&airc, FAKE_AIRC).unwrap();
        std::fs::set_permissions(&airc, std::fs::Permissions::from_mode(0o700)).unwrap();
        Self { dir }
    }

    fn launcher(&self, env: &[(&str, &str)]) -> Command {
        let mut command = Command::new(plugin().join("scripts/airc-delivery"));
        command
            .env_clear()
            .env(
                "PATH",
                format!("{}:/usr/bin:/bin", self.dir.path().display()),
            )
            .env("FAKE_DIR", self.dir.path())
            .envs(env.iter().copied())
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        command
    }

    fn run(&self, env: &[(&str, &str)]) -> Output {
        self.launcher(env).output().unwrap()
    }

    fn argv(&self) -> Option<Vec<String>> {
        let raw = std::fs::read(self.dir.path().join("argv")).ok()?;
        let mut args: Vec<String> = raw
            .split(|byte| *byte == 0)
            .map(|arg| String::from_utf8(arg.to_vec()).unwrap())
            .collect();
        args.pop();
        Some(args)
    }

    fn polls(&self) -> u32 {
        std::fs::read_to_string(self.dir.path().join("polls"))
            .map_or(0, |text| text.trim().parse().unwrap())
    }
}

fn stop_lines(stdout: &str) -> Vec<&str> {
    stdout
        .lines()
        .filter(|line| line.starts_with("AIRC delivery stopped:"))
        .collect()
}

#[test]
fn manifest_declares_one_monitor_running_the_bundled_launcher() {
    let manifest: serde_json::Value = serde_json::from_slice(
        &std::fs::read(plugin().join(".claude-plugin/plugin.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(manifest["name"], "airc-delivery");
    let monitors: serde_json::Value =
        serde_json::from_slice(&std::fs::read(plugin().join("monitors/monitors.json")).unwrap())
            .unwrap();
    let monitors = monitors.as_array().unwrap();
    assert_eq!(monitors.len(), 1);
    assert_eq!(
        monitors[0]["command"],
        "\"${CLAUDE_PLUGIN_ROOT}\"/scripts/airc-delivery"
    );
    let mode = std::fs::metadata(plugin().join("scripts/airc-delivery"))
        .unwrap()
        .permissions()
        .mode();
    assert_eq!(mode & 0o111, 0o111, "launcher must be executable");
}

#[test]
fn without_a_session_the_launcher_does_nothing() {
    let fake = Fake::new();
    let output = fake.run(&[]);
    assert!(output.status.success());
    assert!(output.stdout.is_empty() && output.stderr.is_empty());
    assert_eq!(fake.polls(), 0);
    assert!(fake.argv().is_none());
}

#[test]
fn waits_for_the_connection_then_prints_batches_and_one_stop_line() {
    let fake = Fake::new();
    let output = fake.run(&[
        ("AIRC_DELIVERY_SESSION", "space"),
        ("FAKE_STOPPED_POLLS", "1"),
        ("FAKE_MODE", "batch"),
    ]);
    assert!(output.status.success());
    assert_eq!(fake.polls(), 2, "one stopped poll, then connected");
    assert_eq!(
        fake.argv().unwrap(),
        [
            "subscribe",
            "space",
            "--wake",
            "mentions",
            "--min-interval",
            "15",
            "--max-bytes",
            "2560",
            "--wrap",
            "480",
            "--",
            "-"
        ]
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.starts_with(
        "[airc delivery id=1 session=space]\n@1 #work bob(bob): @worker hi\n! session closed"
    ));
    assert_eq!(
        stop_lines(&stdout),
        ["AIRC delivery stopped: airc subscribe space exited 1; use the airc skill's wait recipe."]
    );
    assert!(stdout.ends_with("wait recipe.\n"));
}

#[test]
fn a_lingering_consumer_gets_one_bounded_pause_before_subscribing() {
    let fake = Fake::new();
    let started = std::time::Instant::now();
    let output = fake.run(&[
        ("AIRC_DELIVERY_SESSION", "space"),
        ("FAKE_CONSUMER", "subscribe pid=9 since=t last_request=t"),
        ("FAKE_MODE", "batch"),
    ]);
    assert!(output.status.success());
    assert!(started.elapsed() >= std::time::Duration::from_secs(2));
    assert_eq!(fake.polls(), 1, "the pause polls nothing more");
    assert_eq!(fake.argv().unwrap()[..2], ["subscribe", "space"]);
    assert_eq!(
        stop_lines(&String::from_utf8(output.stdout).unwrap()).len(),
        1
    );
}

#[test]
fn subscribe_diagnostics_reach_stdout_with_the_chosen_wake() {
    let fake = Fake::new();
    let output = fake.run(&[
        ("AIRC_DELIVERY_SESSION", "space"),
        ("AIRC_DELIVERY_WAKE", "loud"),
        ("FAKE_MODE", "usage"),
    ]);
    assert_eq!(fake.argv().unwrap()[2..4], ["--wake", "loud"]);
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("error: invalid value 'loud'"));
    assert_eq!(stop_lines(&stdout).len(), 1);
    assert!(stdout.contains("exited 2;"));
}

#[test]
fn terminating_the_launcher_stops_its_consumer() {
    let fake = Fake::new();
    let mut child = fake
        .launcher(&[("AIRC_DELIVERY_SESSION", "space"), ("FAKE_MODE", "hold")])
        .spawn()
        .unwrap();
    let mut stdout = BufReader::new(child.stdout.take().unwrap());
    let mut line = String::new();
    stdout.read_line(&mut line).unwrap();
    assert_eq!(line, "ready\n");
    let killed = Command::new("kill")
        .args(["-TERM", &child.id().to_string()])
        .status()
        .unwrap();
    assert!(killed.success());
    let mut rest = String::new();
    stdout.read_to_string(&mut rest).unwrap();
    assert!(child.wait().unwrap().success());
    assert!(
        fake.dir.path().join("term").exists(),
        "subscribe must receive the forwarded TERM"
    );
    assert_eq!(
        stop_lines(&rest),
        ["AIRC delivery stopped: airc subscribe space exited 1; use the airc skill's wait recipe."]
    );
}
