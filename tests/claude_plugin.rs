#![cfg(unix)]

use std::{
    io::{BufRead, BufReader, Read},
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::{Child, ChildStdout, Command, Output, Stdio},
    time::{Duration, Instant},
};

/// Stand-in for `airc`.
///
/// `status` answers poll N from the comma-separated FAKE_STATUS (the last
/// entry repeats): `stopped`, `noepoch` (connected without an epoch), or an
/// epoch, optionally suffixed `+busy` for a registered consumer. Without
/// FAKE_STATUS it reports stopped for FAKE_STOPPED_POLLS polls, then connected
/// in epoch `e` with FAKE_CONSUMER (default none).
///
/// `subscribe` call N records argv (also as `argv.N`) and acts per entry N of
/// FAKE_MODES, or FAKE_MODE.
const FAKE_AIRC: &str = r#"#!/bin/sh
nth() { printf '%s\n' "$1" | tr ',' '\n' | sed -n "$2p"; }
case "$1" in
status)
  n=$(cat "$FAKE_DIR/polls" 2>/dev/null || echo 0)
  n=$((n + 1))
  echo "$n" > "$FAKE_DIR/polls"
  consumer=${FAKE_CONSUMER:-none}
  if [ -n "${FAKE_STATUS:-}" ]; then
    s=$(nth "$FAKE_STATUS" "$n")
    [ -n "$s" ] || s=$(printf '%s\n' "$FAKE_STATUS" | tr ',' '\n' | tail -n 1)
  elif [ "$n" -le "${FAKE_STOPPED_POLLS:-0}" ]; then
    s=stopped
  else
    s=e
  fi
  case $s in *+busy) s=${s%+busy} consumer="subscribe pid=9 since=t last_request=t" ;; esac
  case $s in
  stopped)
    echo "! session $2 is stopped; run airc connect $2 --config FILE. No request was sent."
    exit 1 ;;
  noepoch) echo "connected; worker(acct); unread=0" ;;
  *) echo "connected; worker(acct); epoch=$s; unread=0" ;;
  esac
  echo "connected; no automatic reconnect or resend"
  echo "delivery consumer: $consumer" ;;
subscribe)
  c=$(cat "$FAKE_DIR/calls" 2>/dev/null || echo 0)
  c=$((c + 1))
  echo "$c" > "$FAKE_DIR/calls"
  printf '%s\0' "$@" > "$FAKE_DIR/argv"
  cp "$FAKE_DIR/argv" "$FAKE_DIR/argv.$c"
  mode=${FAKE_MODE:-}
  [ -z "${FAKE_MODES:-}" ] || mode=$(nth "$FAKE_MODES" "$c")
  case "$mode" in
  batch)
    printf '[airc id=1 session=%s events=0 gap retry]\n! delivery settings: account=worker nick=worker wake=mentions max-bytes=2560 wrap=480 min-interval=15\nm:Ej5FZ-ibQtOkVkJmFBdAAA #work bob(bob): @worker hi\n' "$2"
    echo "! session closed before replying; a batch may be pending"
    exit 1 ;;
  drop)
    echo "! IRC disconnected; reconnect explicitly; pending sends are uncertain"
    exit 1 ;;
  active)
    echo "! delivery consumer active since t (pid 9); stop it first. Nothing was replayed."
    exit 1 ;;
  pending)
    echo "! delivery 7 pending since t (outcome uncertain); inspect it. Nothing was replayed."
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

const STOPPED_1: &str =
    "AIRC delivery stopped: airc subscribe space exited 1; use the airc skill's wait recipe.";
const STOPPED_IDLE: &str = "AIRC delivery stopped: the monitor was terminated while no airc subscribe space was running; use the airc skill's wait recipe.";
const WAITING_1: &str = "AIRC delivery waiting: airc subscribe space exited 1 after its connection ended; this monitor subscribes again once the session is connected. Do not arm a waiter.";

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

    fn argv_file(&self, name: &str) -> Option<Vec<String>> {
        let raw = std::fs::read(self.dir.path().join(name)).ok()?;
        let mut args: Vec<String> = raw
            .split(|byte| *byte == 0)
            .map(|arg| String::from_utf8(arg.to_vec()).unwrap())
            .collect();
        args.pop();
        Some(args)
    }

    fn argv(&self) -> Option<Vec<String>> {
        self.argv_file("argv")
    }

    fn counter(&self, name: &str) -> u32 {
        std::fs::read_to_string(self.dir.path().join(name))
            .map_or(0, |text| text.trim().parse().unwrap())
    }

    fn polls(&self) -> u32 {
        self.counter("polls")
    }

    fn calls(&self) -> u32 {
        self.counter("calls")
    }
}

fn stop_lines(stdout: &str) -> Vec<&str> {
    stdout
        .lines()
        .filter(|line| line.starts_with("AIRC delivery stopped:"))
        .collect()
}

fn waiting_lines(stdout: &str) -> Vec<&str> {
    stdout
        .lines()
        .filter(|line| line.starts_with("AIRC delivery waiting:"))
        .collect()
}

/// Read lines until one equals `marker`; returns everything read.
fn read_until(stdout: &mut BufReader<ChildStdout>, marker: &str) -> String {
    let mut seen = String::new();
    loop {
        let mut line = String::new();
        assert_ne!(
            stdout.read_line(&mut line).unwrap(),
            0,
            "EOF before {marker:?}; got {seen:?}"
        );
        seen.push_str(&line);
        if line.trim_end() == marker {
            return seen;
        }
    }
}

/// TERM the launcher and read the rest of its output, which must reach EOF
/// promptly: no sleep or consumer may keep the output open.
fn terminate(child: &mut Child, stdout: &mut BufReader<ChildStdout>) -> (String, Duration) {
    let started = Instant::now();
    let killed = Command::new("kill")
        .args(["-TERM", &child.id().to_string()])
        .status()
        .unwrap();
    assert!(killed.success());
    let mut rest = String::new();
    stdout.read_to_string(&mut rest).unwrap();
    let elapsed = started.elapsed();
    assert!(child.wait().unwrap().success());
    (rest, elapsed)
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
    assert_eq!(
        fake.polls(),
        3,
        "one stopped poll, then connected, then the same epoch after subscribe"
    );
    assert_eq!(fake.calls(), 1);
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
    // The monitor forwards the compact header, its bare flags and the settings
    // notice untouched.
    assert!(stdout.starts_with(
        "[airc id=1 session=space events=0 gap retry]\n! delivery settings: account=worker nick=worker wake=mentions max-bytes=2560 wrap=480 min-interval=15\nm:Ej5FZ-ibQtOkVkJmFBdAAA #work bob(bob): @worker hi\n! session closed"
    ));
    assert!(waiting_lines(&stdout).is_empty());
    assert_eq!(stop_lines(&stdout), [STOPPED_1]);
    assert!(stdout.ends_with("wait recipe.\n"));
}

#[test]
fn a_lingering_consumer_gets_one_bounded_pause_before_subscribing() {
    let fake = Fake::new();
    let started = Instant::now();
    let output = fake.run(&[
        ("AIRC_DELIVERY_SESSION", "space"),
        ("FAKE_CONSUMER", "subscribe pid=9 since=t last_request=t"),
        ("FAKE_MODE", "batch"),
    ]);
    assert!(output.status.success());
    assert!(started.elapsed() >= Duration::from_secs(2));
    assert_eq!(
        fake.polls(),
        2,
        "the pause polls nothing; one poll after subscribe"
    );
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
    assert_eq!(fake.calls(), 1, "a usage error is final");
    assert_eq!(fake.polls(), 1, "and needs no status after it");
    assert!(waiting_lines(&stdout).is_empty());
}

#[test]
fn terminating_the_launcher_stops_its_consumer() {
    let fake = Fake::new();
    let mut child = fake
        .launcher(&[("AIRC_DELIVERY_SESSION", "space"), ("FAKE_MODE", "hold")])
        .spawn()
        .unwrap();
    let mut stdout = BufReader::new(child.stdout.take().unwrap());
    read_until(&mut stdout, "ready");
    let (rest, _) = terminate(&mut child, &mut stdout);
    assert!(
        fake.dir.path().join("term").exists(),
        "subscribe must receive the forwarded TERM"
    );
    assert_eq!(stop_lines(&rest), [STOPPED_1]);
    assert_eq!(fake.calls(), 1);
}

#[test]
fn a_dropped_connection_resubscribes_after_the_reconnect_with_the_same_arguments() {
    let fake = Fake::new();
    let mut child = fake
        .launcher(&[
            ("AIRC_DELIVERY_SESSION", "space"),
            ("AIRC_DELIVERY_WAKE", "messages"),
            ("FAKE_STATUS", "e1,stopped,stopped,e2"),
            ("FAKE_MODES", "drop,hold"),
        ])
        .spawn()
        .unwrap();
    let mut stdout = BufReader::new(child.stdout.take().unwrap());
    let before = read_until(&mut stdout, "ready");
    assert_eq!(
        before,
        format!(
            "! IRC disconnected; reconnect explicitly; pending sends are uncertain\n{WAITING_1}\nready\n"
        )
    );
    assert_eq!(fake.calls(), 2);
    assert_eq!(fake.polls(), 4);
    let first = fake.argv_file("argv.1").unwrap();
    assert_eq!(first[2..4], ["--wake", "messages"]);
    assert_eq!(fake.argv_file("argv.2").unwrap(), first);
    let (rest, _) = terminate(&mut child, &mut stdout);
    assert!(fake.dir.path().join("term").exists());
    assert_eq!(stop_lines(&rest), [STOPPED_1]);
    assert!(waiting_lines(&rest).is_empty());
}

#[test]
fn a_reconnect_seen_right_after_the_drop_resubscribes_once() {
    let fake = Fake::new();
    let output = fake.run(&[
        ("AIRC_DELIVERY_SESSION", "space"),
        ("FAKE_STATUS", "e1,e2"),
        ("FAKE_MODES", "drop,batch"),
    ]);
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert_eq!(fake.calls(), 2);
    assert_eq!(waiting_lines(&stdout), [WAITING_1]);
    // The second subscribe ends while e2 is still connected: final.
    assert_eq!(stop_lines(&stdout), [STOPPED_1]);
    assert!(stdout.contains("m:Ej5FZ-ibQtOkVkJmFBdAAA"));
}

#[test]
fn a_refusal_from_the_same_connection_is_final() {
    let fake = Fake::new();
    let output = fake.run(&[
        ("AIRC_DELIVERY_SESSION", "space"),
        ("FAKE_STATUS", "e1"),
        ("FAKE_MODE", "active"),
    ]);
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert_eq!(fake.calls(), 1, "no second attempt in the same epoch");
    assert!(stdout.starts_with("! delivery consumer active"));
    assert!(waiting_lines(&stdout).is_empty());
    assert_eq!(stop_lines(&stdout), [STOPPED_1]);
}

#[test]
fn a_pending_batch_after_the_reconnect_ends_the_monitor() {
    let fake = Fake::new();
    let output = fake.run(&[
        ("AIRC_DELIVERY_SESSION", "space"),
        ("FAKE_STATUS", "e1,stopped,e2"),
        ("FAKE_MODES", "drop,pending"),
    ]);
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert_eq!(fake.calls(), 2);
    assert_eq!(waiting_lines(&stdout), [WAITING_1]);
    let lines: Vec<&str> = stdout.lines().collect();
    assert_eq!(
        lines[lines.len() - 2],
        "! delivery 7 pending since t (outcome uncertain); inspect it. Nothing was replayed."
    );
    assert_eq!(lines[lines.len() - 1], STOPPED_1);
}

#[test]
fn a_reconnect_without_a_readable_epoch_is_not_treated_as_new() {
    let fake = Fake::new();
    let output = fake.run(&[
        ("AIRC_DELIVERY_SESSION", "space"),
        ("FAKE_STATUS", "e1,stopped,noepoch"),
        ("FAKE_MODES", "drop,hold"),
    ]);
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert_eq!(fake.calls(), 1);
    assert_eq!(waiting_lines(&stdout), [WAITING_1]);
    assert_eq!(stop_lines(&stdout), [STOPPED_1]);
}

#[test]
fn terminating_while_waiting_for_a_reconnect_ends_promptly() {
    let fake = Fake::new();
    let mut child = fake
        .launcher(&[
            ("AIRC_DELIVERY_SESSION", "space"),
            ("FAKE_STATUS", "e1,stopped"),
            ("FAKE_MODES", "drop,hold"),
        ])
        .spawn()
        .unwrap();
    let mut stdout = BufReader::new(child.stdout.take().unwrap());
    read_until(&mut stdout, WAITING_1);
    // Let the poll pause grow past the first second.
    std::thread::sleep(Duration::from_millis(1500));
    let (rest, elapsed) = terminate(&mut child, &mut stdout);
    assert!(
        elapsed < Duration::from_secs(1),
        "output reached EOF after {elapsed:?}"
    );
    assert_eq!(rest, format!("{STOPPED_IDLE}\n"));
    assert_eq!(fake.calls(), 1);
}

#[test]
fn terminating_during_the_consumer_grace_subscribes_nothing() {
    let fake = Fake::new();
    let mut child = fake
        .launcher(&[
            ("AIRC_DELIVERY_SESSION", "space"),
            ("FAKE_STATUS", "e1+busy"),
            ("FAKE_MODE", "hold"),
        ])
        .spawn()
        .unwrap();
    let mut stdout = BufReader::new(child.stdout.take().unwrap());
    let deadline = Instant::now() + Duration::from_secs(5);
    while fake.polls() == 0 {
        assert!(Instant::now() < deadline, "the launcher never polled");
        std::thread::sleep(Duration::from_millis(20));
    }
    std::thread::sleep(Duration::from_millis(200));
    let (rest, elapsed) = terminate(&mut child, &mut stdout);
    assert!(
        elapsed < Duration::from_secs(1),
        "output reached EOF after {elapsed:?}"
    );
    assert_eq!(rest, format!("{STOPPED_IDLE}\n"));
    assert_eq!(fake.calls(), 0, "no subscribe after termination");
}
