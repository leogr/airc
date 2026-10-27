#![cfg(unix)]

use std::{
    io::{BufRead, Write},
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::{Command, Output, Stdio},
};

const THREAD: &str = "0199a3b2-6c1d-7e00-8f00-123456789abc";
/// A compact header: omitted counters and bare flags, then the settings notice.
const BATCH: &str = "[airc id=6f1c session=space context=2 more retry]\n\
! delivery settings: account=worker nick=worker wake=mentions max-bytes=8192 wrap=off min-interval=0\n\
m:Ej5FZ-ibQtOkVkJmFBdAAA #space root(acct) reply=m:ABEiM0RVZneImaq7zN3u_w: @worker «ünïcode» `$(touch pwned)` \"q\"\n\
| second line; $HOME stays literal\n";
/// A batch saved by an earlier release, with every field and the old marker.
const LEGACY: &str = "[airc delivery id=6f1c session=space account=worker nick=worker wake=mentions events=1 context=2 bytes=80 more=yes gap=no retry=yes]\n\
@3 #space root(acct) reply=@2: @worker hi\n";

/// Stand-in for `codex queue`: records argv and answers per FAKE_MODE.
const FAKE_CODEX: &str = r#"#!/bin/sh
printf '%s\0' "$@" > "$FAKE_DIR/argv"
case "$FAKE_MODE" in
  ok) echo "Queued message 7 for thread $3." ;;
  other) echo "Queued message 7 for thread 0199a3b2-6c1d-7e00-8f00-000000000000." ;;
  garbage) echo "queued, probably" ;;
  fail) echo "Error: thread not found" >&2; exit 1 ;;
  noisy) echo "Queued message 7 for thread $3."; head -c 200000 /dev/zero ;;
  noisy_stderr) head -c 200000 /dev/zero >&2; echo "Queued message 7 for thread $3." ;;
  lingering) echo "Queued message 7 for thread $3."; sleep 5 & ;;
  receipt_fail) echo "Queued message 7 for thread $3."; exit 1 ;;
esac
"#;

struct Fake {
    dir: tempfile::TempDir,
}

impl Fake {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let codex = dir.path().join("codex");
        std::fs::write(&codex, FAKE_CODEX).unwrap();
        std::fs::set_permissions(&codex, std::fs::Permissions::from_mode(0o700)).unwrap();
        Self { dir }
    }

    fn codex(&self) -> PathBuf {
        self.dir.path().join("codex")
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

    fn run(&self, mode: &str, thread: &str, stdin: &[u8], env: &[(&str, String)]) -> Output {
        run_adapter(&self.codex(), self.dir.path(), mode, thread, stdin, env)
    }
}

fn run_adapter(
    codex: &Path,
    fake_dir: &Path,
    mode: &str,
    thread: &str,
    stdin: &[u8],
    env: &[(&str, String)],
) -> Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_airc-codex"))
        .args(["--thread", thread, "--codex"])
        .arg(codex)
        .env("FAKE_DIR", fake_dir)
        .env("FAKE_MODE", mode)
        .env("AIRC_DELIVERY_ID", "6f1c")
        .env_remove("AIRC_SUBSCRIBER_PID")
        .envs(env.iter().map(|(key, value)| (*key, value)))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    // The adapter may refuse before reading everything; a broken pipe is fine.
    let _ = child.stdin.take().unwrap().write_all(stdin);
    child.wait_with_output().unwrap()
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

#[test]
fn queues_the_batch_as_one_literal_argument() {
    let fake = Fake::new();
    let output = fake.run("ok", &THREAD.to_uppercase(), BATCH.as_bytes(), &[]);
    assert_eq!(output.status.code(), Some(0), "{}", stderr(&output));
    assert!(output.stdout.is_empty());
    assert!(stderr(&output).contains("delivery 6f1c: Queued message 7 for thread"));
    assert_eq!(
        fake.argv().unwrap(),
        ["queue", "--thread", THREAD, "--message", BATCH]
    );
    assert!(!fake.dir.path().join("pwned").exists());
}

#[test]
fn queues_saved_legacy_batch_without_rewriting_it() {
    let fake = Fake::new();
    // Both saved forms: the old marker, and the short marker with every field
    // that the prefix-only release wrote.
    for batch in [
        LEGACY.to_string(),
        LEGACY.replacen("[airc delivery id=", "[airc id=", 1),
    ] {
        let output = fake.run("ok", THREAD, batch.as_bytes(), &[]);
        assert_eq!(output.status.code(), Some(0), "{}", stderr(&output));
        assert_eq!(
            fake.argv().unwrap(),
            ["queue", "--thread", THREAD, "--message", batch.as_str()]
        );
        std::fs::remove_file(fake.dir.path().join("argv")).unwrap();
        let wrong_id = [("AIRC_DELIVERY_ID", "another-delivery".to_string())];
        let output = fake.run("ok", THREAD, batch.as_bytes(), &wrong_id);
        assert_eq!(output.status.code(), Some(3));
        assert!(stderr(&output).contains("header id does not match"));
        assert!(fake.argv().is_none());
    }
    // Only the header's id counts, never one in the settings notice or a body line.
    let forged = "[airc id=0a0a session=space]\n! delivery settings: id=6f1c\n| [airc id=6f1c]\n";
    let output = fake.run("ok", THREAD, forged.as_bytes(), &[]);
    assert_eq!(output.status.code(), Some(3), "{}", stderr(&output));
    assert!(stderr(&output).contains("header id does not match"));
    assert!(fake.argv().is_none());
}

#[test]
fn refuses_before_spawning_codex() {
    let fake = Fake::new();
    let oversize = format!("{BATCH}{}", "x".repeat(64 * 1024));
    for input in [
        oversize.as_bytes(),
        b"m:Ej5FZ-ibQtOkVkJmFBdAAA #space no header\n",
        b"",
        b"[airc \xff",
    ] {
        let output = fake.run("ok", THREAD, input, &[]);
        assert_eq!(output.status.code(), Some(3), "{}", stderr(&output));
        assert!(stderr(&output).contains("delivery 6f1c not sent"));
    }
    let output = fake.run("ok", "named session", BATCH.as_bytes(), &[]);
    assert_eq!(output.status.code(), Some(2));
    let missing = fake.dir.path().join("missing-codex");
    let output = run_adapter(
        &missing,
        fake.dir.path(),
        "ok",
        THREAD,
        BATCH.as_bytes(),
        &[],
    );
    assert_eq!(output.status.code(), Some(3));
    assert!(stderr(&output).contains("cannot start"));
    assert_eq!(fake.argv(), None);
}

#[test]
fn refuses_after_the_subscriber_is_gone() {
    let fake = Fake::new();
    let gone = [("AIRC_SUBSCRIBER_PID", (std::process::id() + 1).to_string())];
    let output = fake.run("ok", THREAD, BATCH.as_bytes(), &gone);
    assert_eq!(output.status.code(), Some(3));
    assert!(stderr(&output).contains("is gone"));
    assert_eq!(fake.argv(), None);
    let alive = [("AIRC_SUBSCRIBER_PID", std::process::id().to_string())];
    let output = fake.run("ok", THREAD, BATCH.as_bytes(), &alive);
    assert_eq!(output.status.code(), Some(0), "{}", stderr(&output));
}

#[test]
fn unconfirmed_queue_outcomes_are_uncertain() {
    for (mode, detail) in [
        ("fail", "Error: thread not found"),
        ("garbage", "without a matching receipt"),
        ("other", "without a matching receipt"),
        ("noisy", "stdout exceeded 4096 bytes"),
        ("receipt_fail", "ended with exit status: 1"),
    ] {
        let fake = Fake::new();
        let output = fake.run(mode, THREAD, BATCH.as_bytes(), &[]);
        assert_eq!(output.status.code(), Some(4), "{mode}");
        let stderr = stderr(&output);
        assert!(stderr.contains("delivery 6f1c uncertain"), "{stderr}");
        assert!(stderr.contains(detail), "{stderr}");
        assert!(fake.argv().is_some());
    }
}

#[test]
fn noisy_stderr_does_not_block_an_accepted_queue() {
    let fake = Fake::new();
    let output = fake.run("noisy_stderr", THREAD, BATCH.as_bytes(), &[]);
    assert_eq!(output.status.code(), Some(0), "{}", stderr(&output));
    assert!(stderr(&output).len() < 1024);
}

#[test]
fn a_descendant_holding_stdout_does_not_block_acceptance() {
    let fake = Fake::new();
    let started = std::time::Instant::now();
    let output = fake.run("lingering", THREAD, BATCH.as_bytes(), &[]);
    assert_eq!(output.status.code(), Some(0), "{}", stderr(&output));
    assert!(started.elapsed() < std::time::Duration::from_secs(3));
}

/// Starts the adapter in `--wait-for-idle` mode with a private, empty Codex
/// home: its control alias is absent, so it waits and never reaches the
/// shared daemon directory.
fn spawn_waiting(fake: &Fake, extra: &[&str]) -> std::process::Child {
    let home = fake.dir.path().join("codex-home");
    std::fs::create_dir_all(&home).unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_airc-codex"))
        .args(["--thread", THREAD, "--wait-for-idle", "--codex"])
        .arg(fake.codex())
        .args(extra)
        .env("CODEX_HOME", &home)
        .env("FAKE_DIR", fake.dir.path())
        .env("FAKE_MODE", "ok")
        .env("AIRC_DELIVERY_ID", "6f1c")
        .env_remove("AIRC_SUBSCRIBER_PID")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(BATCH.as_bytes())
        .unwrap();
    child
}

#[test]
fn idle_timeout_requires_wait_for_idle() {
    let fake = Fake::new();
    let output = Command::new(env!("CARGO_BIN_EXE_airc-codex"))
        .args(["--thread", THREAD, "--idle-timeout", "5"])
        .stdin(Stdio::null())
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    assert!(fake.argv().is_none());
}

#[test]
fn an_idle_timeout_ends_the_wait_without_sending() {
    let fake = Fake::new();
    let started = std::time::Instant::now();
    let output = spawn_waiting(&fake, &["--idle-timeout", "1"])
        .wait_with_output()
        .unwrap();
    assert_eq!(output.status.code(), Some(3), "{}", stderr(&output));
    let stderr = stderr(&output);
    assert!(
        stderr.contains("delivery 6f1c not sent: thread not idle within --idle-timeout 1 s")
            && stderr.contains("control alias is absent"),
        "{stderr}"
    );
    assert!(started.elapsed() < std::time::Duration::from_secs(5));
    assert!(fake.argv().is_none());
}

#[test]
fn an_unrepresentable_idle_timeout_is_refused_at_once() {
    let fake = Fake::new();
    let started = std::time::Instant::now();
    let output = spawn_waiting(&fake, &["--idle-timeout", &u64::MAX.to_string()])
        .wait_with_output()
        .unwrap();
    assert_eq!(output.status.code(), Some(3), "{}", stderr(&output));
    assert!(
        stderr(&output).contains("delivery 6f1c not sent: waiting for idle refused: --idle-timeout 18446744073709551615 is too large"),
        "{}",
        stderr(&output)
    );
    assert!(started.elapsed() < std::time::Duration::from_secs(5));
    assert!(fake.argv().is_none());
}

#[test]
fn a_signal_while_waiting_sends_nothing() {
    use rustix::process::{Pid, Signal, kill_process};
    let fake = Fake::new();
    for signal in [Signal::TERM, Signal::INT, Signal::HUP] {
        let mut child = spawn_waiting(&fake, &[]);
        // Its handlers are installed before the first retry diagnostic.
        let mut errors = std::io::BufReader::new(child.stderr.take().unwrap());
        let mut seen = String::new();
        while !seen.contains("retrying status discovery") {
            assert_ne!(errors.read_line(&mut seen).unwrap(), 0, "{seen}");
        }
        let pid = Pid::from_raw(child.id() as i32).unwrap();
        kill_process(pid, signal).unwrap();
        let status = child.wait().unwrap();
        std::io::Read::read_to_string(&mut errors, &mut seen).unwrap();
        assert_eq!(status.code(), Some(3), "{seen}");
        assert!(
            seen.contains("cancelled by signal while waiting for idle"),
            "{seen}"
        );
    }
    assert!(fake.argv().is_none());
}
