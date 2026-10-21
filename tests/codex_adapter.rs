#![cfg(unix)]

use std::{
    io::Write,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::{Command, Output, Stdio},
};

const THREAD: &str = "0199a3b2-6c1d-7e00-8f00-123456789abc";
const BATCH: &str = "[airc delivery id=6f1c session=space wake=mentions events=1]\n\
@3 #space root(acct) reply=@2: @worker «ünïcode» `$(touch pwned)` \"q\"\n\
| second line; $HOME stays literal\n";

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
fn refuses_before_spawning_codex() {
    let fake = Fake::new();
    let oversize = format!("{BATCH}{}", "x".repeat(64 * 1024));
    for input in [
        oversize.as_bytes(),
        b"@3 #space no header\n",
        b"",
        b"[airc delivery \xff",
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
