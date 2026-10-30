//! Delivery adapter for `airc subscribe NAME -- airc-codex --thread ID`.
//!
//! Reads one delivered batch from stdin and queues it on an existing Codex
//! thread through the public `codex queue` CLI. Exit 0 means Codex durably
//! queued the message (transport acceptance, not processing). Exit 3 means
//! nothing was sent. Exit 4 means the outcome is uncertain. Never retries.
//! The subscriber must run this binary as its direct child (no wrapper
//! without `exec`), because the parent check compares `AIRC_SUBSCRIBER_PID`.
//!
//! Opt-in `--wait-for-idle` first waits, read-only, until the local Codex app
//! server reports the thread idle (see `idle`), then queues exactly as above.
//! It is meant for `airc subscribe --sink-timeout 0`. A signal while waiting
//! exits 3; while `codex queue` runs, it kills and reaps that child and exits 4.
//!
//! Opt-in `--app-server` starts or steers through the same existing local
//! daemon, with durable per-batch replay protection (see `app_server`). The
//! default and wait-for-idle queue paths remain separate.

#[path = "airc-codex/app_server.rs"]
mod app_server;
#[path = "airc-codex/idle.rs"]
mod idle;

use clap::Parser;
use std::{
    ffi::OsString,
    io::{ErrorKind, Read},
    path::PathBuf,
    process::{Child, Command, ExitCode, ExitStatus, Stdio},
    sync::{
        Arc, Mutex, PoisonError,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

/// Whole stdin payload, header included. Well below Linux `MAX_ARG_STRLEN`
/// because the batch travels as one `codex queue --message` argument.
const MAX_BATCH_BYTES: usize = 64 * 1024;
const HEADER_PREFIX: &str = "[airc id=";
const LEGACY_HEADER_PREFIX: &str = "[airc delivery ";
/// Below the subscriber's default sink timeout so a hung CLI is reported here.
const QUEUE_TIMEOUT: Duration = Duration::from_secs(30);
/// After `codex queue` exits, wait this long for its pipes to reach EOF; a
/// descendant may keep them open, so classification uses what arrived.
const OUTPUT_GRACE: Duration = Duration::from_millis(500);
const CAPTURE_BYTES: usize = 4096;
const NOT_SENT: u8 = 3;
const UNCERTAIN: u8 = 4;
/// Set by a SIGTERM, SIGINT or SIGHUP after `--wait-for-idle` released the
/// batch; never set in the default mode, which installs no handlers.
static CANCELLED: AtomicBool = AtomicBool::new(false);

#[derive(Parser)]
#[command(version, about = "Deliver AIRC batches to an existing Codex thread")]
struct Args {
    /// Codex thread UUID, normally "$CODEX_THREAD_ID" of the session to wake.
    #[arg(long, value_parser = parse_thread)]
    thread: String,
    /// Codex executable; tested with codex-cli 0.163.0.
    #[arg(long, default_value = "codex")]
    codex: PathBuf,
    /// Queue only after the local Codex app server (codex-cli 0.164.0)
    /// reports the thread idle; observed read-only through
    /// $CODEX_HOME/app-server-control. Run under `subscribe --sink-timeout 0`.
    #[arg(long, conflicts_with = "app_server")]
    wait_for_idle: bool,
    /// Opt-in native app-server delivery. The legacy queue path remains default.
    /// Requires the exact verified existing daemon socket and subscriber state.
    #[arg(long, requires = "app_server_socket")]
    app_server: bool,
    /// Absolute existing private socket under `/tmp/codex-daemon-<euid>/`.
    /// No daemon discovery, start, fallback or permission override is attempted.
    #[arg(long, requires = "app_server")]
    app_server_socket: Option<PathBuf>,
    /// Give up waiting for idle after this many seconds, reconnects
    /// included (exit 3, nothing sent). 0 waits until idle, a signal or a
    /// definite refusal.
    #[arg(long, requires = "wait_for_idle", default_value_t = 0)]
    idle_timeout: u64,
}

fn parse_thread(value: &str) -> Result<String, String> {
    uuid::Uuid::parse_str(value)
        .map(|id| id.hyphenated().to_string())
        .map_err(|_| "expected a thread UUID; names and previews are not accepted".into())
}

fn main() -> ExitCode {
    let args = Args::parse();
    let delivery = std::env::var("AIRC_DELIVERY_ID").ok();
    let label = delivery.as_deref().map_or("unknown".into(), printable);
    let subscriber = std::env::var("AIRC_SUBSCRIBER_PID").ok();
    let batch = read_batch(std::io::stdin().lock()).and_then(|batch| {
        check_delivery_id(&batch, delivery.as_deref())?;
        if subscriber.is_none() {
            eprintln!("airc-codex: AIRC_SUBSCRIBER_PID is unset; parent check skipped");
        }
        check_subscriber(subscriber.as_deref())?;
        Ok(batch)
    });
    let batch = match batch {
        Ok(batch) => batch,
        Err(error) => {
            eprintln!("airc-codex: delivery {label} not sent: {error}");
            return ExitCode::from(NOT_SENT);
        }
    };
    if args.app_server {
        let (Some(delivery), Some(_), Some(state_dir), Some(socket)) = (
            delivery.as_deref(),
            subscriber.as_deref(),
            std::env::var_os("AIRC_DELIVERY_STATE_DIR"),
            args.app_server_socket.as_deref(),
        ) else {
            eprintln!(
                "airc-codex: app-server mode requires a direct named subscriber, batch ID and private state directory"
            );
            return ExitCode::from(NOT_SENT);
        };
        return ExitCode::from(app_server::run(
            socket,
            &PathBuf::from(state_dir),
            &args.thread,
            delivery,
            &batch,
            || check_subscriber(subscriber.as_deref()),
        ));
    }
    let endpoint = match args.wait_for_idle.then(idle::Endpoint::local).transpose() {
        Ok(endpoint) => endpoint,
        Err(error) => {
            eprintln!("airc-codex: delivery {label} not sent: {error}");
            return ExitCode::from(NOT_SENT);
        }
    };
    let still_ours = || check_subscriber(subscriber.as_deref());
    ExitCode::from(deliver(
        &args,
        &label,
        batch,
        endpoint.as_ref(),
        still_ours,
        idle::Timing::DEFAULT,
        &CANCELLED,
    ))
}

/// Optionally waits for idle, then runs `codex queue` once and classifies it.
fn deliver(
    args: &Args,
    label: &str,
    batch: String,
    endpoint: Option<&idle::Endpoint>,
    still_ours: impl Fn() -> Result<(), String>,
    timing: idle::Timing,
    cancel: &'static AtomicBool,
) -> u8 {
    // Kept until exit: it owns the signal handlers that cancel the queue.
    let _runtime = match endpoint {
        Some(endpoint) => match wait_for_idle(endpoint, args, timing, still_ours, cancel) {
            Ok(runtime) => {
                eprintln!("airc-codex: delivery {label}: thread is idle; queuing");
                Some(runtime)
            }
            Err(error) => {
                eprintln!("airc-codex: delivery {label} not sent: {error}");
                return NOT_SENT;
            }
        },
        None => None,
    };
    if cancel.load(Ordering::SeqCst) {
        eprintln!("airc-codex: delivery {label} not sent: cancelled by signal");
        return NOT_SENT;
    }
    let child = Command::new(&args.codex)
        .args([
            OsString::from("queue"),
            "--thread".into(),
            args.thread.as_str().into(),
            "--message".into(),
            batch.into(),
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn();
    let child = match child {
        Ok(child) => child,
        Err(error) => {
            eprintln!(
                "airc-codex: delivery {label} not sent: cannot start {}: {error}",
                args.codex.display()
            );
            return NOT_SENT;
        }
    };
    let (status, stdout, stderr) = run_bounded(child, QUEUE_TIMEOUT, cancel);
    match classify(status, &stdout, &args.thread) {
        Ok(receipt) => {
            eprintln!("airc-codex: delivery {label}: {}", printable(receipt));
            0
        }
        Err(reason) => {
            eprintln!(
                "airc-codex: delivery {label} uncertain: codex queue {reason}; inspect the thread queue before retrying"
            );
            for (name, captured) in [("stdout", &stdout), ("stderr", &stderr)] {
                if !captured.bytes.is_empty() {
                    eprintln!("codex queue {name}:\n{}", printable(&captured.text()));
                }
            }
            UNCERTAIN
        }
    }
}

/// Waits until the thread is idle. The returned runtime keeps listening for
/// SIGTERM, SIGINT and SIGHUP and then sets `cancel`; a signal during the
/// wait itself is a refusal.
fn wait_for_idle(
    endpoint: &idle::Endpoint,
    args: &Args,
    timing: idle::Timing,
    still_ours: impl Fn() -> Result<(), String>,
    cancel: &'static AtomicBool,
) -> Result<tokio::runtime::Runtime, String> {
    use tokio::signal::unix::{SignalKind, signal};
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .map_err(|error| format!("cannot start the status client: {error}"))?;
    runtime.block_on(async {
        let handle =
            |kind| signal(kind).map_err(|error| format!("cannot handle signals: {error}"));
        let mut term = handle(SignalKind::terminate())?;
        let mut int = handle(SignalKind::interrupt())?;
        let mut hup = handle(SignalKind::hangup())?;
        let mut cancelled = Box::pin(async move {
            tokio::select! {
                _ = term.recv() => {}
                _ = int.recv() => {}
                _ = hup.recv() => {}
            }
        });
        let waited = tokio::select! {
            waited = idle::wait(endpoint, &args.thread, args.idle_timeout, timing, still_ours) => waited,
            _ = &mut cancelled => return Err("cancelled by signal while waiting for idle".into()),
        };
        waited.map_err(|refusal| match refusal {
            idle::Refusal::Definite(reason) => format!("waiting for idle refused: {reason}"),
            idle::Refusal::Deadline { seconds, last } => {
                format!("thread not idle within --idle-timeout {seconds} s (last: {last})")
            }
        })?;
        tokio::spawn(async move {
            cancelled.await;
            cancel.store(true, Ordering::SeqCst);
        });
        Ok::<(), String>(())
    })?;
    Ok(runtime)
}

/// Accepts only a clean exit whose bounded stdout names exactly this thread.
fn classify<'a>(
    status: Result<ExitStatus, String>,
    stdout: &'a Captured,
    thread: &str,
) -> Result<&'a str, String> {
    let status = status?;
    if !status.success() {
        return Err(format!("ended with {status}"));
    }
    if stdout.overflow {
        return Err(format!(
            "ended with {status}; stdout exceeded {CAPTURE_BYTES} bytes"
        ));
    }
    queued_receipt(std::str::from_utf8(&stdout.bytes).unwrap_or(""), thread)
        .ok_or_else(|| format!("ended with {status} without a matching receipt"))
}

/// A bounded prefix of one output stream as read so far.
#[derive(Clone, Default)]
struct Captured {
    bytes: Vec<u8>,
    overflow: bool,
    eof: bool,
}

impl Captured {
    fn text(&self) -> String {
        String::from_utf8_lossy(&self.bytes).into_owned()
    }
}

/// Waits at most `timeout` for the child (killing and reaping it at once if
/// `cancel` is set), then at most `OUTPUT_GRACE` for its pipes. Keeps `CAPTURE_BYTES` of each stream and drains the rest so the
/// child never blocks on a full pipe.
fn run_bounded(
    mut child: Child,
    timeout: Duration,
    cancel: &AtomicBool,
) -> (Result<ExitStatus, String>, Captured, Captured) {
    let stdout = capture(child.stdout.take());
    let stderr = capture(child.stderr.take());
    let deadline = Instant::now() + timeout;
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break Ok(status),
            Ok(None) if cancel.load(Ordering::SeqCst) => {
                let _ = child.kill();
                let _ = child.wait();
                break Err("was killed when the adapter was cancelled".into());
            }
            Ok(None) if Instant::now() < deadline => thread::sleep(Duration::from_millis(20)),
            Ok(None) => {
                let _ = child.kill();
                let _ = child.wait();
                break Err(format!("timed out after {:?} and was killed", timeout));
            }
            Err(error) => {
                let _ = child.kill();
                let _ = child.wait();
                break Err(format!("could not be awaited ({error}) and was killed"));
            }
        }
    };
    let snapshot = |shared: &Mutex<Captured>| {
        shared
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    };
    let grace = Instant::now() + OUTPUT_GRACE;
    while Instant::now() < grace && !(snapshot(&stdout).eof && snapshot(&stderr).eof) {
        thread::sleep(Duration::from_millis(10));
    }
    (status, snapshot(&stdout), snapshot(&stderr))
}

fn capture(stream: Option<impl Read + Send + 'static>) -> Arc<Mutex<Captured>> {
    let shared = Arc::new(Mutex::new(Captured::default()));
    let Some(mut stream) = stream else {
        shared.lock().unwrap_or_else(PoisonError::into_inner).eof = true;
        return shared;
    };
    let out = Arc::clone(&shared);
    thread::spawn(move || {
        let mut chunk = [0; 1024];
        loop {
            let n = match stream.read(&mut chunk) {
                Ok(0) => break,
                Ok(n) => n,
                Err(error) if error.kind() == ErrorKind::Interrupted => continue,
                Err(_) => break,
            };
            let mut captured = out.lock().unwrap_or_else(PoisonError::into_inner);
            let room = CAPTURE_BYTES - captured.bytes.len();
            captured.overflow |= n > room;
            captured.bytes.extend_from_slice(&chunk[..n.min(room)]);
        }
        out.lock().unwrap_or_else(PoisonError::into_inner).eof = true;
    });
    shared
}

/// Escapes control and bidirectional-format characters so child output and
/// identifiers cannot drive the subscriber's terminal or log viewer.
fn printable(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for c in text.chars() {
        let hidden = matches!(
            c,
            '\u{200B}'..='\u{200F}' | '\u{202A}'..='\u{202E}' | '\u{2066}'..='\u{2069}' | '\u{FEFF}'
        );
        if (c.is_control() && c != '\n') || hidden {
            out.extend(c.escape_unicode());
        } else {
            out.push(c);
        }
    }
    out
}

/// Reads at most one byte beyond the bound so oversize input is refused
/// without buffering the remainder.
fn read_batch(input: impl Read) -> Result<String, String> {
    let mut bytes = Vec::new();
    input
        .take(MAX_BATCH_BYTES as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| format!("cannot read stdin: {error}"))?;
    if bytes.len() > MAX_BATCH_BYTES {
        return Err(format!("batch exceeds {MAX_BATCH_BYTES} bytes"));
    }
    let batch = String::from_utf8(bytes).map_err(|_| "batch is not UTF-8".to_string())?;
    if batch.contains('\0') {
        return Err("batch contains NUL".into());
    }
    if !batch.starts_with(HEADER_PREFIX) && !batch.starts_with(LEGACY_HEADER_PREFIX) {
        return Err("batch does not start with an airc delivery header".into());
    }
    Ok(batch)
}

/// Refuses a batch whose header names a different delivery than the
/// subscriber announced.
fn check_delivery_id(batch: &str, expected: Option<&str>) -> Result<(), String> {
    let Some(expected) = expected else {
        return Ok(());
    };
    let header = batch.lines().next().unwrap_or("");
    match header
        .split_whitespace()
        .find_map(|word| word.strip_prefix("id="))
    {
        Some(id) if id == expected => Ok(()),
        _ => Err("header id does not match AIRC_DELIVERY_ID".into()),
    }
}

/// Refuses to send after the subscriber that spawned this adapter has exited.
fn check_subscriber(expected: Option<&str>) -> Result<(), String> {
    let Some(expected) = expected else {
        return Ok(());
    };
    let expected: u32 = expected
        .parse()
        .map_err(|_| "AIRC_SUBSCRIBER_PID is not a process id".to_string())?;
    let parent = std::os::unix::process::parent_id();
    if parent != expected {
        return Err(format!(
            "subscriber {expected} is gone (parent is {parent})"
        ));
    }
    Ok(())
}

/// Matches codex-cli's `Queued message <row> for thread <id>.` confirmation
/// for exactly this thread.
fn queued_receipt<'a>(stdout: &'a str, thread: &str) -> Option<&'a str> {
    stdout.lines().map(str::trim).find(|line| {
        line.strip_prefix("Queued message ")
            .and_then(|rest| rest.strip_suffix('.'))
            .and_then(|rest| rest.split_once(" for thread "))
            .is_some_and(|(row, id)| !row.is_empty() && !row.contains(' ') && id == thread)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const THREAD: &str = "0199a3b2-6c1d-7e00-8f00-123456789abc";

    fn sh(script: &str) -> Child {
        Command::new("sh")
            .args(["-c", script])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap()
    }

    fn receipt() -> String {
        format!("Queued message 1 for thread {THREAD}.")
    }

    #[test]
    fn batch_bounds_and_shape() {
        for marker in ["[airc id=", "[airc delivery id="] {
            let batch =
                format!("{marker}x session=space]\nm:Ej5FZ-ibQtOkVkJmFBdAAA #space a(b): hi\n");
            assert_eq!(read_batch(batch.as_bytes()).unwrap(), batch);
        }
        let exact = format!(
            "{HEADER_PREFIX}{}",
            "x".repeat(MAX_BATCH_BYTES - HEADER_PREFIX.len())
        );
        assert!(read_batch(exact.as_bytes()).is_ok());
        let over = format!("{exact}y");
        assert!(read_batch(over.as_bytes()).unwrap_err().contains("exceeds"));
        assert!(read_batch(&b""[..]).is_err());
        assert!(read_batch(&b"m:Ej5FZ-ibQtOkVkJmFBdAAA #space a(b): no header"[..]).is_err());
        assert!(read_batch(&b"[airc session=space]\n| [airc id=x]"[..]).is_err());
        assert!(read_batch(&b"[airc \0"[..]).unwrap_err().contains("NUL"));
        assert!(
            read_batch(&b"[airc \xff"[..])
                .unwrap_err()
                .contains("UTF-8")
        );
    }

    #[test]
    fn compact_headers_with_bare_flags_and_zero_counters_keep_the_exact_id_check() {
        for header in [
            "[airc id=6f1c session=space]",
            "[airc id=6f1c session=space events=0 gap]",
            "[airc id=6f1c session=space events=3 context=2 more gap retry]",
        ] {
            let batch = format!(
                "{header}\n! delivery settings: account=a nick=a wake=mentions max-bytes=8192 wrap=off min-interval=0\n"
            );
            assert_eq!(read_batch(batch.as_bytes()).unwrap(), batch);
            assert!(check_delivery_id(&batch, Some("6f1c")).is_ok());
            assert!(check_delivery_id(&batch, Some("6f1")).is_err());
        }
    }

    #[test]
    fn header_id_must_match_the_announced_delivery() {
        for marker in ["[airc ", "[airc delivery "] {
            let batch = format!(
                "{marker}id=6f1c session=space]\nm:Ej5FZ-ibQtOkVkJmFBdAAA #space a(b): id=6f1c\n"
            );
            assert!(check_delivery_id(&batch, None).is_ok());
            assert!(check_delivery_id(&batch, Some("6f1c")).is_ok());
            assert!(check_delivery_id(&batch, Some("6f1d")).is_err());
            let no_id = format!("{marker}session=space]\n| [airc id=6f1c session=space]\n");
            assert!(check_delivery_id(&no_id, Some("6f1c")).is_err());
        }
    }

    #[test]
    fn thread_must_be_a_uuid() {
        assert_eq!(parse_thread(&THREAD.to_uppercase()).unwrap(), THREAD);
        assert!(parse_thread("my session").is_err());
        assert!(parse_thread("").is_err());
    }

    #[test]
    fn subscriber_check() {
        assert!(check_subscriber(None).is_ok());
        let parent = std::os::unix::process::parent_id();
        assert!(check_subscriber(Some(&parent.to_string())).is_ok());
        assert!(check_subscriber(Some(&(parent + 1).to_string())).is_err());
        assert!(check_subscriber(Some("pid")).is_err());
    }

    #[test]
    fn hung_queue_is_killed_at_the_deadline() {
        let started = Instant::now();
        let (status, _, _) = run_bounded(sh("sleep 30"), Duration::from_millis(100), &CANCELLED);
        assert!(status.unwrap_err().contains("timed out"));
        assert!(started.elapsed() < Duration::from_secs(5));
    }

    #[test]
    fn noisy_output_is_drained_and_overflow_is_uncertain() {
        let started = Instant::now();
        let script = format!("echo '{}'; head -c 200000 /dev/zero", receipt());
        let (status, stdout, _) = run_bounded(sh(&script), Duration::from_secs(10), &CANCELLED);
        assert!(stdout.overflow);
        assert_eq!(stdout.bytes.len(), CAPTURE_BYTES);
        let error = classify(status, &stdout, THREAD).unwrap_err();
        assert!(error.contains("exceeded"));
        let script = format!("head -c 200000 /dev/zero >&2; echo '{}'", receipt());
        let (status, stdout, stderr) =
            run_bounded(sh(&script), Duration::from_secs(10), &CANCELLED);
        assert!(stderr.overflow && !stdout.overflow);
        assert_eq!(classify(status, &stdout, THREAD), Ok(receipt().as_str()));
        assert!(started.elapsed() < Duration::from_secs(10));
    }

    #[test]
    fn receipt_counts_when_a_descendant_keeps_stdout_open() {
        let started = Instant::now();
        let script = format!("echo '{}'; sleep 5 &", receipt());
        let (status, stdout, _) = run_bounded(sh(&script), Duration::from_secs(10), &CANCELLED);
        assert!(!stdout.eof);
        assert_eq!(classify(status, &stdout, THREAD), Ok(receipt().as_str()));
        assert!(started.elapsed() < Duration::from_secs(3));
    }

    #[test]
    fn receipt_with_a_failed_exit_is_uncertain() {
        let script = format!("echo '{}'; exit 1", receipt());
        let (status, stdout, _) = run_bounded(sh(&script), Duration::from_secs(10), &CANCELLED);
        let error = classify(status, &stdout, THREAD).unwrap_err();
        assert!(error.contains("ended with"));
    }

    #[test]
    fn receipt_must_name_the_thread() {
        let line = format!("{}\n", receipt());
        assert_eq!(queued_receipt(&line, THREAD), Some(line.trim()));
        let other = "0199a3b2-6c1d-7e00-8f00-000000000000";
        assert_eq!(queued_receipt(&line, other), None);
        let empty_row = format!("Queued message  for thread {THREAD}.");
        assert_eq!(queued_receipt(&empty_row, THREAD), None);
        assert_eq!(queued_receipt("", THREAD), None);
    }

    /// Records each `codex` invocation's argv, NUL-separated, one per line;
    /// `$FAKE_MODE` is replaced by the mode before it is written.
    const FAKE_CODEX: &str = r#"#!/bin/sh
printf '%s\0' "$@" >> "$0.calls"; echo >> "$0.calls"
case "$FAKE_MODE" in
  ok) echo "Queued message 7 for thread $3." ;;
  fail) echo "Error: thread not found" >&2; exit 1 ;;
esac
"#;

    const BATCH: &str =
        "[airc id=6f1c session=space]\nm:Ej5FZ-ibQtOkVkJmFBdAAA #space a(b): @c hi\n";

    struct Flow {
        layout: idle::tests::Layout,
        /// Keeps the mock server running for the test's duration.
        _server: tokio::runtime::Runtime,
        log: idle::tests::Log,
    }

    impl Flow {
        fn new(script: Vec<Vec<idle::tests::Reply>>) -> Self {
            let layout = idle::tests::Layout::new();
            let server = tokio::runtime::Runtime::new().unwrap();
            let log = idle::tests::Log::default();
            {
                let _context = server.enter();
                idle::tests::serve(layout.bind(), script, log.clone());
            }
            Self {
                layout,
                _server: server,
                log,
            }
        }

        fn deliver(&self, mode: &str, idle_timeout: u64) -> u8 {
            let codex = self.layout.dir.path().join(format!("codex-{mode}"));
            let script = FAKE_CODEX.replace("$FAKE_MODE", mode);
            std::fs::write(&codex, script).unwrap();
            std::fs::set_permissions(&codex, std::os::unix::fs::PermissionsExt::from_mode(0o700))
                .unwrap();
            let args = Args {
                thread: THREAD.into(),
                codex,
                wait_for_idle: true,
                app_server: false,
                app_server_socket: None,
                idle_timeout,
            };
            let cancel = Box::leak(Box::new(AtomicBool::new(false)));
            deliver(
                &args,
                "6f1c",
                BATCH.into(),
                Some(&self.layout.endpoint),
                || Ok(()),
                idle::tests::fast(),
                cancel,
            )
        }

        /// Each recorded invocation's argv.
        fn calls(&self, mode: &str) -> Vec<Vec<String>> {
            let path = self.layout.dir.path().join(format!("codex-{mode}.calls"));
            let raw = std::fs::read_to_string(path).unwrap_or_default();
            raw.split_terminator("\0\n")
                .map(|call| call.split('\0').map(str::to_string).collect())
                .collect()
        }

        fn reads(&self) -> usize {
            idle::tests::reads(&self.log)
                .iter()
                .filter(|(_, method)| method == "thread/read")
                .count()
        }
    }

    #[test]
    fn waits_while_active_then_queues_the_identical_batch_once() {
        use idle::tests::Reply::Status;
        let flow = Flow::new(vec![vec![
            Status("active"),
            Status("active"),
            Status("idle"),
        ]]);
        let started = Instant::now();
        assert_eq!(flow.deliver("ok", 0), 0);
        // Two 300 ms rechecks passed before the only queue invocation.
        assert!(started.elapsed() >= Duration::from_millis(600));
        assert_eq!(flow.reads(), 3);
        assert_eq!(
            flow.calls("ok"),
            [["queue", "--thread", THREAD, "--message", BATCH]]
        );
    }

    #[test]
    fn a_turn_starting_after_idle_still_uses_the_queue() {
        let flow = Flow::new(vec![vec![idle::tests::Reply::IdleThenActive]]);
        assert_eq!(flow.deliver("ok", 0), 0);
        assert_eq!(flow.calls("ok").len(), 1);
        assert_eq!(flow.calls("ok")[0][0], "queue");
    }

    #[test]
    fn refusals_and_deadlines_never_start_codex() {
        use idle::tests::Reply::{Raw, Status};
        let not_loaded = r#"{"id":ID,"result":{"thread":{"id":"0199a3b2-6c1d-7e00-8f00-123456789abc","status":{"type":"notLoaded"}}}}"#;
        let flow = Flow::new(vec![vec![Raw(not_loaded)]]);
        assert_eq!(flow.deliver("ok", 0), NOT_SENT);
        assert!(flow.calls("ok").is_empty());
        let flow = Flow::new(vec![vec![Status("active")]]);
        let started = Instant::now();
        assert_eq!(flow.deliver("ok", 1), NOT_SENT);
        assert!(started.elapsed() < Duration::from_secs(3));
        assert!(flow.calls("ok").is_empty());
    }

    #[test]
    fn queue_outcomes_keep_their_classification_after_waiting() {
        let flow = Flow::new(vec![vec![idle::tests::Reply::Status("idle")]]);
        assert_eq!(flow.deliver("fail", 0), UNCERTAIN);
        assert_eq!(flow.calls("fail").len(), 1);
    }

    #[test]
    fn a_cancelled_queue_is_killed_and_reaped() {
        let cancel = Arc::new(AtomicBool::new(false));
        let setter = Arc::clone(&cancel);
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(100));
            setter.store(true, Ordering::SeqCst);
        });
        let started = Instant::now();
        let child = sh("sleep 30");
        let pid = child.id();
        let (status, _, _) = run_bounded(child, Duration::from_secs(20), &cancel);
        assert!(status.unwrap_err().contains("cancelled"));
        assert!(started.elapsed() < Duration::from_secs(5));
        // Reaped, not a zombie: the process no longer exists at all.
        #[cfg(target_os = "linux")]
        assert!(!std::path::Path::new(&format!("/proc/{pid}")).exists());
    }

    #[test]
    fn printable_escapes_controls_and_bidi() {
        assert_eq!(printable("a\nb"), "a\nb");
        assert_eq!(
            printable("\u{1b}[2J\u{202e}x\r"),
            "\\u{1b}[2J\\u{202e}x\\u{d}"
        );
    }
}
