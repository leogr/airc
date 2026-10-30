use super::*;
use std::{
    fs,
    os::unix::fs::{PermissionsExt, symlink},
    sync::{Arc, Mutex},
};
use tempfile::TempDir;
use tokio::net::UnixListener;
use tokio_tungstenite::accept_async;

const THREAD: &str = "0199a3b2-6c1d-7e00-8f00-000000000001";
const OTHER: &str = "0199a3b2-6c1d-7e00-8f00-000000000002";
const TURN: &str = "0199a3b2-6c1d-7e00-8f00-000000000003";
const NEXT: &str = "0199a3b2-6c1d-7e00-8f00-000000000004";
const BATCH: &str = "0199a3b2-6c1d-7e00-8f00-000000000005";
const BODY: &str = "[airc id=0199a3b2-6c1d-7e00-8f00-000000000005 session=trial]\nm:Ej5FZ-ibQtOkVkJmFBdAAA #trial human(account): @codex use the corrected patch\n";

#[derive(Clone)]
enum Reply {
    Ok(Value),
    Error(i64, &'static str),
    Drop,
    Hang,
    CorruptJournal(Value),
    Raw(&'static str),
    ReplaceSocket(Value),
}
struct Step {
    method: &'static str,
    reply: Reply,
}
fn ok(method: &'static str, result: Value) -> Step {
    Step {
        method,
        reply: Reply::Ok(result),
    }
}
fn status(id: &str, status: &str) -> Value {
    json!({"thread":{"id":id,"status":{"type":status}}})
}
fn turn_page(status: &str, id: &str, cursor: Value) -> Value {
    json!({"data":[{"id":id,"status":status}],"nextCursor":cursor})
}
fn started(id: &str) -> Value {
    json!({"turn":{"id":id,"status":"inProgress"}})
}

struct Fixture {
    dir: TempDir,
    state: TempDir,
    path: PathBuf,
    identity: Identity,
    uid: u32,
}
impl Fixture {
    fn new() -> (Self, UnixListener) {
        // Both paths remain below the macOS Unix socket limit.
        let dir = tempfile::Builder::new()
            .prefix("cx-")
            .tempdir_in("/tmp")
            .unwrap();
        let state = tempfile::Builder::new()
            .prefix("cq-")
            .tempdir_in("/tmp")
            .unwrap();
        fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o700)).unwrap();
        fs::set_permissions(state.path(), fs::Permissions::from_mode(0o700)).unwrap();
        let path = dir.path().join("a".repeat(64));
        let listener = UnixListener::bind(&path).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        let uid = rustix::process::geteuid().as_raw();
        let identity = Identity::inspect(&path, dir.path(), uid).unwrap();
        (
            Self {
                dir,
                state,
                path,
                identity,
                uid,
            },
            listener,
        )
    }
    fn journal(&self) -> Journal {
        Journal::open(self.state.path()).unwrap()
    }
}

type Log = Arc<Mutex<Vec<Value>>>;
fn serve(
    f: &Fixture,
    listener: UnixListener,
    steps: Vec<Step>,
    log: Log,
) -> tokio::task::JoinHandle<()> {
    let record = f
        .state
        .path()
        .join("codex-app-server")
        .join(format!("{BATCH}.json"));
    let path = f.path.clone();
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut ws = accept_async(stream).await.unwrap();
        let first = ws.next().await.unwrap().unwrap().into_text().unwrap();
        let first: Value = serde_json::from_str(&first).unwrap();
        assert_eq!(first["method"], "initialize");
        assert_eq!(first["params"]["capabilities"]["experimentalApi"], true);
        ws.send(Message::text(
            json!({"id":first["id"],"result":{"userAgent":"fixture"}}).to_string(),
        ))
        .await
        .unwrap();
        let initialized = ws.next().await.unwrap().unwrap().into_text().unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&initialized).unwrap()["method"],
            "initialized"
        );
        let mut replacement = None;
        for step in steps {
            let message = ws
                .next()
                .await
                .expect("client must issue every scripted request")
                .expect("valid client frame");
            let value: Value = serde_json::from_str(message.into_text().unwrap().as_str()).unwrap();
            assert_eq!(value["method"], step.method);
            assert_eq!(value["params"]["threadId"], THREAD);
            if matches!(step.method, "turn/start" | "turn/steer") {
                // The exact persisted intent must precede the mutation frame.
                let saved: Value = serde_json::from_slice(&fs::read(&record).unwrap()).unwrap();
                assert_eq!(saved["phase"], "in_flight");
                assert_eq!(saved["rpc_id"], value["id"]);
                assert_eq!(saved["method"], step.method);
                assert_eq!(
                    value["params"]["input"],
                    json!([{"type":"text","text":BODY}])
                );
                assert!(value["params"].get("model").is_none());
                assert!(value["params"].get("approvalPolicy").is_none());
                assert!(value["params"].get("sandboxPolicy").is_none());
            }
            log.lock().unwrap().push(value.clone());
            let reply = match step.reply {
                Reply::Ok(result) => json!({"id":value["id"],"result":result}),
                Reply::Error(code, message) => {
                    json!({"id":value["id"],"error":{"code":code,"message":message}})
                }
                Reply::Drop => return,
                Reply::Hang => {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    return;
                }
                Reply::CorruptJournal(result) => {
                    fs::write(&record, b"broken after effect").unwrap();
                    json!({"id":value["id"],"result":result})
                }
                Reply::Raw(raw) => {
                    let _ = ws.send(Message::text(raw)).await;
                    return;
                }
                Reply::ReplaceSocket(result) => {
                    fs::remove_file(&path).unwrap();
                    replacement = Some(UnixListener::bind(&path).unwrap());
                    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
                    json!({"id":value["id"],"result":result})
                }
            };
            if ws.send(Message::text(reply.to_string())).await.is_err() {
                return;
            }
        }
        drop(replacement);
    })
}

async fn flow(f: &Fixture, journal: &Journal, parent_ok: bool) -> Outcome {
    deliver(Delivery {
        journal,
        identity: &f.identity,
        daemon_dir: f.dir.path(),
        thread: THREAD,
        batch_id: BATCH,
        batch: BODY,
        uid: f.uid,
        inflight: &AtomicBool::new(false),
        still_ours: &|| {
            if parent_ok {
                Ok(())
            } else {
                Err("subscriber gone".into())
            }
        },
        timeout: Duration::from_millis(500),
    })
    .await
}
fn effects(log: &Log) -> Vec<String> {
    log.lock()
        .unwrap()
        .iter()
        .filter_map(|v| {
            v["method"]
                .as_str()
                .filter(|s| matches!(*s, "turn/start" | "turn/steer"))
                .map(str::to_string)
        })
        .collect()
}

#[tokio::test]
async fn idle_is_started_once_and_durable_receipt_is_not_resent() {
    let (f, listener) = Fixture::new();
    let journal = f.journal();
    let log = Log::default();
    let task = serve(
        &f,
        listener,
        vec![
            ok("thread/read", status(THREAD, "idle")),
            ok("turn/start", started(TURN)),
        ],
        log.clone(),
    );
    assert!(matches!(flow(&f,&journal,true).await, Outcome::Accepted(id,"turn/start") if id==TURN));
    task.await.unwrap();
    assert_eq!(journal.load(BATCH).unwrap().unwrap().phase, Phase::Accepted);
    drop(journal);
    // Replay settlement uses the stored receipt, not even a socket connection.
    assert_eq!(
        run(
            Path::new("/not-an-endpoint"),
            f.state.path(),
            THREAD,
            BATCH,
            "retry rendering differs",
            || Ok(())
        ),
        0
    );
    assert_eq!(effects(&log), vec!["turn/start"]);
}

#[tokio::test]
async fn active_id_is_found_through_pages_and_steered_with_expected_identity() {
    let (f, listener) = Fixture::new();
    let journal = f.journal();
    let log = Log::default();
    let task = serve(
        &f,
        listener,
        vec![
            ok("thread/read", status(THREAD, "active")),
            ok(
                "thread/turns/list",
                turn_page("completed", OTHER, json!("older")),
            ),
            ok(
                "thread/turns/list",
                turn_page("inProgress", TURN, Value::Null),
            ),
            ok("turn/steer", json!({"turnId":TURN})),
        ],
        log.clone(),
    );
    assert!(matches!(flow(&f,&journal,true).await,Outcome::Accepted(id,"turn/steer") if id==TURN));
    task.await.unwrap();
    let recorded = log.lock().unwrap();
    assert_eq!(recorded[2]["params"]["cursor"], "older");
    assert_eq!(recorded[3]["params"]["expectedTurnId"], TURN);
}

#[tokio::test]
async fn idle_becoming_busy_accepts_same_active_turn_without_a_second_submission() {
    let (f, listener) = Fixture::new();
    let journal = f.journal();
    let log = Log::default();
    let task = serve(
        &f,
        listener,
        vec![
            ok("thread/read", status(THREAD, "idle")),
            ok("turn/start", started(TURN)),
        ],
        log.clone(),
    );
    assert!(matches!(flow(&f,&journal,true).await,Outcome::Accepted(id,_) if id==TURN));
    task.await.unwrap();
    assert_eq!(effects(&log).len(), 1);
}

#[tokio::test]
async fn definite_completion_race_reclassifies_once() {
    let (f, listener) = Fixture::new();
    let journal = f.journal();
    let log = Log::default();
    let task = serve(
        &f,
        listener,
        vec![
            ok("thread/read", status(THREAD, "active")),
            ok(
                "thread/turns/list",
                turn_page("inProgress", TURN, Value::Null),
            ),
            Step {
                method: "turn/steer",
                reply: Reply::Error(-32600, "no active turn to steer"),
            },
            ok("thread/read", status(THREAD, "idle")),
            ok("turn/start", started(NEXT)),
        ],
        log.clone(),
    );
    assert!(matches!(flow(&f,&journal,true).await,Outcome::Accepted(id,"turn/start") if id==NEXT));
    task.await.unwrap();
    assert_eq!(effects(&log), vec!["turn/steer", "turn/start"]);
}

#[tokio::test]
async fn twice_changing_turns_stop_without_a_third_submission() {
    let (f, listener) = Fixture::new();
    let journal = f.journal();
    let log = Log::default();
    let mut steps = Vec::new();
    for id in [TURN, NEXT] {
        steps.extend([
            ok("thread/read", status(THREAD, "active")),
            ok(
                "thread/turns/list",
                turn_page("inProgress", id, Value::Null),
            ),
            Step {
                method: "turn/steer",
                reply: Reply::Error(-32600, "no active turn to steer"),
            },
        ]);
    }
    let task = serve(&f, listener, steps, log.clone());
    assert!(matches!(
        flow(&f, &journal, true).await,
        Outcome::NotSent(_)
    ));
    task.await.unwrap();
    assert_eq!(journal.load(BATCH).unwrap().unwrap().phase, Phase::NotSent);
    assert_eq!(effects(&log).len(), 2);
}

#[tokio::test]
async fn lost_reply_remains_inflight_and_restart_does_not_connect_or_resend() {
    let (f, listener) = Fixture::new();
    let journal = f.journal();
    let log = Log::default();
    let task = serve(
        &f,
        listener,
        vec![
            ok("thread/read", status(THREAD, "idle")),
            Step {
                method: "turn/start",
                reply: Reply::Drop,
            },
        ],
        log.clone(),
    );
    assert!(matches!(
        flow(&f, &journal, true).await,
        Outcome::Uncertain(_)
    ));
    task.await.unwrap();
    assert_eq!(journal.load(BATCH).unwrap().unwrap().phase, Phase::InFlight);
    drop(journal);
    assert_eq!(
        run(
            Path::new("/not-an-endpoint"),
            f.state.path(),
            THREAD,
            BATCH,
            BODY,
            || Ok(())
        ),
        super::super::UNCERTAIN
    );
    assert_eq!(effects(&log), vec!["turn/start"]);
}

#[tokio::test]
async fn replacement_turn_rejection_is_conservative_and_has_no_fallback() {
    let (f, listener) = Fixture::new();
    let journal = f.journal();
    let log = Log::default();
    let task = serve(
        &f,
        listener,
        vec![
            ok("thread/read", status(THREAD, "active")),
            ok(
                "thread/turns/list",
                turn_page("inProgress", TURN, Value::Null),
            ),
            Step {
                method: "turn/steer",
                reply: Reply::Error(-32600, "expectedTurnId does not match active turn"),
            },
        ],
        log.clone(),
    );
    assert!(matches!(
        flow(&f, &journal, true).await,
        Outcome::Uncertain(_)
    ));
    task.await.unwrap();
    assert_eq!(effects(&log), vec!["turn/steer"]);
}

#[tokio::test]
async fn incorrect_steer_receipt_remains_unknown() {
    let (f, listener) = Fixture::new();
    let journal = f.journal();
    let log = Log::default();
    let task = serve(
        &f,
        listener,
        vec![
            ok("thread/read", status(THREAD, "active")),
            ok(
                "thread/turns/list",
                turn_page("inProgress", TURN, Value::Null),
            ),
            ok("turn/steer", json!({"turnId":OTHER})),
        ],
        log.clone(),
    );
    assert!(matches!(
        flow(&f, &journal, true).await,
        Outcome::Uncertain(_)
    ));
    task.await.unwrap();
    assert_eq!(journal.load(BATCH).unwrap().unwrap().phase, Phase::InFlight);
}

#[tokio::test]
async fn malformed_reply_after_submission_is_never_called_not_started() {
    let (f, listener) = Fixture::new();
    let journal = f.journal();
    let log = Log::default();
    let task = serve(
        &f,
        listener,
        vec![
            ok("thread/read", status(THREAD, "idle")),
            Step {
                method: "turn/start",
                reply: Reply::Raw("not JSON"),
            },
        ],
        log.clone(),
    );
    assert!(matches!(
        flow(&f, &journal, true).await,
        Outcome::Uncertain(_)
    ));
    task.await.unwrap();
    assert_eq!(effects(&log).len(), 1);
}

#[tokio::test]
async fn wrong_thread_unloaded_or_active_without_id_never_receive_effects() {
    for answer in [
        status(OTHER, "idle"),
        status(THREAD, "notLoaded"),
        status(THREAD, "systemError"),
    ] {
        let (f, listener) = Fixture::new();
        let journal = f.journal();
        let log = Log::default();
        let task = serve(&f, listener, vec![ok("thread/read", answer)], log.clone());
        assert!(matches!(
            flow(&f, &journal, true).await,
            Outcome::NotSent(_)
        ));
        task.await.unwrap();
        assert!(effects(&log).is_empty());
        assert!(journal.load(BATCH).unwrap().is_none());
    }
    let (f, listener) = Fixture::new();
    let journal = f.journal();
    let log = Log::default();
    let task = serve(
        &f,
        listener,
        vec![
            ok("thread/read", status(THREAD, "active")),
            ok(
                "thread/turns/list",
                turn_page("completed", TURN, Value::Null),
            ),
        ],
        log.clone(),
    );
    assert!(matches!(
        flow(&f, &journal, true).await,
        Outcome::NotSent(_)
    ));
    task.await.unwrap();
    assert!(effects(&log).is_empty());
}

#[tokio::test]
async fn subscriber_loss_capacity_and_socket_replacement_block_before_effect() {
    for capacity in [false, true] {
        let (f, listener) = Fixture::new();
        let mut journal = f.journal();
        if capacity {
            journal.set_test_capacity(0);
        }
        let log = Log::default();
        let task = serve(
            &f,
            listener,
            vec![ok("thread/read", status(THREAD, "idle"))],
            log.clone(),
        );
        assert!(matches!(
            flow(&f, &journal, capacity).await,
            Outcome::NotSent(_)
        ));
        task.await.unwrap();
        assert!(effects(&log).is_empty());
    }
    let (f, listener) = Fixture::new();
    let journal = f.journal();
    let log = Log::default();
    let task = serve(
        &f,
        listener,
        vec![Step {
            method: "thread/read",
            reply: Reply::ReplaceSocket(status(THREAD, "idle")),
        }],
        log.clone(),
    );
    assert!(matches!(
        flow(&f, &journal, true).await,
        Outcome::NotSent(_)
    ));
    task.await.unwrap();
    assert!(effects(&log).is_empty());
}

#[tokio::test]
async fn endpoint_rejects_symlinks_foreign_owner_and_nonprivate_directories() {
    let (f, _listener) = Fixture::new();
    assert!(Identity::inspect(&f.path, f.dir.path(), f.uid + 1).is_err());
    let alias = f.dir.path().join("b".repeat(64));
    symlink(&f.path, &alias).unwrap();
    assert!(Identity::inspect(&alias, f.dir.path(), f.uid).is_err());
    fs::set_permissions(f.dir.path(), fs::Permissions::from_mode(0o755)).unwrap();
    assert!(Identity::inspect(&f.path, f.dir.path(), f.uid).is_err());
}

#[test]
fn journal_lock_corruption_and_symlinks_never_reset_replay_protection() {
    let state = tempfile::Builder::new()
        .prefix("cq-")
        .tempdir_in("/tmp")
        .unwrap();
    fs::set_permissions(state.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let journal = Journal::open(state.path()).unwrap();
    assert!(Journal::open(state.path()).is_err());
    let record = state
        .path()
        .join("codex-app-server")
        .join(format!("{BATCH}.json"));
    fs::write(&record, b"broken").unwrap();
    fs::set_permissions(&record, fs::Permissions::from_mode(0o600)).unwrap();
    assert!(journal.load(BATCH).is_err());
    fs::remove_file(&record).unwrap();
    let outside = state.path().join("outside");
    fs::write(&outside, b"unchanged").unwrap();
    symlink(&outside, &record).unwrap();
    assert!(journal.load(BATCH).is_err());
    assert_eq!(fs::read(outside).unwrap(), b"unchanged");
}

#[test]
fn legacy_default_and_new_mode_flags_are_separate() {
    use clap::Parser;
    let legacy = super::super::Args::try_parse_from(["airc-codex", "--thread", THREAD]).unwrap();
    assert!(!legacy.app_server && !legacy.wait_for_idle);
    assert!(
        super::super::Args::try_parse_from(["airc-codex", "--thread", THREAD, "--app-server"])
            .is_err()
    );
    assert!(
        super::super::Args::try_parse_from([
            "airc-codex",
            "--thread",
            THREAD,
            "--app-server-socket",
            "/tmp/socket"
        ])
        .is_err()
    );
    assert!(
        super::super::Args::try_parse_from([
            "airc-codex",
            "--thread",
            THREAD,
            "--app-server",
            "--app-server-socket",
            "/tmp/socket",
            "--wait-for-idle"
        ])
        .is_err()
    );
}

#[tokio::test]
async fn timeout_and_receipt_storage_failure_keep_submission_unknown() {
    for reply in [Reply::Hang, Reply::CorruptJournal(started(TURN))] {
        let (f, listener) = Fixture::new();
        let journal = f.journal();
        let log = Log::default();
        let task = serve(
            &f,
            listener,
            vec![
                ok("thread/read", status(THREAD, "idle")),
                Step {
                    method: "turn/start",
                    reply,
                },
            ],
            log.clone(),
        );
        assert!(matches!(
            flow(&f, &journal, true).await,
            Outcome::Uncertain(_)
        ));
        task.await.unwrap();
        drop(journal);
        assert_eq!(
            run(
                Path::new("/not-an-endpoint"),
                f.state.path(),
                THREAD,
                BATCH,
                BODY,
                || Ok(())
            ),
            super::super::UNCERTAIN
        );
        assert_eq!(effects(&log), vec!["turn/start"]);
    }
}

#[tokio::test]
async fn journal_exclusivity_covers_the_whole_submission() {
    let (f, listener) = Fixture::new();
    let journal = f.journal();
    let log = Log::default();
    let task = serve(
        &f,
        listener,
        vec![
            ok("thread/read", status(THREAD, "idle")),
            Step {
                method: "turn/start",
                reply: Reply::Hang,
            },
        ],
        log.clone(),
    );
    let delivery = flow(&f, &journal, true);
    tokio::pin!(delivery);
    tokio::select! {
        _= &mut delivery => panic!("must still be waiting for the receipt"),
        _=tokio::time::sleep(Duration::from_millis(50)) => {},
    }
    assert!(Journal::open(f.state.path()).is_err());
    assert!(matches!(delivery.await, Outcome::Uncertain(_)));
    task.await.unwrap();
    assert_eq!(effects(&log), vec!["turn/start"]);
}

#[tokio::test]
async fn bounded_turn_history_refuses_repeated_ids_and_unsupported_states() {
    for page in [
        turn_page("futureState", TURN, Value::Null),
        json!({"data":[{"id":TURN,"status":"inProgress"},{"id":NEXT,"status":"inProgress"}],"nextCursor":null}),
    ] {
        let (f, listener) = Fixture::new();
        let journal = f.journal();
        let log = Log::default();
        let task = serve(
            &f,
            listener,
            vec![
                ok("thread/read", status(THREAD, "active")),
                ok("thread/turns/list", page),
            ],
            log.clone(),
        );
        assert!(matches!(
            flow(&f, &journal, true).await,
            Outcome::NotSent(_)
        ));
        task.await.unwrap();
        assert!(effects(&log).is_empty());
    }
    let (f, listener) = Fixture::new();
    let journal = f.journal();
    let log = Log::default();
    let task = serve(
        &f,
        listener,
        vec![
            ok("thread/read", status(THREAD, "active")),
            ok(
                "thread/turns/list",
                turn_page("completed", TURN, json!("next")),
            ),
            ok(
                "thread/turns/list",
                turn_page("inProgress", TURN, Value::Null),
            ),
        ],
        log.clone(),
    );
    assert!(matches!(
        flow(&f, &journal, true).await,
        Outcome::NotSent(_)
    ));
    task.await.unwrap();
    assert!(effects(&log).is_empty());
}

#[test]
fn unexpected_fifo_record_is_refused_without_waiting_for_a_writer() {
    let state = tempfile::tempdir().unwrap();
    fs::set_permissions(state.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let journal = Journal::open(state.path()).unwrap();
    let record = state
        .path()
        .join("codex-app-server")
        .join(format!("{BATCH}.json"));
    rustix::fs::mkfifoat(
        rustix::fs::CWD,
        &record,
        rustix::fs::Mode::from_raw_mode(0o600),
    )
    .unwrap();
    let (sent, received) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || sent.send(journal.load(BATCH).is_err()).unwrap());
    assert!(
        received
            .recv_timeout(Duration::from_secs(1))
            .expect("FIFO read must not block")
    );
    worker.join().unwrap();
}
