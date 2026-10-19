//! Server-only control socket: one bounded JSONL request per connection.
//! It reports applied state and reloads membership; it never carries tokens.
//! Replies stay under 1 MiB: status lists connected accounts only and reload
//! caps per-channel kick details while reporting exact counts.
use super::{ControlOp, Event, OwnedSocket};
use crate::{config::Config, protocol::read_line};
use anyhow::Result;
use serde::Deserialize;
use serde_json::{Value, json};
use std::{path::PathBuf, time::Duration};
use tokio::{
    io::{AsyncWriteExt, BufReader},
    net::{UnixListener, UnixStream},
    sync::{mpsc, oneshot, watch},
    task::JoinSet,
};

const MAX_REQUEST: usize = 16 * 1024;
const MAX_REPLY: usize = 1024 * 1024;

#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
enum Request {
    Status,
    Reload,
}

pub(super) async fn serve(
    listener: UnixListener,
    _owned: OwnedSocket,
    source: Option<PathBuf>,
    tx: mpsc::Sender<Event>,
    mut stop: watch::Receiver<bool>,
) {
    let mut requests = JoinSet::new();
    loop {
        tokio::select! {
            _ = stop.changed() => break,
            Some(_) = requests.join_next(), if !requests.is_empty() => {}
            accepted = listener.accept(), if requests.len() < 4 => {
                let Ok((stream, _)) = accepted else { continue };
                let (source, tx) = (source.clone(), tx.clone());
                requests.spawn(async move {
                    let _ = tokio::time::timeout(
                        Duration::from_secs(10),
                        handle(stream, source, tx),
                    )
                    .await;
                });
            }
        }
    }
    requests.abort_all();
}

async fn handle(
    stream: UnixStream,
    source: Option<PathBuf>,
    tx: mpsc::Sender<Event>,
) -> Result<()> {
    let mut stream = BufReader::new(stream);
    let reply = match read_line(&mut stream, MAX_REQUEST).await {
        Ok(Some(line)) => match serde_json::from_str::<Request>(&line) {
            Ok(request) => {
                let op = match request {
                    Request::Status => ControlOp::Status,
                    Request::Reload => ControlOp::Reload(match &source {
                        Some(path) => Config::load_with_digest(path)
                            .map(Box::new)
                            .map_err(|e| format!("{e:#}")),
                        None => Err("server was not started from a configuration file".into()),
                    }),
                };
                let (reply, response) = oneshot::channel();
                tx.send(Event::Control { op, reply }).await?;
                response.await?
            }
            Err(_) => failure(
                "INVALID_REQUEST",
                "expected {\"op\":\"status\"} or {\"op\":\"reload\"}",
            ),
        },
        _ => failure("INVALID_REQUEST", "request exceeds 16 KiB or is incomplete"),
    };
    let mut line = serde_json::to_vec(&reply)?;
    // The actor bounds every reply; this cap only guards that invariant.
    anyhow::ensure!(line.len() < MAX_REPLY, "control reply exceeds its bound");
    line.push(b'\n');
    stream.get_mut().write_all(&line).await?;
    stream.get_mut().shutdown().await?;
    Ok(())
}

fn failure(code: &str, error: &str) -> Value {
    json!({"ok": false, "code": code, "error": error})
}
