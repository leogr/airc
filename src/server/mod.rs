mod ban;
mod core;
mod http;

use crate::{
    config::Config,
    protocol::{MAX_LINE, Message, read_line},
    transport::{self, Stream},
};
use anyhow::Result;
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncWriteExt, BufReader},
    net::TcpListener,
    sync::{Semaphore, mpsc, oneshot, watch},
    task::{JoinHandle, JoinSet},
};

pub(super) enum Event {
    Connect {
        id: u64,
        out: mpsc::Sender<Message>,
        kill: watch::Sender<bool>,
    },
    Message(u64, Message),
    Disconnect(u64),
    Tick,
    Artifact {
        account: String,
        channel: String,
        filename: String,
        data: Vec<u8>,
        reply: oneshot::Sender<Result<serde_json::Value>>,
    },
    Download {
        account: String,
        id: String,
        reply: oneshot::Sender<Result<Option<crate::store::Artifact>>>,
    },
    Shutdown,
}

pub struct RunningServer {
    pub address: SocketAddr,
    pub artifact_address: Option<SocketAddr>,
    stop: watch::Sender<bool>,
    task: Option<JoinHandle<Result<()>>>,
}

impl RunningServer {
    /// Observe unexpected listener/actor termination as well as graceful shutdown.
    pub async fn wait(&mut self) -> Result<()> {
        if let Some(task) = self.task.as_mut() {
            let result = task.await;
            self.task = None;
            result??;
        }
        Ok(())
    }

    pub async fn shutdown(mut self) -> Result<()> {
        let _ = self.stop.send(true);
        self.wait().await
    }
}
impl Drop for RunningServer {
    fn drop(&mut self) {
        let _ = self.stop.send(true);
    }
}

pub async fn start(mut config: Config) -> Result<RunningServer> {
    config.validate()?;
    let tls = config.tls.as_ref().map(transport::acceptor).transpose()?;
    let listener = TcpListener::bind(config.listen).await?;
    let address = listener.local_addr()?;
    let http_listener = if let Some(a) = &mut config.artifacts {
        let listener = TcpListener::bind(a.listen).await?;
        if a.listen.port() == 0 {
            a.public_url = format!("http://{}", listener.local_addr()?);
        }
        Some(listener)
    } else {
        None
    };
    let artifact_address = http_listener
        .as_ref()
        .map(TcpListener::local_addr)
        .transpose()?;
    let store = crate::store::Store::open(&config)?;
    let config = Arc::new(config);
    let (tx, mut rx) = mpsc::channel(256);
    let (stop, mut stopping) = watch::channel(false);
    let mut state = core::State::new(config.clone(), store)?;
    let actor = tokio::task::spawn_blocking(move || {
        while let Some(event) = rx.blocking_recv() {
            if matches!(event, Event::Shutdown) {
                break;
            }
            state.handle(event);
        }
        state.shutdown();
    });
    let http_task = http_listener.map(|listener| {
        tokio::spawn(http::serve(
            listener,
            config.clone(),
            tx.clone(),
            stopping.clone(),
        ))
    });
    let task = tokio::spawn(async move {
        let semaphore = Arc::new(Semaphore::new(config.limits.connections));
        let mut clients = JoinSet::new();
        let mut next_id = 0u64;
        let mut tick = tokio::time::interval(Duration::from_secs(10));
        let result: Result<()> = async {
            loop {
                tokio::select! {
                    _ = stopping.changed() => break,
                    _ = tick.tick() => { tx.send(Event::Tick).await?; },
                    Some(_) = clients.join_next(), if !clients.is_empty() => {},
                    accepted = listener.accept() => {
                        let (socket, _) = accepted?;
                        let Ok(permit) = semaphore.clone().try_acquire_owned() else { continue; };
                        socket.set_nodelay(true)?;
                        next_id += 1;
                        let id = next_id;
                        let tx = tx.clone();
                        let tls = tls.clone();
                        let limit = config.limits.outbound_queue;
                        let mut stopping = stopping.clone();
                        clients.spawn(async move {
                            let _permit = permit;
                            let connect = async {
                                let stream: Stream = if let Some(tls) = tls { Box::new(tls.accept(socket).await?) } else { Box::new(socket) };
                                Ok::<_,anyhow::Error>(stream)
                            };
                            let stream = tokio::select! {
                                _ = stopping.changed() => return,
                                result = tokio::time::timeout(Duration::from_secs(10), connect) => match result { Ok(Ok(s)) => s, _ => return },
                            };
                            let _ = session(id, stream, tx, limit, stopping).await;
                        });
                    }
                }
            }
            Ok(())
        }.await;
        clients.abort_all();
        while clients.join_next().await.is_some() {}
        let _ = tx.send(Event::Shutdown).await;
        actor.await?;
        if let Some(task) = http_task {
            task.abort();
            let _ = task.await;
        }
        result
    });
    Ok(RunningServer {
        address,
        artifact_address,
        stop,
        task: Some(task),
    })
}

async fn session(
    id: u64,
    stream: Stream,
    tx: mpsc::Sender<Event>,
    limit: usize,
    mut stopping: watch::Receiver<bool>,
) -> Result<()> {
    let (out, mut output) = mpsc::channel::<Message>(limit);
    let (kill, mut killed) = watch::channel(false);
    tx.send(Event::Connect { id, out, kill }).await?;
    let (reader, mut writer) = tokio::io::split(stream);
    let mut reader = BufReader::new(reader);
    // Keep reads in their own future: cancelling read_line after consuming partial
    // data would lose a fragmented message when output becomes ready.
    let input = async {
        while let Some(line) = read_line(&mut reader, MAX_LINE).await? {
            if line.trim_matches(['\r', '\n', ' ']).is_empty() {
                tokio::time::sleep(Duration::from_millis(10)).await;
                continue;
            }
            let message = Message::parse(&line)?;
            anyhow::ensure!(message.prefix.is_none(), "client prefix forbidden");
            tx.send(Event::Message(id, message)).await?;
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok::<_, anyhow::Error>(())
    };
    let output = async {
        while let Some(message) = output.recv().await {
            let line = message.encode()?;
            tokio::time::timeout(Duration::from_secs(10), writer.write_all(line.as_bytes()))
                .await??;
        }
        Ok::<_, anyhow::Error>(())
    };
    let result = tokio::select! { result = input => result, result = output => result, _ = killed.changed() => Ok(()), _ = stopping.changed() => Ok(()) };
    let _ = tx.send(Event::Disconnect(id)).await;
    result
}
