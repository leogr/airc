mod ban;
#[cfg(unix)]
mod control;
mod core;
mod http;

use crate::{
    config::Config,
    protocol::{MAX_LINE, Message, read_line},
    transport::{self, Stream},
};
use anyhow::{Context, Result};
use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncWriteExt, BufReader},
    net::TcpListener,
    sync::{Semaphore, mpsc, oneshot, watch},
    task::{JoinHandle, JoinSet},
};

pub(super) enum Event {
    /// `out` carries pre-encoded frame groups; `queued` counts their unwritten bytes.
    Connect {
        id: u64,
        out: mpsc::Sender<String>,
        queued: Arc<AtomicUsize>,
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
    Control {
        op: ControlOp,
        reply: oneshot::Sender<serde_json::Value>,
    },
    Shutdown,
}

/// A control-socket operation. Reload carries the configuration re-read from
/// the server's own file, with the SHA-256 of exactly those bytes.
pub(super) enum ControlOp {
    Status,
    Reload(std::result::Result<Box<(Config, String)>, String>),
}

pub struct RunningServer {
    /// The TCP IRC listener, or `None` in Unix mode.
    pub address: Option<SocketAddr>,
    pub unix: Option<PathBuf>,
    pub artifact_address: Option<SocketAddr>,
    pub artifact_unix: Option<PathBuf>,
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

/// One IRC or artifact listener. Unix sockets are never paired with TCP.
enum Listener {
    Tcp(TcpListener),
    #[cfg(unix)]
    Unix(tokio::net::UnixListener, OwnedSocket),
}

impl Listener {
    async fn bind(tcp: Option<SocketAddr>, unix: Option<&Path>) -> Result<Self> {
        match (tcp, unix) {
            (Some(address), None) => Ok(Self::Tcp(TcpListener::bind(address).await?)),
            #[cfg(unix)]
            (None, Some(path)) => {
                let (listener, owned) = bind_unix(path)?;
                Ok(Self::Unix(listener, owned))
            }
            _ => anyhow::bail!("listener requires one TCP address or Unix path"),
        }
    }
    fn tcp(&self) -> Option<SocketAddr> {
        match self {
            Self::Tcp(listener) => listener.local_addr().ok(),
            #[cfg(unix)]
            Self::Unix(..) => None,
        }
    }
    fn unix(&self) -> Option<PathBuf> {
        match self {
            Self::Tcp(_) => None,
            #[cfg(unix)]
            Self::Unix(_, owned) => Some(owned.path.clone()),
        }
    }
}

/// Unlinks its socket on drop only while the path still names the same inode,
/// so a replacement server's socket is never removed.
#[cfg(unix)]
pub(super) struct OwnedSocket {
    path: PathBuf,
    identity: (u64, u64),
    /// Held for the listener's lifetime; serializes servers sharing the path.
    _lock: std::fs::File,
}
#[cfg(unix)]
impl Drop for OwnedSocket {
    fn drop(&mut self) {
        use std::os::unix::fs::MetadataExt;
        if std::fs::symlink_metadata(&self.path).is_ok_and(|m| (m.dev(), m.ino()) == self.identity)
        {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

/// Bind a 0600 socket. A stable lock beside the socket serializes servers that
/// share the path. An existing path is removed only if it is a socket refusing
/// connections; publication is a no-replace hard link, so a live server, file
/// or symlink appearing concurrently is never replaced.
#[cfg(unix)]
pub(super) fn bind_unix(path: &Path) -> Result<(tokio::net::UnixListener, OwnedSocket)> {
    use std::os::unix::fs::{FileTypeExt, MetadataExt, OpenOptionsExt, PermissionsExt};
    let name = path
        .file_name()
        .context("socket path has no file name")?
        .to_string_lossy();
    let lock_path = path.with_file_name(format!(".{name}.lock"));
    anyhow::ensure!(
        !std::fs::symlink_metadata(&lock_path).is_ok_and(|m| !m.file_type().is_file()),
        "{} must be a regular file",
        lock_path.display()
    );
    let lock = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .open(&lock_path)?;
    lock.try_lock()
        .with_context(|| format!("another server owns {}", path.display()))?;
    match std::fs::symlink_metadata(path) {
        Ok(meta) => {
            anyhow::ensure!(
                meta.file_type().is_socket(),
                "{} exists and is not a socket",
                path.display()
            );
            match std::os::unix::net::UnixStream::connect(path) {
                Ok(_) => anyhow::bail!("another server is listening on {}", path.display()),
                Err(e) if e.kind() == std::io::ErrorKind::ConnectionRefused => {
                    std::fs::remove_file(path)?;
                }
                Err(e) => return Err(e.into()),
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e.into()),
    }
    // Bind privately under a temporary name, then publish without replacement.
    let temporary = path.with_file_name(format!(".{}.tmp", uuid::Uuid::new_v4().simple()));
    let listener = std::os::unix::net::UnixListener::bind(&temporary)?;
    let published = (|| {
        std::fs::set_permissions(&temporary, std::fs::Permissions::from_mode(0o600))?;
        let meta = std::fs::symlink_metadata(&temporary)?;
        std::fs::hard_link(&temporary, path)?;
        Ok::<_, std::io::Error>((meta.dev(), meta.ino()))
    })();
    let _ = std::fs::remove_file(&temporary);
    let identity = published.with_context(|| format!("publish {}", path.display()))?;
    listener.set_nonblocking(true)?;
    Ok((
        tokio::net::UnixListener::from_std(listener)?,
        OwnedSocket {
            path: path.to_owned(),
            identity,
            _lock: lock,
        },
    ))
}

pub async fn start(config: Config) -> Result<RunningServer> {
    start_inner(config, None).await
}

/// Start from a configuration file. Only such a server can reload membership,
/// and its status reports the SHA-256 of the exact file bytes it applied.
pub async fn start_file(path: &Path) -> Result<RunningServer> {
    let (config, digest) = Config::load_with_digest(path)?;
    start_inner(config, Some((std::fs::canonicalize(path)?, digest))).await
}

async fn start_inner(
    mut config: Config,
    source: Option<(PathBuf, String)>,
) -> Result<RunningServer> {
    config.validate()?;
    let baseline = core::reloadable_baseline(&config)?;
    #[cfg(unix)]
    let control = config.control_unix.as_deref().map(bind_unix).transpose()?;
    #[cfg(not(unix))]
    anyhow::ensure!(
        config.control_unix.is_none(),
        "control_unix requires a Unix host"
    );
    let tls = config.tls.as_ref().map(transport::acceptor).transpose()?;
    let listener = Listener::bind(config.listen, config.listen_unix.as_deref()).await?;
    let (address, unix) = (listener.tcp(), listener.unix());
    let http_listener = if let Some(a) = &mut config.artifacts {
        let listener = Listener::bind(a.listen, a.listen_unix.as_deref()).await?;
        if let (Some(requested), Some(bound)) = (a.listen, listener.tcp())
            && requested.port() == 0
        {
            a.public_url = format!("http://{bound}");
        }
        Some(listener)
    } else {
        None
    };
    let artifact_address = http_listener.as_ref().and_then(Listener::tcp);
    let artifact_unix = http_listener.as_ref().and_then(Listener::unix);
    let store = crate::store::Store::open(&config)?;
    let config = Arc::new(config);
    let (tx, mut rx) = mpsc::channel(256);
    let (stop, mut stopping) = watch::channel(false);
    let mut state = core::State::new(config.clone(), store)?;
    let source_path = source.as_ref().map(|(path, _)| path.clone());
    if let Some((_, digest)) = source {
        state.set_source(digest, baseline);
    }
    let shared = state.shared_config();
    #[cfg(unix)]
    let control_task = control.map(|(listener, owned)| {
        tokio::spawn(control::serve(
            listener,
            owned,
            source_path.clone(),
            tx.clone(),
            stopping.clone(),
        ))
    });
    #[cfg(not(unix))]
    let _ = source_path;
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
        let (config, tx, stopping) = (shared.clone(), tx.clone(), stopping.clone());
        tokio::spawn(async move {
            match listener {
                Listener::Tcp(listener) => http::serve(listener, config, tx, stopping).await,
                #[cfg(unix)]
                Listener::Unix(listener, _owned) => {
                    http::serve(listener, config, tx, stopping).await
                }
            }
        })
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
                    accepted = accept(&listener) => {
                        let socket = accepted?;
                        let Ok(permit) = semaphore.clone().try_acquire_owned() else { continue; };
                        next_id += 1;
                        let id = next_id;
                        let tx = tx.clone();
                        let tls = tls.clone();
                        let limit = config.limits.outbound_queue;
                        let mut stopping = stopping.clone();
                        clients.spawn(async move {
                            let _permit = permit;
                            let connect = async {
                                let stream: Stream = match (socket, tls) {
                                    (Accepted::Tcp(socket), Some(tls)) => Box::new(tls.accept(socket).await?),
                                    (Accepted::Tcp(socket), None) => Box::new(socket),
                                    #[cfg(unix)]
                                    (Accepted::Unix(socket), _) => Box::new(socket),
                                };
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
        // Unlink the IRC socket (if still ours) before reporting shutdown.
        drop(listener);
        let _ = tx.send(Event::Shutdown).await;
        actor.await?;
        if let Some(task) = http_task {
            task.abort();
            let _ = task.await;
        }
        #[cfg(unix)]
        if let Some(task) = control_task {
            task.abort();
            let _ = task.await;
        }
        result
    });
    Ok(RunningServer {
        address,
        unix,
        artifact_address,
        artifact_unix,
        stop,
        task: Some(task),
    })
}

enum Accepted {
    Tcp(tokio::net::TcpStream),
    #[cfg(unix)]
    Unix(tokio::net::UnixStream),
}

async fn accept(listener: &Listener) -> std::io::Result<Accepted> {
    match listener {
        Listener::Tcp(listener) => {
            let (socket, _) = listener.accept().await?;
            socket.set_nodelay(true)?;
            Ok(Accepted::Tcp(socket))
        }
        #[cfg(unix)]
        Listener::Unix(listener, _) => Ok(Accepted::Unix(listener.accept().await?.0)),
    }
}

async fn session(
    id: u64,
    stream: Stream,
    tx: mpsc::Sender<Event>,
    limit: usize,
    mut stopping: watch::Receiver<bool>,
) -> Result<()> {
    let (out, mut output) = mpsc::channel::<String>(limit);
    let queued = Arc::new(AtomicUsize::new(0));
    let (kill, mut killed) = watch::channel(false);
    tx.send(Event::Connect {
        id,
        out,
        queued: queued.clone(),
        kill,
    })
    .await?;
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
        // A frame group (for example one multiline batch) is written contiguously.
        while let Some(lines) = output.recv().await {
            if lines.is_empty() {
                // Graceful close: every group queued before it has been written.
                let _ = tokio::time::timeout(Duration::from_secs(1), writer.shutdown()).await;
                break;
            }
            tokio::time::timeout(Duration::from_secs(10), writer.write_all(lines.as_bytes()))
                .await??;
            queued.fetch_sub(lines.len(), Ordering::Relaxed);
        }
        Ok::<_, anyhow::Error>(())
    };
    // Only an explicit kill (true) closes immediately. When the actor drops a
    // gracefully closed client, its sender disappears and the output drains to
    // the close marker instead, so a queued ERROR is always delivered.
    let kill = async {
        while killed.changed().await.is_ok() {
            if *killed.borrow() {
                return;
            }
        }
        std::future::pending::<()>().await;
    };
    let result = tokio::select! { result = input => result, result = output => result, _ = kill => Ok(()), _ = stopping.changed() => Ok(()) };
    let _ = tx.send(Event::Disconnect(id)).await;
    result
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;

    #[tokio::test]
    async fn graceful_close_delivers_queued_error_after_the_kill_sender_drops() {
        let (tx, mut rx) = mpsc::channel(8);
        let (mut peer, server) = tokio::io::duplex(4096);
        let (_stop, stopping) = watch::channel(false);
        let task = tokio::spawn(session(1, Box::new(server), tx, 8, stopping));
        let Some(Event::Connect { out, kill, .. }) = rx.recv().await else {
            panic!("no connect event");
        };
        out.try_send("ERROR :Membership revoked\r\n".into())
            .unwrap();
        out.try_send(String::new()).unwrap();
        // The actor removed the client: its kill sender disappears first.
        drop(kill);
        let mut received = String::new();
        tokio::time::timeout(Duration::from_secs(2), peer.read_to_string(&mut received))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(received, "ERROR :Membership revoked\r\n");
        task.await.unwrap().unwrap();
        // An explicit kill still closes at once, without draining.
        let (tx, mut rx) = mpsc::channel(8);
        let (_peer, server) = tokio::io::duplex(4096);
        let (_stop, stopping) = watch::channel(false);
        let task = tokio::spawn(session(2, Box::new(server), tx, 8, stopping));
        let Some(Event::Connect { kill, .. }) = rx.recv().await else {
            panic!("no connect event");
        };
        kill.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn socket_ownership_is_serialized_by_a_stable_lock() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("irc.sock");
        let first = bind_unix(&path).unwrap();
        // A second starter cannot even inspect or replace the path meanwhile.
        let error = bind_unix(&path).err().expect("second owner refused");
        assert!(
            error.to_string().contains("another server owns"),
            "{error:#}"
        );
        drop(first);
        assert!(!path.exists(), "owned socket removed");
        let again = bind_unix(&path).unwrap();
        drop(again);
    }
}
