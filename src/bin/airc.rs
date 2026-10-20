use anyhow::Result;
use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(version, about = "Persistent IRC client for agents")]
struct Args {
    #[arg(long)]
    config: Option<PathBuf>,
    /// JSONL protocol for --config: 1 (raw events) or 2 (multiline, receipts).
    #[arg(long, default_value_t = 1, value_parser = clap::value_parser!(u8).range(1..=2))]
    jsonl: u8,
    #[arg(long, global = true)]
    state_dir: Option<PathBuf>,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Print the machine-readable supported-feature probe as JSON.
    Features,
    /// Transfer an artifact using authenticated HTTP only; no IRC connection.
    Artifact {
        #[command(subcommand)]
        command: ArtifactCommand,
    },
    /// Start a named persistent connection, or explicitly reconnect a stopped one.
    Connect {
        session: String,
        #[arg(long)]
        config: PathBuf,
    },
    /// Read new actionable events, or replay the last read after interrupted output.
    Read {
        session: String,
        #[arg(long, default_value_t = 0)]
        wait: u64,
        #[arg(long, default_value_t = 50)]
        limit: usize,
        #[arg(long)]
        again: bool,
    },
    /// Wait for unread events without consuming them; does not wake a model itself.
    Wait {
        session: String,
        /// Stop after this many seconds (zero checks once); omit to keep waiting.
        #[arg(long)]
        timeout: Option<u64>,
        /// Which unread events wake: all, messages (default) or mentions.
        #[arg(long, value_enum, default_value_t)]
        wake: airc::agent_session::Wake,
    },
    /// Send once and wait for the server's correlated echo. Never retries.
    /// Text with newlines is sent as one multiline message when supported.
    Send {
        session: String,
        target: String,
        /// Message text; omit with --stdin.
        #[arg(required_unless_present = "stdin", conflicts_with = "stdin")]
        text: Option<String>,
        /// Read the exact text from stdin; one trailing newline is removed.
        #[arg(long)]
        stdin: bool,
        #[arg(long)]
        reply: Option<String>,
    },
    /// Explicitly mark this connection away; wait for the server acknowledgment.
    Away { session: String, reason: String },
    /// Clear this connection's explicit away state; never inferred from waits.
    Back { session: String },
    /// Join a channel on this connection and follow it on reconnect. Waits
    /// for the server's confirmation; never retried.
    Join { session: String, channel: String },
    /// Leave a channel and stop following it. Waits for confirmation; never retried.
    Part { session: String, channel: String },
    /// Read a joined channel's topic, or set/clear it (channel operators only).
    /// Waits for the server's confirmation; never retried.
    Topic {
        session: String,
        channel: String,
        /// New topic text (1–200 UTF-8 bytes); use --clear to remove it.
        #[arg(long, conflicts_with = "clear")]
        set: Option<String>,
        #[arg(long)]
        clear: bool,
    },
    /// Show connection state, channels, and the last 32 send and channel control outcomes.
    Status { session: String },
    /// Close only this named client, preserving local recovery state.
    Disconnect { session: String },
    #[command(hide = true)]
    SessionWorker {
        session: String,
        #[arg(long)]
        config: PathBuf,
    },
}

#[derive(Subcommand)]
enum ArtifactCommand {
    /// Upload raw stdin once. Requires an existing joined speaking session.
    Upload {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        channel: String,
        #[arg(long)]
        name: String,
        #[arg(long, required = true)]
        stdin: bool,
    },
    /// Download a UUID into a new private file; refuses URLs and overwrites.
    Download {
        #[arg(long)]
        config: PathBuf,
        artifact: String,
        path: PathBuf,
    },
}

async fn artifact_command(command: ArtifactCommand) -> Result<()> {
    let value = match command {
        ArtifactCommand::Upload {
            config,
            channel,
            name,
            stdin: _,
        } => {
            use tokio::io::AsyncReadExt;
            let config = airc::client::ClientConfig::load(&config)?;
            let mut bytes = Vec::new();
            tokio::time::timeout(
                std::time::Duration::from_secs(30),
                tokio::io::stdin()
                    .take(airc::client::MAX_ARTIFACT_BYTES as u64 + 1)
                    .read_to_end(&mut bytes),
            )
            .await
            .map_err(|_| anyhow::anyhow!("artifact stdin timed out"))??;
            airc::client::upload_bytes(&config, &channel, &name, bytes).await?
        }
        ArtifactCommand::Download {
            config,
            artifact,
            path,
        } => {
            let config = airc::client::ClientConfig::load(&config)?;
            airc::client::download(&config, &artifact, &path).await?
        }
    };
    println!("{value}");
    Ok(())
}

fn main() -> Result<()> {
    let args = Args::parse();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let named = args.command.is_some() && !matches!(args.command, Some(Command::Artifact { .. }));
    let result = runtime.block_on(run(args));
    // Tokio stdin uses an uncancellable blocking read. After explicit quit, do
    // not require the caller to close its pipe or type another character.
    runtime.shutdown_timeout(std::time::Duration::from_millis(100));
    if named && let Err(e) = result {
        // Agent shell tools may discard stderr. Essential state/uncertainty
        // belongs in the normal output, together with a failing exit status.
        println!("! {e:#}");
        std::process::exit(1);
    }
    result
}

async fn run(args: Args) -> Result<()> {
    if matches!(args.command, Some(Command::Features)) {
        println!("{}", airc::feature_probe());
        return Ok(());
    }
    if let Some(command) = args.command {
        anyhow::ensure!(
            args.config.is_none(),
            "use connect SESSION --config FILE, or --config FILE alone for JSONL"
        );
        let command = match command {
            Command::Artifact { command } => return artifact_command(command).await,
            command => command,
        };
        #[cfg(unix)]
        {
            use airc::agent_session::{self as sessions, Operation};
            let root = args
                .state_dir
                .map(Ok)
                .unwrap_or_else(sessions::default_directory)?;
            let response = match command {
                Command::Connect { session, config } => {
                    sessions::connect(&root, &session, &config).await?
                }
                Command::Read {
                    session,
                    wait,
                    limit,
                    again,
                } => {
                    sessions::call(&root, &session, Operation::Read { wait, limit, again }).await?
                }
                Command::Wait {
                    session,
                    timeout,
                    wake,
                } => sessions::wait(&root, &session, timeout, wake).await?,
                Command::Send {
                    session,
                    target,
                    text,
                    stdin,
                    reply,
                } => {
                    let text = match text {
                        Some(text) => text,
                        None if stdin => read_stdin_text()?,
                        None => anyhow::bail!("give message text or --stdin"),
                    };
                    sessions::call(
                        &root,
                        &session,
                        Operation::Send {
                            target,
                            text,
                            reply,
                        },
                    )
                    .await?
                }
                Command::Away { session, reason } => {
                    sessions::call(&root, &session, Operation::Away { reason }).await?
                }
                Command::Back { session } => {
                    sessions::call(&root, &session, Operation::Back).await?
                }
                // Each invocation binds a fresh operation ID before any request.
                Command::Join { session, channel } => {
                    let operation_id = sessions::operation_id();
                    sessions::call(
                        &root,
                        &session,
                        Operation::Join {
                            channel,
                            operation_id,
                        },
                    )
                    .await?
                }
                Command::Part { session, channel } => {
                    let operation_id = sessions::operation_id();
                    sessions::call(
                        &root,
                        &session,
                        Operation::Part {
                            channel,
                            operation_id,
                        },
                    )
                    .await?
                }
                Command::Topic {
                    session,
                    channel,
                    set,
                    clear,
                } => {
                    anyhow::ensure!(
                        set.as_deref() != Some(""),
                        "--set needs text; use --clear to remove the topic"
                    );
                    let text = if clear { Some(String::new()) } else { set };
                    let operation_id = sessions::operation_id();
                    sessions::call(
                        &root,
                        &session,
                        Operation::Topic {
                            channel,
                            operation_id,
                            text,
                        },
                    )
                    .await?
                }
                Command::Status { session } => {
                    sessions::call(&root, &session, Operation::Status).await?
                }
                Command::Disconnect { session } => {
                    sessions::call(&root, &session, Operation::Disconnect).await?
                }
                Command::Features | Command::Artifact { .. } => {
                    unreachable!("handled before named sessions")
                }
                Command::SessionWorker { session, config } => {
                    return sessions::worker(&root, &session, &config).await;
                }
            };
            print!("{}", response.text);
            if !response.ok {
                std::process::exit(1);
            }
            return Ok(());
        }
        #[cfg(not(unix))]
        {
            let _ = command;
            anyhow::bail!("named sessions currently require Unix; use --config FILE for JSONL");
        }
    }
    let config = airc::client::ClientConfig::load(&args.config.ok_or_else(|| {
        anyhow::anyhow!("specify connect SESSION --config FILE, or --config FILE for JSONL")
    })?)?;
    airc::client::frontend_with(
        config,
        args.jsonl,
        tokio::io::BufReader::new(tokio::io::stdin()),
        tokio::io::stdout(),
    )
    .await
}

/// Bounded exact stdin text for `send --stdin`; a single final newline (as
/// written by heredocs and editors) is removed, all other bytes are kept.
fn read_stdin_text() -> Result<String> {
    use std::io::Read;
    let limit = airc::multiline::CLIENT_MAX_BYTES;
    let mut bytes = Vec::new();
    std::io::stdin()
        .take(limit as u64 + 2)
        .read_to_end(&mut bytes)?;
    if bytes.last() == Some(&b'\n') {
        bytes.pop();
    }
    anyhow::ensure!(
        bytes.len() <= limit,
        "stdin exceeds {limit} bytes; share larger content as an artifact"
    );
    Ok(String::from_utf8(bytes)?)
}
