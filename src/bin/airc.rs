use anyhow::Result;
use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(version, about = "Persistent IRC client for agents")]
struct Args {
    #[arg(long)]
    config: Option<PathBuf>,
    #[arg(long, global = true)]
    state_dir: Option<PathBuf>,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
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
    },
    /// Send once and wait for the server's correlated echo. Never retries.
    Send {
        session: String,
        target: String,
        text: String,
        #[arg(long)]
        reply: Option<String>,
    },
    /// Show connection state and the last 32 send outcomes.
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

fn main() -> Result<()> {
    let args = Args::parse();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let named = args.command.is_some();
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
    if let Some(command) = args.command {
        anyhow::ensure!(
            args.config.is_none(),
            "use connect SESSION --config FILE, or --config FILE alone for JSONL"
        );
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
                Command::Wait { session, timeout } => {
                    sessions::wait(&root, &session, timeout).await?
                }
                Command::Send {
                    session,
                    target,
                    text,
                    reply,
                } => {
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
                Command::Status { session } => {
                    sessions::call(&root, &session, Operation::Status).await?
                }
                Command::Disconnect { session } => {
                    sessions::call(&root, &session, Operation::Disconnect).await?
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
    airc::client::frontend(
        config,
        tokio::io::BufReader::new(tokio::io::stdin()),
        tokio::io::stdout(),
    )
    .await
}
