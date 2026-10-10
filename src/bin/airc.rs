use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;

#[derive(Parser)]
#[command(version, about = "Persistent JSONL IRC client for agents")]
struct Args {
    #[arg(long)]
    config: PathBuf,
}

fn main() -> Result<()> {
    let config = airc::client::ClientConfig::load(&Args::parse().config)?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let result = runtime.block_on(airc::client::frontend(
        config,
        tokio::io::BufReader::new(tokio::io::stdin()),
        tokio::io::stdout(),
    ));
    // Tokio stdin uses an uncancellable blocking read. After explicit quit, do
    // not require the caller to close its pipe or type another character.
    runtime.shutdown_timeout(std::time::Duration::from_millis(100));
    result
}
