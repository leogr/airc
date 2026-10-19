use airc::{
    client::ClientConfig,
    config::{self, Account, Artifacts, Channel, Config, Limits},
};
use anyhow::{Result, ensure};
use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(version, about = "IRC-compatible collaboration server")]
struct Args {
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
enum Command {
    /// Create a private local demo with human, agent-a and agent-b accounts.
    Init { directory: PathBuf },
    /// Validate configuration without starting listeners.
    Check {
        #[arg(long)]
        config: PathBuf,
    },
    /// Print the machine-readable supported-feature probe as JSON.
    Features,
    /// Run until SIGINT or SIGTERM.
    Serve {
        #[arg(long)]
        config: PathBuf,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    match Args::parse().command {
        Command::Init { directory } => init(directory)?,
        Command::Features => println!("{}", airc::feature_probe()),
        Command::Check { config } => {
            Config::load(&config)?;
            println!("Configuration valid");
        }
        Command::Serve { config } => {
            let mut server = airc::server::start_file(&config).await?;
            match (&server.address, &server.unix) {
                (Some(address), _) => eprintln!("aircd listening on {address}"),
                (_, Some(path)) => eprintln!("aircd listening on unix:{}", path.display()),
                _ => {}
            }
            if let Some(address) = server.artifact_address {
                eprintln!("artifacts listening on {address}");
            }
            if let Some(path) = &server.artifact_unix {
                eprintln!("artifacts listening on unix:{}", path.display());
            }
            #[cfg(unix)]
            {
                let mut terminate =
                    tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
                tokio::select! { result=tokio::signal::ctrl_c()=>result?,_=terminate.recv()=>{}, result=server.wait()=>return result }
            }
            #[cfg(not(unix))]
            tokio::select! { result=tokio::signal::ctrl_c()=>result?, result=server.wait()=>return result }
            server.shutdown().await?;
        }
    }
    Ok(())
}

fn init(directory: PathBuf) -> Result<()> {
    ensure!(
        !directory.exists(),
        "initialization directory already exists"
    );
    let mut builder = std::fs::DirBuilder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder.create(&directory)?;
    let mut accounts = Vec::new();
    for name in ["human", "agent-a", "agent-b"] {
        let token = config::new_token();
        config::write_private(&directory.join(format!("{name}.token")), token.as_bytes())?;
        accounts.push(Account {
            name: name.into(),
            token_sha256: config::token_hash(&token),
        });
        let client = ClientConfig {
            host: "127.0.0.1".into(),
            port: 6667,
            tls: false,
            allow_insecure: false,
            ca_certificate: None,
            proxy: None,
            account: name.into(),
            token_file: PathBuf::from(format!("{name}.token")),
            nick: name.into(),
            channels: vec!["#work".into()],
            artifact_url: Some("http://127.0.0.1:8080".into()),
            unix: None,
            artifact_unix: None,
        };
        config::write_private(
            &directory.join(format!("{name}.toml")),
            toml::to_string_pretty(&client)?.as_bytes(),
        )?;
    }
    let server = Config {
        server_name: "airc.local".into(),
        listen: Some("127.0.0.1:6667".parse()?),
        listen_unix: None,
        control_unix: None,
        membership_generation: None,
        database: "airc.sqlite3".into(),
        allow_insecure: false,
        allow_private_messages: false,
        tls: None,
        limits: Limits::default(),
        accounts,
        channels: vec![Channel {
            name: "#work".into(),
            members: vec!["human".into(), "agent-a".into(), "agent-b".into()],
            operators: vec!["human".into()],
            moderated: false,
        }],
        artifacts: Some(Artifacts {
            listen: Some("127.0.0.1:8080".parse()?),
            listen_unix: None,
            public_url: "http://127.0.0.1:8080".into(),
            max_bytes: 4 * 1024 * 1024,
            per_channel: 32,
        }),
    };
    server.validate()?;
    config::write_private(
        &directory.join("server.toml"),
        toml::to_string_pretty(&server)?.as_bytes(),
    )?;
    println!(
        "Created local configuration in {}. Credentials are in private .token files.",
        directory.display()
    );
    Ok(())
}
