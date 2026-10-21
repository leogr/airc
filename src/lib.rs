#[cfg(unix)]
pub mod agent_session;
pub mod client;
pub mod config;
pub mod multiline;
pub mod protocol;
pub mod server;
mod store;
pub mod transport;

/// Implemented deployment features of this build. A deployer must refuse a
/// managed mode whose feature is absent rather than fall back to another mode.
pub const FEATURES: &[&str] = &[
    "multiline",
    "unix-listeners",
    "unix-artifacts",
    "control-status",
    "control-reload",
    "acl-revoke",
    "client-unix",
    "client-artifact-unix",
    "client-multiline",
    "jsonl-v2",
    "named-multiline",
    "send-stdin",
    "client-presence",
    "server-presence",
    "channel-add-reload",
    "client-topic",
    "named-channels",
    "artifact-cli",
    "artifact-metadata",
    "named-waiter-status",
    "named-delivery",
    "named-show",
];

/// The `features` probe printed by both binaries.
pub fn feature_probe() -> serde_json::Value {
    serde_json::json!({
        "protocol": 1,
        "version": env!("CARGO_PKG_VERSION"),
        "features": FEATURES,
    })
}
