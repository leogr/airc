use super::{ControlOp, Event, ban};
use crate::{
    config::Config,
    protocol::{Message, fold, valid_channel, valid_nick},
    store::{Ban, Entry, Store},
};
use anyhow::{Result, bail, ensure};
use base64::{Engine, engine::general_purpose::STANDARD};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::{mpsc, watch};

const CAPS: &[&str] = &[
    "sasl",
    "message-tags",
    "server-time",
    "account-tag",
    "batch",
    "echo-message",
    "draft/chathistory",
    "cap-notify",
    "away-notify",
    MULTILINE,
];
const AWAY_LEN: usize = 200;
const MULTILINE: &str = "draft/multiline";
const CONCAT: &str = "draft/multiline-concat";
const BATCH_TIMEOUT: Duration = Duration::from_secs(30);

/// One client-initiated draft/multiline batch, assembled before any delivery.
struct OpenBatch {
    reference: String,
    target: String,
    tags: BTreeMap<String, String>,
    command: Option<String>,
    text: String,
    /// Byte length and draft/multiline-concat flag of each received line.
    lines: Vec<(usize, bool)>,
    /// Last accepted line; human clients may pace batch lines slowly.
    active: Instant,
}

struct Client {
    nick: String,
    user: bool,
    account: Option<String>,
    registered: bool,
    /// Explicit session availability; never inferred from activity or waiters.
    away: Option<String>,
    negotiating: bool,
    cap302: bool,
    caps: BTreeSet<String>,
    sasl: Option<String>,
    failures: u8,
    out: mpsc::Sender<String>,
    queued: Arc<AtomicUsize>,
    kill: watch::Sender<bool>,
    batch: Option<OpenBatch>,
    /// Recently failed batch references whose remaining lines are ignored.
    discarded: VecDeque<String>,
    created: Instant,
    active: Instant,
}
#[derive(Default, Clone, Copy)]
struct Role {
    op: bool,
    voice: bool,
}
struct Channel {
    name: String,
    moderated: bool,
    topic: String,
    bans: BTreeMap<String, Ban>,
    members: BTreeMap<u64, Role>,
}

impl Channel {
    fn load(config: &crate::config::Channel, store: &Store) -> Result<Self> {
        let key = fold(&config.name);
        let (moderated, topic) = store.channel(&key)?;
        ensure!(
            topic.len() <= 200 && !topic.contains(['\r', '\n', '\0']),
            "invalid stored topic"
        );
        let bans = store.bans(&key, ban::MAX_BANS)?;
        for entry in &bans {
            ensure!(
                ban::normalize(&entry.mask).as_deref() == Some(entry.mask.as_str()),
                "invalid stored ban mask"
            );
        }
        Ok(Self {
            name: config.name.clone(),
            moderated,
            topic,
            bans: bans
                .into_iter()
                .map(|entry| (entry.mask.clone(), entry))
                .collect(),
            members: BTreeMap::new(),
        })
    }
}

/// The live configuration; membership reload replaces it for every reader.
pub(super) type SharedConfig = Arc<std::sync::RwLock<Arc<Config>>>;

pub(super) struct State {
    config: Arc<Config>,
    shared: SharedConfig,
    store: Store,
    clients: BTreeMap<u64, Client>,
    channels: BTreeMap<String, Channel>,
    batch: u64,
    /// Random per process, so a status from a replaced server never matches.
    incarnation: String,
    /// SHA-256 of the configuration bytes last applied from the source file.
    digest: Option<String>,
    /// Non-reloadable configuration; reload refuses any difference from it.
    baseline: Option<serde_json::Value>,
}

/// Configuration fields that a reload must leave unchanged.
pub(super) fn reloadable_baseline(config: &Config) -> Result<serde_json::Value> {
    let mut value = serde_json::to_value(config)?;
    if let Some(fields) = value.as_object_mut() {
        for reloadable in ["accounts", "channels", "membership_generation"] {
            fields.remove(reloadable);
        }
    }
    Ok(value)
}

impl State {
    pub fn new(config: Arc<Config>, store: Store) -> Result<Self> {
        let mut channels = BTreeMap::new();
        for c in &config.channels {
            channels.insert(fold(&c.name), Channel::load(c, &store)?);
        }
        Ok(Self {
            shared: Arc::new(std::sync::RwLock::new(config.clone())),
            config,
            store,
            clients: BTreeMap::new(),
            channels,
            batch: 0,
            incarnation: uuid::Uuid::new_v4().to_string(),
            digest: None,
            baseline: None,
        })
    }

    pub fn shared_config(&self) -> SharedConfig {
        self.shared.clone()
    }

    /// Mark this server as started from a file whose bytes had `digest`.
    pub fn set_source(&mut self, digest: String, baseline: serde_json::Value) {
        self.digest = Some(digest);
        self.baseline = Some(baseline);
    }

    fn control(&mut self, op: ControlOp) -> serde_json::Value {
        match op {
            ControlOp::Status => serde_json::json!({"ok": true, "result": self.status()}),
            ControlOp::Reload(Err(error)) => control_failure("INVALID_CONFIG", &error),
            ControlOp::Reload(Ok(loaded)) => {
                let (config, digest) = *loaded;
                self.reload(config, digest)
            }
        }
    }

    fn status(&self) -> serde_json::Value {
        // Connected accounts only: bounded by the connection limit, never truncated.
        let mut accounts = BTreeMap::<&str, usize>::new();
        for account in self.clients.values().filter_map(|c| c.account.as_deref()) {
            *accounts.entry(account).or_default() += 1;
        }
        let accounts = accounts
            .into_iter()
            .map(|(name, connections)| {
                (
                    name.to_owned(),
                    serde_json::json!({"connections": connections}),
                )
            })
            .collect::<serde_json::Map<_, _>>();
        let kind = |unix: bool| if unix { "unix" } else { "tcp" };
        serde_json::json!({
            "protocol": 1,
            "features": crate::FEATURES,
            "incarnation": self.incarnation,
            "applied_generation": self.config.membership_generation,
            "config_sha256": self.digest,
            "listeners": {
                "irc": kind(self.config.listen_unix.is_some()),
                "artifacts": self.config.artifacts.as_ref().map(|a| kind(a.listen_unix.is_some())),
            },
            "configured_accounts": self.config.accounts.len(),
            "accounts": accounts,
        })
    }

    /// Apply accounts and channel membership atomically, then revoke live
    /// authority that the new membership no longer grants.
    fn reload(&mut self, new: Config, digest: String) -> serde_json::Value {
        let Some(baseline) = &self.baseline else {
            return control_failure("RESTART_REQUIRED", "server was not started from a file");
        };
        if reloadable_baseline(&new).ok().as_ref() != Some(baseline) {
            return control_failure(
                "RESTART_REQUIRED",
                "only accounts, channel additions/members/operators and membership_generation reload",
            );
        }
        // Compare with the currently applied catalog, not the startup baseline:
        // A -> A+B -> A+C must refuse removing B. Exact spelling is stable even
        // when the generic config uses mixed-case names.
        if self.config.channels.iter().any(|old| {
            !new.channels
                .iter()
                .any(|candidate| candidate.name == old.name)
        }) {
            return control_failure(
                "RESTART_REQUIRED",
                "reload cannot remove or rename an applied channel",
            );
        }
        let applied = self.config.membership_generation.unwrap_or(0);
        let generation = new.membership_generation.unwrap_or(0);
        if generation < applied {
            return control_failure(
                "GENERATION_STALE",
                &format!("generation {generation} is older than applied {applied}"),
            );
        }
        if generation == applied {
            if self.digest.as_deref() != Some(digest.as_str()) {
                return control_failure(
                    "GENERATION_CONFLICT",
                    &format!("generation {generation} is applied with different content"),
                );
            }
            return self.reloaded(Vec::new(), Vec::new(), 0);
        }
        let additions = new
            .channels
            .iter()
            .filter(|channel| !self.channels.contains_key(&fold(&channel.name)))
            .cloned()
            .collect::<Vec<_>>();
        let prepared = (|| -> Result<BTreeMap<String, Channel>> {
            self.store.add_channels(&additions)?;
            additions
                .iter()
                .map(|channel| Ok((fold(&channel.name), Channel::load(channel, &self.store)?)))
                .collect()
        })();
        let prepared = match prepared {
            Ok(prepared) => prepared,
            Err(error) => {
                return control_failure(
                    "STORAGE_ERROR",
                    &format!("channel additions were not applied: {error:#}"),
                );
            }
        };
        let old = self.config.clone();
        let mut next = (*old).clone();
        next.accounts = new.accounts;
        next.channels = new.channels;
        next.membership_generation = new.membership_generation;
        let next = Arc::new(next);
        // All fallible store work is finished. Actor state and shared ACL/proof
        // become visible together before the next queued event is processed.
        self.channels.extend(prepared);
        self.config = next.clone();
        *self
            .shared
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = next.clone();
        self.digest = Some(digest);
        // A removed account or a changed token hash ends every session of it.
        let token = |config: &Config, name: &str| {
            config
                .accounts
                .iter()
                .find(|a| a.name == name)
                .map(|a| a.token_sha256.clone())
        };
        let revoked = self
            .clients
            .iter()
            .filter_map(|(id, c)| {
                let account = c.account.as_deref()?;
                let kept =
                    token(&old, account).is_some() && token(&old, account) == token(&next, account);
                (!kept).then(|| (*id, account.to_owned()))
            })
            .collect::<Vec<_>>();
        let mut disconnected = BTreeSet::new();
        for (id, account) in revoked {
            self.close(id, "Membership revoked");
            disconnected.insert(account);
        }
        // Channel membership and configured operator roles follow the new ACL.
        let mut kicked = Vec::new();
        let mut kicks = 0usize;
        for channel in next.channels.clone() {
            let key = fold(&channel.name);
            let was_operator = |account: &str| {
                old.channels
                    .iter()
                    .find(|c| fold(&c.name) == key)
                    .is_some_and(|c| c.operators.iter().any(|a| a == account))
            };
            let members = self.channels[&key]
                .members
                .iter()
                .map(|(id, role)| {
                    (
                        *id,
                        *role,
                        self.clients[id].account.clone().unwrap_or_default(),
                    )
                })
                .collect::<Vec<_>>();
            for (id, role, account) in members {
                let name = self.channels[&key].name.clone();
                let nick = self.clients[&id].nick.clone();
                let peers = self.channels[&key]
                    .members
                    .keys()
                    .copied()
                    .collect::<Vec<_>>();
                if !channel.members.contains(&account) {
                    let mut kick = Message::new("KICK", [&name, &nick, "Membership revoked"]);
                    kick.prefix = Some(self.config.server_name.clone());
                    self.broadcast(peers, kick);
                    self.channels.get_mut(&key).unwrap().members.remove(&id);
                    if kicked.len() < MAX_KICK_DETAILS {
                        kicked.push(serde_json::json!({"channel": name, "account": account}));
                    }
                    kicks += 1;
                    continue;
                }
                let operator = channel.operators.contains(&account);
                let change = if operator && !was_operator(&account) && !role.op {
                    Some("+o")
                } else if !operator && was_operator(&account) && role.op {
                    Some("-o")
                } else {
                    None
                };
                if let Some(change) = change {
                    self.channels
                        .get_mut(&key)
                        .unwrap()
                        .members
                        .get_mut(&id)
                        .unwrap()
                        .op = change == "+o";
                    let mut mode = Message::new("MODE", [&name, change, &nick]);
                    mode.prefix = Some(self.config.server_name.clone());
                    self.broadcast(peers, mode);
                }
            }
        }
        self.reloaded(disconnected.into_iter().collect(), kicked, kicks)
    }

    /// Applied state is always exact. `disconnected` is bounded by the
    /// connection limit; `kicked` details stop at `MAX_KICK_DETAILS`, with the
    /// exact `kicked_count` and `kicked_truncated` reporting the rest.
    fn reloaded(
        &self,
        disconnected: Vec<String>,
        kicked: Vec<serde_json::Value>,
        kicks: usize,
    ) -> serde_json::Value {
        serde_json::json!({"ok": true, "result": {
            "applied_generation": self.config.membership_generation,
            "config_sha256": self.digest,
            "incarnation": self.incarnation,
            "disconnected": disconnected,
            "kicked_truncated": kicks > kicked.len(),
            "kicked": kicked,
            "kicked_count": kicks,
        }})
    }

    /// Remove a session immediately, delivering `ERROR :reason` before the
    /// connection closes. Later input from that connection is ignored.
    fn close(&mut self, id: u64, reason: &str) {
        if !self.clients.contains_key(&id) {
            return;
        }
        let mut quit = Message::new("QUIT", [reason]);
        quit.prefix = Some(self.prefix(id));
        let peers = self
            .peers(id)
            .into_iter()
            .filter(|peer| *peer != id)
            .collect::<Vec<_>>();
        self.broadcast(peers, quit);
        for c in self.channels.values_mut() {
            c.members.remove(&id);
        }
        self.send(id, Message::new("ERROR", [reason]));
        if let Some(c) = self.clients.remove(&id)
            && c.out.try_send(String::new()).is_err()
        {
            let _ = c.kill.send(true);
        }
    }

    pub fn shutdown(&mut self) {
        for c in self.clients.values() {
            let _ = c.kill.send(true);
        }
    }

    pub fn handle(&mut self, event: Event) {
        match event {
            Event::Connect {
                id,
                out,
                queued,
                kill,
            } => {
                self.clients.insert(
                    id,
                    Client {
                        nick: String::new(),
                        user: false,
                        account: None,
                        registered: false,
                        away: None,
                        negotiating: false,
                        cap302: false,
                        caps: BTreeSet::new(),
                        sasl: None,
                        failures: 0,
                        out,
                        queued,
                        kill,
                        batch: None,
                        discarded: VecDeque::new(),
                        created: Instant::now(),
                        active: Instant::now(),
                    },
                );
            }
            Event::Disconnect(id) => self.disconnect(id, "Connection closed"),
            Event::Message(id, message) if self.clients.contains_key(&id) => {
                self.clients.get_mut(&id).unwrap().active = Instant::now();
                if self.command(id, message).is_err() {
                    self.numeric(id, "400", &["Request could not be completed"]);
                }
            }
            Event::Tick => {
                let expired: Vec<_> = self
                    .clients
                    .iter()
                    .filter(|(_, c)| {
                        (!c.registered && c.created.elapsed() > Duration::from_secs(30))
                            || c.active.elapsed() > Duration::from_secs(120)
                    })
                    .map(|(id, _)| *id)
                    .collect();
                for id in expired {
                    self.disconnect(id, "Connection timed out");
                }
                let late: Vec<_> = self
                    .clients
                    .iter()
                    .filter(|(_, c)| {
                        c.batch
                            .as_ref()
                            .is_some_and(|b| b.active.elapsed() > BATCH_TIMEOUT)
                    })
                    .map(|(id, _)| *id)
                    .collect();
                for id in late {
                    self.timeout_batch(id);
                }
                let idle: Vec<_> = self
                    .clients
                    .iter()
                    .filter(|(_, c)| c.active.elapsed() > Duration::from_secs(60))
                    .map(|(id, _)| *id)
                    .collect();
                for id in idle {
                    self.reply(id, "PING", &[&self.config.server_name.clone()]);
                }
                if self.store.prune().is_err() {
                    eprintln!("aircd: retention maintenance failed");
                }
            }
            Event::Artifact {
                account,
                channel,
                filename,
                data,
                reply,
            } => {
                let result = self.artifact(&account, &channel, &filename, &data);
                let _ = reply.send(result);
            }
            Event::Download { account, id, reply } => {
                let result = self
                    .store
                    .artifact(&id)
                    .map(|a| a.filter(|a| self.config.may_read(&account, &a.channel)));
                let _ = reply.send(result);
            }
            Event::Control { op, reply } => {
                let result = self.control(op);
                let _ = reply.send(result);
            }
            _ => {}
        }
        let dead: Vec<_> = self
            .clients
            .iter()
            .filter(|(_, c)| *c.kill.borrow())
            .map(|(id, _)| *id)
            .collect();
        for id in dead {
            self.disconnect(id, "Output queue exceeded");
        }
    }

    fn send(&mut self, id: u64, message: Message) {
        self.send_frames(id, vec![message]);
    }

    /// Queue frames contiguously, filtered for the recipient. A recipient whose
    /// unwritten bytes or queued groups exceed their limits is disconnected.
    fn send_frames(&mut self, id: u64, frames: Vec<Message>) {
        let Some(c) = self.clients.get(&id) else {
            return;
        };
        let mut lines = String::new();
        for mut message in frames {
            message.tags.retain(|key, _| match key.as_str() {
                "time" => c.caps.contains("server-time"),
                "account" => c.caps.contains("account-tag"),
                "batch" => c.caps.contains("batch"),
                CONCAT => c.caps.contains(MULTILINE),
                _ => c.caps.contains("message-tags"),
            });
            match message.encode() {
                Ok(line) => lines.push_str(&line),
                Err(_) => {
                    let _ = c.kill.send(true);
                    return;
                }
            }
        }
        let size = lines.len();
        let queued = c.queued.fetch_add(size, Ordering::Relaxed) + size;
        if queued > self.config.limits.outbound_bytes || c.out.try_send(lines).is_err() {
            let _ = c.kill.send(true);
        }
    }

    fn multiline(&self, id: u64) -> bool {
        self.clients
            .get(&id)
            .is_some_and(|c| c.caps.contains(MULTILINE) && c.caps.contains("batch"))
    }

    fn reply(&mut self, id: u64, command: &str, params: &[&str]) {
        let mut m = Message::new(command, params.iter().copied());
        m.prefix = Some(self.config.server_name.clone());
        self.send(id, m);
    }
    fn numeric(&mut self, id: u64, code: &str, params: &[&str]) {
        let nick = self
            .clients
            .get(&id)
            .map(|c| c.nick.as_str())
            .filter(|n| !n.is_empty())
            .unwrap_or("*")
            .to_owned();
        let mut p = vec![nick.as_str()];
        p.extend(params);
        self.reply(id, code, &p);
    }
    fn fail(&mut self, id: u64, command: &str, code: &str, target: &str, reason: &str) {
        self.reply(id, "FAIL", &[command, code, target, reason]);
    }
    fn prefix(&self, id: u64) -> String {
        let c = &self.clients[&id];
        format!(
            "{}!{}@airc",
            c.nick,
            c.account.as_deref().unwrap_or("unknown")
        )
    }
    fn peers(&self, id: u64) -> BTreeSet<u64> {
        self.channels
            .values()
            .filter(|c| c.members.contains_key(&id))
            .flat_map(|c| c.members.keys().copied())
            .collect()
    }
    fn broadcast(&mut self, ids: impl IntoIterator<Item = u64>, message: Message) {
        for id in ids {
            self.send(id, message.clone());
        }
    }

    fn disconnect(&mut self, id: u64, reason: &str) {
        if !self.clients.contains_key(&id) {
            return;
        }
        let mut m = Message::new("QUIT", [reason]);
        m.prefix = Some(self.prefix(id));
        let peers = self
            .peers(id)
            .into_iter()
            .filter(|peer| *peer != id)
            .collect::<Vec<_>>();
        self.broadcast(peers, m);
        for c in self.channels.values_mut() {
            c.members.remove(&id);
        }
        if let Some(c) = self.clients.remove(&id) {
            let _ = c.kill.send(true);
        }
    }

    fn command(&mut self, id: u64, m: Message) -> Result<()> {
        if m.command == "BATCH" || m.tags.contains_key("batch") {
            if !self.clients[&id].registered {
                self.numeric(id, "451", &["Register first"]);
                return Ok(());
            }
            return self.client_batch(id, m);
        }
        // Client batches must be contiguous; keepalives may still interleave.
        if self.clients[&id].batch.is_some() && !matches!(m.command.as_str(), "PING" | "PONG") {
            self.abort_batch(
                id,
                &["MULTILINE_INVALID"],
                "Batch interrupted by another command",
            );
        }
        let p = &m.params;
        let required = match m.command.as_str() {
            "NICK" | "PASS" | "AUTHENTICATE" | "JOIN" | "PART" | "PING" | "NAMES" | "WHO"
            | "WHOIS" | "USERHOST" | "MODE" | "TOPIC" | "TAGMSG" | "KICK" => 1,
            "PRIVMSG" | "NOTICE" => 2,
            "USER" => 4,
            _ => 0,
        };
        if p.len() < required {
            if m.command != "NOTICE" {
                self.numeric(id, "461", &[&m.command, "Not enough parameters"]);
            }
            return Ok(());
        }
        match m.command.as_str() {
            "CAP" => return self.cap(id, p),
            "PING" => {
                self.reply(id, "PONG", &[&self.config.server_name.clone(), &p[0]]);
                return Ok(());
            }
            "PONG" => return Ok(()),
            "QUIT" => {
                self.disconnect(id, "Client quit");
                return Ok(());
            }
            "PASS" | "USER" | "AUTHENTICATE" if self.clients[&id].registered => {
                self.numeric(id, "462", &["Already registered"]);
                return Ok(());
            }
            "PASS" => {
                if let Some((name, token)) = p[0].split_once(':') {
                    let account = self.config.authenticate(name, token);
                    if account.is_some() {
                        self.clients.get_mut(&id).unwrap().account = account;
                    } else {
                        self.auth_failed(id);
                    }
                } else {
                    self.auth_failed(id);
                }
            }
            "AUTHENTICATE" => return self.sasl(id, &p[0]),
            "NICK" => {
                if !valid_nick(&p[0]) {
                    self.numeric(id, "432", &["*", "Erroneous nickname"]);
                    return Ok(());
                }
                if self
                    .clients
                    .iter()
                    .any(|(other, c)| *other != id && fold(&c.nick) == fold(&p[0]))
                {
                    self.numeric(id, "433", &[&p[0], "Nickname in use"]);
                    return Ok(());
                }
                if self.clients[&id].registered {
                    let mut change = Message::new("NICK", [&p[0]]);
                    change.prefix = Some(self.prefix(id));
                    let mut peers = self.peers(id);
                    peers.insert(id);
                    self.broadcast(peers, change);
                }
                self.clients.get_mut(&id).unwrap().nick = p[0].clone();
            }
            "USER" => {
                self.clients.get_mut(&id).unwrap().user = true;
            }
            _ if !self.clients[&id].registered => {
                self.numeric(id, "451", &["Register first"]);
                return Ok(());
            }
            "JOIN" => {
                for name in p[0].split(',').take(128) {
                    self.join(id, name);
                }
            }
            "PART" => {
                for name in p[0].split(',').take(128) {
                    self.part(id, name);
                }
            }
            "PRIVMSG" | "NOTICE" | "TAGMSG" => self.chat(id, m)?,
            "NAMES" => self.names(id, &p[0]),
            "MODE" => self.mode(id, p)?,
            "TOPIC" => self.topic(id, p)?,
            "KICK" => self.kick(id, p),
            "CHATHISTORY" => self.history(id, p)?,
            "WHO" => self.who(id, &p[0]),
            "WHOIS" => self.whois(id, &p[0]),
            "USERHOST" => self.userhost(id, p),
            "LIST" => {
                let account = self.clients[&id].account.clone().unwrap();
                let list = self
                    .channels
                    .values()
                    .filter(|c| self.config.may_read(&account, &c.name))
                    .map(|c| (c.name.clone(), c.members.len().to_string(), c.topic.clone()))
                    .collect::<Vec<_>>();
                self.numeric(id, "321", &["Channel", "Users Name"]);
                for (name, count, topic) in list {
                    self.numeric(id, "322", &[&name, &count, &topic]);
                }
                self.numeric(id, "323", &["End of LIST"]);
            }
            "MOTD" => self.numeric(id, "422", &["No MOTD"]),
            "AWAY" => self.away(id, p.first().map(String::as_str)),
            _ => self.numeric(id, "421", &[&m.command, "Unknown command"]),
        }
        self.register(id);
        Ok(())
    }

    fn register(&mut self, id: u64) {
        let Some(c) = self.clients.get(&id) else {
            return;
        };
        if c.registered || c.negotiating || c.nick.is_empty() || !c.user {
            return;
        }
        if c.account.is_none() {
            self.numeric(
                id,
                "464",
                &["Authentication required: use SASL PLAIN or PASS account:token"],
            );
            return;
        }
        self.clients.get_mut(&id).unwrap().registered = true;
        self.numeric(id, "001", &["Welcome to AIRC"]);
        self.numeric(id, "002", &["Your host is AIRC"]);
        self.numeric(
            id,
            "003",
            &["This server was created for shared conversations"],
        );
        self.numeric(
            id,
            "004",
            &[
                &self.config.server_name.clone(),
                env!("CARGO_PKG_VERSION"),
                "+",
                "bmnotv",
            ],
        );
        self.numeric(
            id,
            "005",
            &[
                "CHANTYPES=#",
                "CASEMAPPING=rfc1459",
                "PREFIX=(ov)@+",
                "CHANMODES=b,,,mnt",
                &format!("MAXLIST=b:{}", ban::MAX_BANS),
                "NICKLEN=24",
                &format!("AWAYLEN={AWAY_LEN}"),
                "CHANNELLEN=48",
                "UTF8ONLY",
                &format!("CHATHISTORY={}", self.config.limits.history_page),
                "MSGREFTYPES=msgid,timestamp",
                "are supported",
            ],
        );
        self.numeric(id, "422", &["No MOTD"]);
    }

    fn cap(&mut self, id: u64, p: &[String]) -> Result<()> {
        let sub = p.first().map(String::as_str).unwrap_or("");
        let nick = if self.clients[&id].nick.is_empty() {
            "*".into()
        } else {
            self.clients[&id].nick.clone()
        };
        match sub {
            "LS" => {
                let c = self.clients.get_mut(&id).unwrap();
                if !c.registered {
                    c.negotiating = true;
                }
                if p.get(1)
                    .and_then(|v| v.parse::<u32>().ok())
                    .is_some_and(|v| v >= 302)
                {
                    c.cap302 = true;
                    c.caps.insert("cap-notify".into());
                }
                let multiline = format!(
                    "{MULTILINE}=max-bytes={},max-lines={}",
                    self.config.limits.multiline_bytes, self.config.limits.multiline_lines
                );
                let caps = CAPS
                    .iter()
                    .map(|cap| match *cap {
                        "sasl" if c.cap302 => "sasl=PLAIN",
                        MULTILINE if c.cap302 => multiline.as_str(),
                        cap => cap,
                    })
                    .collect::<Vec<_>>()
                    .join(" ");
                self.reply(id, "CAP", &[&nick, "LS", &caps]);
            }
            "LIST" => {
                let caps = self.clients[&id]
                    .caps
                    .iter()
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(" ");
                self.reply(id, "CAP", &[&nick, "LIST", &caps]);
            }
            "REQ" if p.len() == 2 => {
                let c = self.clients.get_mut(&id).unwrap();
                if !c.registered {
                    c.negotiating = true;
                }
                let requested = p[1].split_whitespace().collect::<Vec<_>>();
                if requested.iter().any(|cap| {
                    !CAPS.contains(&cap.strip_prefix('-').unwrap_or(cap))
                        || (*cap == "-cap-notify" && c.cap302)
                }) {
                    self.reply(id, "CAP", &[&nick, "NAK", &p[1]]);
                } else {
                    for cap in requested {
                        if let Some(cap) = cap.strip_prefix('-') {
                            c.caps.remove(cap);
                        } else {
                            c.caps.insert(cap.into());
                        }
                    }
                    self.reply(id, "CAP", &[&nick, "ACK", &p[1]]);
                }
            }
            "END" => {
                self.clients.get_mut(&id).unwrap().negotiating = false;
                self.register(id);
            }
            _ => self.numeric(id, "410", &[sub, "Invalid CAP command"]),
        }
        Ok(())
    }

    fn auth_failed(&mut self, id: u64) {
        self.numeric(id, "904", &["Authentication failed"]);
        let c = self.clients.get_mut(&id).unwrap();
        c.sasl = None;
        c.failures += 1;
        if c.failures >= 3 {
            let _ = c.kill.send(true);
        }
    }

    fn sasl(&mut self, id: u64, p: &str) -> Result<()> {
        if !self.clients[&id].caps.contains("sasl") {
            self.auth_failed(id);
            return Ok(());
        }
        if self.clients[&id].account.is_some() {
            self.numeric(id, "907", &["Already authenticated"]);
            return Ok(());
        }
        if p == "*" {
            self.clients.get_mut(&id).unwrap().sasl = None;
            self.numeric(id, "906", &["Authentication aborted"]);
            return Ok(());
        }
        if self.clients[&id].sasl.is_none() {
            if p != "PLAIN" {
                self.auth_failed(id);
                return Ok(());
            }
            self.clients.get_mut(&id).unwrap().sasl = Some(String::new());
            self.reply(id, "AUTHENTICATE", &["+"]);
            return Ok(());
        }
        let buffer = self.clients.get_mut(&id).unwrap().sasl.as_mut().unwrap();
        if p.len() > 400 || buffer.len() + p.len() > 1600 {
            self.auth_failed(id);
            return Ok(());
        }
        if p != "+" {
            buffer.push_str(p);
        }
        if p.len() == 400 {
            return Ok(());
        }
        let encoded = self.clients.get_mut(&id).unwrap().sasl.take().unwrap();
        let auth = STANDARD
            .decode(encoded)
            .ok()
            .and_then(|b| String::from_utf8(b).ok())
            .and_then(|s| {
                let p = s.split('\0').collect::<Vec<_>>();
                if p.len() != 3 || (!p[0].is_empty() && fold(p[0]) != fold(p[1])) {
                    None
                } else {
                    self.config.authenticate(p[1], p[2])
                }
            });
        if let Some(account) = auth {
            self.clients.get_mut(&id).unwrap().account = Some(account.clone());
            self.numeric(
                id,
                "900",
                &[&self.prefix(id), &account, "You are logged in"],
            );
            self.numeric(id, "903", &["SASL authentication successful"]);
            self.register(id);
        } else {
            self.auth_failed(id);
        }
        Ok(())
    }

    fn join(&mut self, id: u64, name: &str) {
        let key = fold(name);
        let account = self.clients[&id].account.clone().unwrap();
        if !self.config.may_read(&account, &key) {
            self.numeric(id, "403", &[name, "No accessible channel"]);
            return;
        }
        let c = &self.channels[&key];
        if c.members.contains_key(&id) {
            return;
        }
        if self.banned(id, &key) {
            self.numeric(id, "474", &[name, "Cannot join channel (+b)"]);
            return;
        }
        let op = self
            .config
            .channels
            .iter()
            .find(|c| fold(&c.name) == key)
            .unwrap()
            .operators
            .contains(&account);
        let c = self.channels.get_mut(&key).unwrap();
        c.members.insert(id, Role { op, voice: false });
        let name = c.name.clone();
        let peers = c.members.keys().copied().collect::<Vec<_>>();
        let topic = c.topic.clone();
        let mut m = Message::new("JOIN", [&name]);
        m.prefix = Some(self.prefix(id));
        self.broadcast(peers.clone(), m);
        if self.clients[&id].away.is_some() {
            self.notify_away(id, peers.iter().copied());
        }
        if op {
            let mut m = Message::new("MODE", [&name, "+o", &self.clients[&id].nick]);
            m.prefix = Some(self.config.server_name.clone());
            self.broadcast(peers, m);
        }
        if topic.is_empty() {
            self.numeric(id, "331", &[&name, "No topic"]);
        } else {
            self.numeric(id, "332", &[&name, &topic]);
        }
        self.names(id, &name);
    }

    fn member(&mut self, id: u64, name: &str) -> bool {
        if self
            .channels
            .get(&fold(name))
            .is_some_and(|c| c.members.contains_key(&id))
        {
            true
        } else {
            self.numeric(id, "442", &[name, "You are not on that channel"]);
            false
        }
    }

    fn banned(&self, id: u64, channel: &str) -> bool {
        let prefix = self.prefix(id);
        self.channels[channel]
            .bans
            .keys()
            .any(|mask| ban::matches(mask, &prefix))
    }

    fn may_speak(&self, id: u64, channel: &str) -> bool {
        let c = &self.channels[channel];
        c.members
            .get(&id)
            .is_some_and(|r| r.op || r.voice || (!c.moderated && !self.banned(id, channel)))
    }
    fn operator(&mut self, id: u64, name: &str) -> bool {
        if !self.member(id, name) {
            return false;
        }
        if self.channels[&fold(name)].members[&id].op {
            true
        } else {
            self.numeric(id, "482", &[name, "Channel operator required"]);
            false
        }
    }

    fn part(&mut self, id: u64, name: &str) {
        if !self.member(id, name) {
            return;
        }
        let c = &self.channels[&fold(name)];
        let peers = c.members.keys().copied().collect::<Vec<_>>();
        let mut m = Message::new("PART", [&c.name]);
        m.prefix = Some(self.prefix(id));
        self.broadcast(peers, m);
        self.channels
            .get_mut(&fold(name))
            .unwrap()
            .members
            .remove(&id);
    }

    fn names(&mut self, id: u64, name: &str) {
        if !self.member(id, name) {
            return;
        }
        let c = &self.channels[&fold(name)];
        let name = c.name.clone();
        let names = c
            .members
            .iter()
            .map(|(id, role)| {
                format!(
                    "{}{}",
                    if role.op {
                        "@"
                    } else if role.voice {
                        "+"
                    } else {
                        ""
                    },
                    self.clients[id].nick
                )
            })
            .collect::<Vec<_>>();
        for chunk in names.chunks(10) {
            self.numeric(id, "353", &["=", &name, &chunk.join(" ")]);
        }
        self.numeric(id, "366", &[&name, "End of NAMES"]);
    }

    fn chat(&mut self, id: u64, m: Message) -> Result<()> {
        let notice = m.command == "NOTICE";
        let expected = if m.command == "TAGMSG" { 1 } else { 2 };
        if m.params.len() != expected {
            if !notice {
                self.numeric(id, "461", &[&m.command, "Invalid parameter count"]);
            }
            return Ok(());
        }
        if m.command == "TAGMSG" && !self.clients[&id].caps.contains("message-tags") {
            return Ok(());
        }
        self.relay(id, m, None, false)
    }

    /// Check permission, persist and deliver one logical message. `lines` holds
    /// the (text, concat) composition of a multiline message.
    fn relay(
        &mut self,
        id: u64,
        mut m: Message,
        lines: Option<Vec<(String, bool)>>,
        batch: bool,
    ) -> Result<()> {
        let target = m.params[0].clone();
        let notice = m.command == "NOTICE";
        if target.contains(',') {
            self.deny(id, batch, notice, "407", &["*", "One target per message"]);
            return Ok(());
        }
        m.tags
            .retain(|k, _| k.starts_with('+') && self.clients[&id].caps.contains("message-tags"));
        // Reserve space for authoritative tags; never let client metadata overflow a recipient.
        if m.tags
            .iter()
            .map(|(k, v)| k.len() + v.len() * 2 + 2)
            .sum::<usize>()
            > 2048
        {
            self.deny(id, batch, notice, "417", &["Tags too long"]);
            return Ok(());
        }
        m.prefix = Some(self.prefix(id));
        m.tags
            .insert("account".into(), self.clients[&id].account.clone().unwrap());
        m.tags.insert("time".into(), crate::store::now());
        let fits = match &lines {
            None => m.encode().is_ok(),
            Some(lines) => lines.iter().all(|(text, _)| {
                let mut line = Message::new(&m.command, [target.clone(), text.clone()]);
                line.prefix = m.prefix.clone();
                line.encode().is_ok()
            }),
        };
        if !fits {
            self.deny(
                id,
                batch,
                notice,
                "417",
                &["Message too long after relay prefix"],
            );
            return Ok(());
        }
        let mut peers;
        if target.starts_with('#') {
            let key = fold(&target);
            let Some(c) = self.channels.get(&key) else {
                self.deny(id, batch, notice, "403", &[&target, "No such channel"]);
                return Ok(());
            };
            if !self.may_speak(id, &key) {
                self.deny(
                    id,
                    batch,
                    notice,
                    "404",
                    &[&target, "Cannot send to channel"],
                );
                return Ok(());
            }
            m.params[0] = c.name.clone();
            peers = c.members.keys().copied().collect::<BTreeSet<_>>();
            if m.command != "TAGMSG" {
                self.store.append(&key, &mut m, None)?;
            }
        } else {
            if !self.config.allow_private_messages {
                self.deny(
                    id,
                    batch,
                    notice,
                    "404",
                    &[&target, "Private messages are disabled"],
                );
                return Ok(());
            }
            let Some(target_id) = self
                .clients
                .iter()
                .find(|(_, c)| c.registered && fold(&c.nick) == fold(&target))
                .map(|(id, _)| *id)
            else {
                self.deny(id, batch, notice, "401", &[&target, "No such nick"]);
                return Ok(());
            };
            if m.command == "PRIVMSG"
                && let Some(reason) = self.clients[&target_id].away.clone()
            {
                self.numeric(
                    id,
                    "301",
                    &[&self.clients[&target_id].nick.clone(), &reason],
                );
            }
            peers = BTreeSet::from([target_id]);
        }
        if self.clients[&id].caps.contains("echo-message") {
            peers.insert(id);
        } else {
            peers.remove(&id);
        }
        if m.command == "TAGMSG" {
            peers.retain(|id| self.clients[id].caps.contains("message-tags"));
        }
        for peer in peers {
            let multiline = self.multiline(peer);
            let frames = self.chat_frames(&m, lines.as_deref(), multiline, None);
            self.send_frames(peer, frames);
        }
        Ok(())
    }

    /// Reject one message. A rejected multiline batch always reports
    /// `FAIL BATCH`, even for NOTICE, so its sender gets a definite outcome.
    fn deny(&mut self, id: u64, batch: bool, notice: bool, code: &str, params: &[&str]) {
        if batch {
            let reason = params.last().copied().unwrap_or("Message rejected");
            self.fail_batch(id, &["MULTILINE_INVALID"], reason);
        } else if !notice {
            self.numeric(id, code, params);
        }
    }

    /// Frames for one logical message. Multiline-capable recipients receive a
    /// draft/multiline batch; others receive its non-blank lines, with the
    /// message ID and client tags only on the first line.
    fn chat_frames(
        &mut self,
        m: &Message,
        lines: Option<&[(String, bool)]>,
        multiline: bool,
        parent: Option<&str>,
    ) -> Vec<Message> {
        let nest = |mut m: Message| {
            if let Some(parent) = parent {
                m.tags.insert("batch".into(), parent.into());
            }
            m
        };
        let Some(lines) = lines else {
            return vec![nest(m.clone())];
        };
        let target = m.params[0].clone();
        let line = |text: &str| {
            let mut line = Message::new(&m.command, [target.clone(), text.to_owned()]);
            line.prefix = m.prefix.clone();
            line
        };
        if !multiline {
            let mut frames = Vec::new();
            for (text, _) in lines.iter().filter(|(text, _)| !text.is_empty()) {
                let mut fallback = line(text);
                fallback.tags = m
                    .tags
                    .iter()
                    .filter(|(key, _)| {
                        frames.is_empty() || ["account", "time"].contains(&key.as_str())
                    })
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect();
                frames.push(nest(fallback));
            }
            return frames;
        }
        self.batch += 1;
        let reference = format!("m{}", self.batch);
        let mut open = Message::new(
            "BATCH",
            [format!("+{reference}"), MULTILINE.into(), target.clone()],
        );
        open.tags = m.tags.clone();
        open.prefix = m.prefix.clone();
        let mut frames = vec![nest(open)];
        for (text, concat) in lines {
            let mut part = line(text);
            part.tags.insert("batch".into(), reference.clone());
            if *concat {
                part.tags.insert(CONCAT.into(), String::new());
            }
            frames.push(part);
        }
        let mut close = Message::new("BATCH", [format!("-{reference}")]);
        close.prefix = m.prefix.clone();
        frames.push(nest(close));
        frames
    }

    fn fail_batch(&mut self, id: u64, params: &[&str], reason: &str) {
        let mut p = vec!["BATCH"];
        p.extend(params);
        p.push(reason);
        self.reply(id, "FAIL", &p);
    }

    fn discard(&mut self, id: u64, reference: &str) {
        let c = self.clients.get_mut(&id).unwrap();
        if reference.len() <= 128 {
            c.discarded.push_back(reference.into());
            if c.discarded.len() > 4 {
                c.discarded.pop_front();
            }
        }
    }

    /// Reject the open batch; its later lines and closing BATCH are ignored.
    fn abort_batch(&mut self, id: u64, params: &[&str], reason: &str) {
        if let Some(batch) = self.clients.get_mut(&id).unwrap().batch.take() {
            self.fail_batch(id, params, reason);
            self.discard(id, &batch.reference);
        }
    }

    fn timeout_batch(&mut self, id: u64) {
        if let Some(reference) = self.clients[&id]
            .batch
            .as_ref()
            .map(|b| b.reference.clone())
        {
            self.abort_batch(id, &["TIMEOUT", &reference], "Batch timed out");
        }
    }

    fn client_batch(&mut self, id: u64, m: Message) -> Result<()> {
        if m.command != "BATCH" {
            let reference = m.tags["batch"].clone();
            let open = self.clients[&id]
                .batch
                .as_ref()
                .map(|b| b.reference.clone());
            if open.as_deref() == Some(reference.as_str()) {
                return self.batch_line(id, m);
            }
            if !self.clients[&id].discarded.contains(&reference) {
                self.fail_batch(id, &["MULTILINE_INVALID"], "Unknown batch reference");
            }
            return Ok(());
        }
        let Some(first) = m.params.first().cloned() else {
            self.numeric(id, "461", &["BATCH", "Not enough parameters"]);
            return Ok(());
        };
        if m.tags.contains_key("batch") {
            let reason = "Nested client batches are not supported";
            if self.clients[&id].batch.is_some() {
                self.abort_batch(id, &["MULTILINE_INVALID"], reason);
            } else {
                self.fail_batch(id, &["MULTILINE_INVALID"], reason);
            }
            if let Some(reference) = first.strip_prefix('+') {
                self.discard(id, reference);
            }
            return Ok(());
        }
        if let Some(reference) = first.strip_prefix('-') {
            let open = self.clients[&id]
                .batch
                .as_ref()
                .map(|b| b.reference.clone());
            if open.as_deref() == Some(reference) {
                return self.close_batch(id);
            }
            let c = self.clients.get_mut(&id).unwrap();
            if let Some(n) = c.discarded.iter().position(|r| r == reference) {
                c.discarded.remove(n);
            } else {
                self.fail_batch(id, &["MULTILINE_INVALID"], "Unknown batch reference");
            }
            return Ok(());
        }
        let Some(reference) = first.strip_prefix('+') else {
            self.fail_batch(id, &["MULTILINE_INVALID"], "Invalid BATCH reference");
            return Ok(());
        };
        if self.clients[&id].batch.is_some() {
            // Overlapping batches violate client-batch framing; reject both.
            self.abort_batch(
                id,
                &["MULTILINE_INVALID"],
                "A batch was opened before the previous batch closed",
            );
            self.discard(id, reference);
            return Ok(());
        }
        if reference.is_empty()
            || reference.len() > 64
            || !reference
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-')
        {
            self.fail_batch(id, &["INVALID_REFTAG", "*"], "Invalid batch reference");
            self.discard(id, reference);
            return Ok(());
        }
        let kind = m.params.get(1).map(String::as_str).unwrap_or("");
        let failure = if kind != MULTILINE {
            let kind = if valid_middle(kind) { kind } else { "*" };
            Some((
                vec!["UNKNOWN_TYPE", reference, kind],
                "Unsupported batch type",
            ))
        } else if !self.multiline(id) {
            Some((
                vec!["MULTILINE_INVALID"],
                "Negotiate batch and draft/multiline first",
            ))
        } else if m.params.len() != 3 || !(valid_channel(&m.params[2]) || valid_nick(&m.params[2]))
        {
            Some((
                vec!["MULTILINE_INVALID"],
                "A multiline batch needs one valid target",
            ))
        } else {
            None
        };
        if let Some((params, reason)) = failure {
            self.fail_batch(id, &params, reason);
            self.discard(id, reference);
            return Ok(());
        }
        let mut tags = m.tags;
        tags.retain(|key, _| key.starts_with('+'));
        self.clients.get_mut(&id).unwrap().batch = Some(OpenBatch {
            reference: reference.into(),
            target: m.params[2].clone(),
            tags,
            command: None,
            text: String::new(),
            lines: Vec::new(),
            active: Instant::now(),
        });
        Ok(())
    }

    fn batch_line(&mut self, id: u64, m: Message) -> Result<()> {
        let limits = &self.config.limits;
        let prefix = self.prefix(id);
        let b = self.clients[&id].batch.as_ref().unwrap();
        let concat = m.tags.contains_key(CONCAT);
        let max_lines = limits.multiline_lines.to_string();
        let max_bytes = limits.multiline_bytes.to_string();
        let mut relayed = Message::new(
            &m.command,
            [
                b.target.clone(),
                m.params.last().cloned().unwrap_or_default(),
            ],
        );
        relayed.prefix = Some(prefix);
        let too_long = relayed.encode().is_err();
        let failure: Option<(Vec<String>, &str)> = if b.active.elapsed() > BATCH_TIMEOUT {
            Some((
                vec!["TIMEOUT".into(), b.reference.clone()],
                "Batch timed out",
            ))
        } else if !["PRIVMSG", "NOTICE"].contains(&m.command.as_str()) || m.params.len() != 2 {
            Some((
                vec!["MULTILINE_INVALID".into()],
                "Multiline batches contain only PRIVMSG or NOTICE lines",
            ))
        } else if b.command.as_ref().is_some_and(|c| *c != m.command) {
            Some((vec!["MULTILINE_INVALID".into()], "Invalid multiline batch"))
        } else if fold(&m.params[0]) != fold(&b.target) {
            let provided = if valid_middle(&m.params[0]) {
                m.params[0].clone()
            } else {
                "*".into()
            };
            Some((
                vec![
                    "MULTILINE_INVALID_TARGET".into(),
                    b.target.clone(),
                    provided,
                ],
                "Invalid multiline target",
            ))
        } else if m.tags.keys().any(|key| key != "batch" && key != CONCAT) {
            Some((
                vec!["MULTILINE_INVALID".into()],
                "Line tags belong on the opening BATCH",
            ))
        } else if concat && m.params[1].is_empty() {
            Some((
                vec!["MULTILINE_INVALID".into()],
                "Invalid multiline batch with concatenated blank line",
            ))
        } else if too_long {
            Some((
                vec!["MULTILINE_INVALID".into()],
                "Multiline line too long after relay prefix",
            ))
        } else if b.lines.len() >= limits.multiline_lines {
            Some((
                vec!["MULTILINE_MAX_LINES".into(), max_lines],
                "Multiline batch max-lines exceeded",
            ))
        } else if b.text.len() + m.params[1].len() + usize::from(!b.lines.is_empty() && !concat)
            > limits.multiline_bytes
        {
            Some((
                vec!["MULTILINE_MAX_BYTES".into(), max_bytes],
                "Multiline batch max-bytes exceeded",
            ))
        } else {
            None
        };
        if let Some((params, reason)) = failure {
            let params = params.iter().map(String::as_str).collect::<Vec<_>>();
            self.abort_batch(id, &params, reason);
            return Ok(());
        }
        let b = self.clients.get_mut(&id).unwrap().batch.as_mut().unwrap();
        if !b.lines.is_empty() && !concat {
            b.text.push('\n');
        }
        b.text.push_str(&m.params[1]);
        b.lines.push((m.params[1].len(), concat));
        b.command = Some(m.command);
        b.active = Instant::now();
        Ok(())
    }

    fn close_batch(&mut self, id: u64) -> Result<()> {
        if self.clients[&id]
            .batch
            .as_ref()
            .is_some_and(|b| b.active.elapsed() > BATCH_TIMEOUT)
        {
            self.timeout_batch(id);
            return Ok(());
        }
        if self.clients[&id]
            .batch
            .as_ref()
            .is_some_and(|b| b.lines.iter().all(|(len, _)| *len == 0))
        {
            self.abort_batch(
                id,
                &["MULTILINE_INVALID"],
                "Invalid multiline batch with blank lines only",
            );
            return Ok(());
        }
        let b = self.clients.get_mut(&id).unwrap().batch.take().unwrap();
        let mut lines = Vec::with_capacity(b.lines.len());
        let mut offset = 0;
        for (n, (len, concat)) in b.lines.iter().enumerate() {
            if n > 0 && !concat {
                offset += 1;
            }
            lines.push((b.text[offset..offset + len].to_owned(), *concat));
            offset += len;
        }
        let message = Message {
            tags: b.tags,
            prefix: None,
            command: b.command.unwrap(),
            params: vec![b.target, b.text],
        };
        // A lone unconcatenated line is an ordinary message.
        if lines.len() == 1 && !lines[0].1 {
            self.relay(id, message, None, true)
        } else {
            self.relay(id, message, Some(lines), true)
        }
    }

    fn mode(&mut self, id: u64, p: &[String]) -> Result<()> {
        if !p[0].starts_with('#') {
            if fold(&p[0]) == fold(&self.clients[&id].nick) {
                self.numeric(id, "221", &["+"]);
            } else {
                self.numeric(id, "502", &["Cannot change other users"]);
            }
            return Ok(());
        }
        if !self.member(id, &p[0]) {
            return Ok(());
        }
        let key = fold(&p[0]);
        let c = &self.channels[&key];
        let name = c.name.clone();
        if p.len() == 1 {
            let modes = if c.moderated { "+mnt" } else { "+nt" };
            self.numeric(id, "324", &[&name, modes]);
            return Ok(());
        }
        let mut adding = true;
        let mut index = 2;
        for mode in p[1].chars() {
            match mode {
                '+' => {
                    adding = true;
                    continue;
                }
                '-' => {
                    adding = false;
                    continue;
                }
                'n' | 't' if adding => continue,
                _ => {}
            }
            if mode == 'b' && p.get(index).is_none() {
                let bans = self.channels[&key]
                    .bans
                    .values()
                    .cloned()
                    .collect::<Vec<_>>();
                for entry in bans {
                    self.numeric(
                        id,
                        "367",
                        &[&name, &entry.mask, &entry.set_by, &entry.set_at.to_string()],
                    );
                }
                self.numeric(id, "368", &[&name, "End of channel ban list"]);
                continue;
            }
            if !self.operator(id, &name) {
                return Ok(());
            }
            let mut params = vec![
                name.clone(),
                format!("{}{mode}", if adding { '+' } else { '-' }),
            ];
            match mode {
                'b' => {
                    let raw = &p[index];
                    index += 1;
                    let Some(mask) = ban::normalize(raw) else {
                        self.numeric(
                            id,
                            "696",
                            &[
                                &name,
                                "b",
                                "*",
                                "Invalid ban mask (maximum 200 ASCII bytes)",
                            ],
                        );
                        continue;
                    };
                    let exists = self.channels[&key].bans.contains_key(&mask);
                    if adding {
                        if exists {
                            continue;
                        }
                        if self.channels[&key].bans.len() >= ban::MAX_BANS {
                            self.numeric(id, "478", &[&name, &mask, "Channel ban list is full"]);
                            continue;
                        }
                        let entry = Ban {
                            mask: mask.clone(),
                            set_by: self.prefix(id),
                            set_at: chrono::Utc::now().timestamp(),
                        };
                        self.store.add_ban(&key, &entry)?;
                        self.channels
                            .get_mut(&key)
                            .unwrap()
                            .bans
                            .insert(mask.clone(), entry);
                    } else {
                        if !exists {
                            continue;
                        }
                        self.store.remove_ban(&key, &mask)?;
                        self.channels.get_mut(&key).unwrap().bans.remove(&mask);
                    }
                    params.push(mask);
                }
                'm' => {
                    let c = &self.channels[&key];
                    self.store.set_channel(&key, adding, &c.topic)?;
                    self.channels.get_mut(&key).unwrap().moderated = adding;
                }
                'o' | 'v' => {
                    let Some(nick) = p.get(index) else {
                        self.numeric(id, "461", &["MODE", "Missing nickname"]);
                        break;
                    };
                    index += 1;
                    let target = self
                        .clients
                        .iter()
                        .find(|(_, c)| fold(&c.nick) == fold(nick))
                        .map(|(id, _)| *id);
                    let Some(role) = target
                        .and_then(|id| self.channels.get_mut(&key).unwrap().members.get_mut(&id))
                    else {
                        self.numeric(id, "441", &[nick, &name, "User not in channel"]);
                        continue;
                    };
                    if mode == 'o' {
                        role.op = adding;
                    } else {
                        role.voice = adding;
                    }
                    params.push(nick.clone());
                }
                _ => {
                    self.numeric(id, "472", &[&mode.to_string(), "Unknown or fixed mode"]);
                    continue;
                }
            }
            let mut m = Message::new("MODE", params);
            m.prefix = Some(self.prefix(id));
            self.broadcast(
                self.channels[&key]
                    .members
                    .keys()
                    .copied()
                    .collect::<Vec<_>>(),
                m,
            );
        }
        Ok(())
    }

    fn topic(&mut self, id: u64, p: &[String]) -> Result<()> {
        if !self.member(id, &p[0]) {
            return Ok(());
        }
        let key = fold(&p[0]);
        let c = &self.channels[&key];
        let name = c.name.clone();
        if p.len() == 1 {
            let topic = c.topic.clone();
            self.numeric(
                id,
                if topic.is_empty() { "331" } else { "332" },
                &[&name, &topic],
            );
            return Ok(());
        }
        if !self.operator(id, &name) {
            return Ok(());
        }
        if p[1].len() > 200 {
            self.numeric(id, "417", &["Topic exceeds 200 bytes"]);
            return Ok(());
        }
        self.store
            .set_channel(&key, self.channels[&key].moderated, &p[1])?;
        self.channels.get_mut(&key).unwrap().topic = p[1].clone();
        let mut m = Message::new("TOPIC", [&name, &p[1]]);
        m.prefix = Some(self.prefix(id));
        self.broadcast(
            self.channels[&key]
                .members
                .keys()
                .copied()
                .collect::<Vec<_>>(),
            m,
        );
        Ok(())
    }

    fn kick(&mut self, id: u64, p: &[String]) {
        if p.len() < 2 {
            self.numeric(id, "461", &["KICK", "Missing nickname"]);
            return;
        }
        if !self.operator(id, &p[0]) {
            return;
        }
        let key = fold(&p[0]);
        let target = self
            .clients
            .iter()
            .find(|(_, c)| fold(&c.nick) == fold(&p[1]))
            .map(|(id, _)| *id);
        if let Some(target) =
            target.filter(|target| self.channels[&key].members.contains_key(target))
        {
            let mut m = Message::new(
                "KICK",
                [
                    &self.channels[&key].name,
                    &self.clients[&target].nick,
                    "Removed by channel operator",
                ],
            );
            m.prefix = Some(self.prefix(id));
            self.broadcast(
                self.channels[&key]
                    .members
                    .keys()
                    .copied()
                    .collect::<Vec<_>>(),
                m,
            );
            self.channels.get_mut(&key).unwrap().members.remove(&target);
        } else {
            self.numeric(id, "441", &[&p[1], &p[0], "User not in channel"]);
        }
    }

    fn away(&mut self, id: u64, reason: Option<&str>) {
        // Standard AWAY is connection state, not durable account/task state.
        // Truncate at a UTF-8 boundary to the advertised byte limit.
        let away = reason.filter(|text| !text.is_empty()).map(|text| {
            let mut end = text.len().min(AWAY_LEN);
            while !text.is_char_boundary(end) {
                end -= 1;
            }
            text[..end].to_owned()
        });
        let changed = self.clients[&id].away != away;
        self.clients.get_mut(&id).unwrap().away = away;
        if self.clients[&id].away.is_some() {
            self.numeric(id, "306", &["You have been marked as being away"]);
        } else {
            self.numeric(id, "305", &["You are no longer marked as being away"]);
        }
        if changed {
            self.notify_away(id, self.peers(id));
        }
    }

    fn notify_away(&mut self, id: u64, peers: impl IntoIterator<Item = u64>) {
        let mut message = Message::new("AWAY", self.clients[&id].away.iter().cloned());
        message.prefix = Some(self.prefix(id));
        let recipients = peers
            .into_iter()
            .filter(|peer| *peer != id && self.clients[peer].caps.contains("away-notify"))
            .collect::<BTreeSet<_>>();
        self.broadcast(recipients, message);
    }

    fn who(&mut self, id: u64, name: &str) {
        if self
            .channels
            .get(&fold(name))
            .is_some_and(|c| c.members.contains_key(&id))
        {
            let c = &self.channels[&fold(name)];
            let rows = c
                .members
                .iter()
                .map(|(id, r)| {
                    let c = &self.clients[id];
                    (
                        c.account.clone().unwrap(),
                        c.nick.clone(),
                        format!(
                            "{}{}",
                            if c.away.is_some() { "G" } else { "H" },
                            if r.op {
                                "@"
                            } else if r.voice {
                                "+"
                            } else {
                                ""
                            }
                        ),
                    )
                })
                .collect::<Vec<_>>();
            for (account, nick, flags) in rows {
                self.numeric(
                    id,
                    "352",
                    &[
                        name,
                        &account,
                        "airc",
                        &self.config.server_name.clone(),
                        &nick,
                        &flags,
                        "0 AIRC participant",
                    ],
                );
            }
        }
        self.numeric(id, "315", &[name, "End of WHO"]);
    }

    fn userhost(&mut self, id: u64, nicks: &[String]) {
        // RFC 2812: at most five names, unknown/unregistered users omitted.
        // Channel operators are not IRC operators; +/- is explicit away state.
        let users = nicks
            .iter()
            .take(5)
            .filter_map(|nick| {
                self.clients
                    .values()
                    .find(|c| c.registered && fold(&c.nick) == fold(nick))
            })
            .map(|c| {
                format!(
                    "{}={}{}@airc",
                    c.nick,
                    if c.away.is_some() { "-" } else { "+" },
                    c.account.as_deref().unwrap()
                )
            })
            .collect::<Vec<_>>()
            .join(" ");
        self.numeric(id, "302", &[&users]);
    }

    fn whois(&mut self, id: u64, nick: &str) {
        if let Some(c) = self
            .clients
            .values()
            .find(|c| c.registered && fold(&c.nick) == fold(nick))
        {
            let account = c.account.clone().unwrap();
            let nick = c.nick.clone();
            let away = c.away.clone();
            self.numeric(
                id,
                "311",
                &[&nick, &account, "airc", "*", "AIRC participant"],
            );
            self.numeric(id, "330", &[&nick, &account, "is logged in as"]);
            if let Some(reason) = away {
                self.numeric(id, "301", &[&nick, &reason]);
            }
        } else {
            self.numeric(id, "401", &[nick, "No such nick"]);
        }
        self.numeric(id, "318", &[nick, "End of WHOIS"]);
    }

    fn history(&mut self, id: u64, p: &[String]) -> Result<()> {
        if !self.clients[&id].caps.contains("draft/chathistory") {
            self.fail(
                id,
                "CHATHISTORY",
                "INVALID_PARAMS",
                "*",
                "Negotiate draft/chathistory first",
            );
            return Ok(());
        }
        let sub = p.first().map(String::as_str).unwrap_or("");
        if sub == "TARGETS" {
            return self.history_targets(id, p);
        }
        let expected = if sub == "BETWEEN" { 5 } else { 4 };
        if p.len() != expected || !["LATEST", "BEFORE", "AFTER", "AROUND", "BETWEEN"].contains(&sub)
        {
            self.fail(
                id,
                "CHATHISTORY",
                "INVALID_PARAMS",
                "*",
                "Invalid history request",
            );
            return Ok(());
        }
        let channel = fold(&p[1]);
        if !self
            .channels
            .get(&channel)
            .is_some_and(|c| c.members.contains_key(&id))
        {
            self.fail(
                id,
                "CHATHISTORY",
                "INVALID_TARGET",
                &p[1],
                "Join an authorized channel first",
            );
            return Ok(());
        }
        let Some(limit) = p
            .last()
            .and_then(|s| s.parse::<usize>().ok())
            .filter(|n| *n > 0 && *n <= self.config.limits.history_page)
        else {
            self.fail(id, "CHATHISTORY", "INVALID_PARAMS", &p[1], "Invalid limit");
            return Ok(());
        };
        let rows = self.store.history(&channel)?;
        let selected = select_history(&rows, sub, &p[2], p.get(3).map(String::as_str), limit);
        let (selected, trim) = match selected {
            Ok(rows) => rows,
            Err(e) => {
                self.fail(
                    id,
                    "CHATHISTORY",
                    if e.to_string() == "history reference no longer retained" {
                        "HISTORY_LOST"
                    } else {
                        "INVALID_PARAMS"
                    },
                    &p[1],
                    &e.to_string(),
                );
                return Ok(());
            }
        };
        // Selection uses the index; only the bounded page is decoded.
        let selected = self.store.messages(&selected)?;
        let name = self.channels[&channel].name.clone();
        let batch = self.begin_batch(id, "chathistory", &name);
        let multiline = self.multiline(id);
        let mut pages = Vec::new();
        for message in &selected {
            // Stored multiline messages are re-split into relay-sized lines.
            let lines = message.encode().is_err().then(|| split_lines(message));
            let frames = self.chat_frames(message, lines.as_deref(), multiline, batch.as_deref());
            let size: usize = frames
                .iter()
                .map(|m| m.encode().map_or(0, |line| line.len()))
                .sum();
            pages.push((frames, size));
        }
        // Pages are byte-bounded and always hold one message. Trimming keeps a
        // contiguous run nearest the reference (the anchor for AROUND), so
        // paging on from the returned edge never skips one; clients page until
        // an empty batch.
        let sizes = pages.iter().map(|(_, size)| *size).collect::<Vec<_>>();
        let (start, end) = page_window(&sizes, trim, self.config.limits.history_page_bytes);
        for (frames, _) in pages.into_iter().take(end).skip(start) {
            self.send_frames(id, frames);
        }
        if let Some(batch) = batch {
            self.reply(id, "BATCH", &[&format!("-{batch}")]);
        }
        Ok(())
    }

    fn begin_batch(&mut self, id: u64, kind: &str, target: &str) -> Option<String> {
        if !self.clients[&id].caps.contains("batch") {
            return None;
        }
        self.batch += 1;
        let batch = format!("h{}", self.batch);
        if target.is_empty() {
            self.reply(id, "BATCH", &[&format!("+{batch}"), kind]);
        } else {
            self.reply(id, "BATCH", &[&format!("+{batch}"), kind, target]);
        }
        Some(batch)
    }

    fn history_targets(&mut self, id: u64, p: &[String]) -> Result<()> {
        let parsed = (|| {
            ensure!(p.len() == 4, "parameters");
            let from = timestamp(&p[1])?;
            let to = timestamp(&p[2])?;
            let limit = p[3].parse::<usize>()?;
            ensure!(
                limit > 0 && limit <= self.config.limits.history_page,
                "limit"
            );
            Ok::<_, anyhow::Error>((from, to, limit))
        })();
        let Ok((from, to, limit)) = parsed else {
            self.fail(
                id,
                "CHATHISTORY",
                "INVALID_PARAMS",
                "TARGETS",
                "Invalid target query",
            );
            return Ok(());
        };
        let mut rows = Vec::new();
        for c in self.channels.values().filter(|c| {
            self.config
                .may_read(self.clients[&id].account.as_deref().unwrap(), &c.name)
        }) {
            if let Some(row) = self.store.history(&fold(&c.name))?.last()
                && row.time > std::cmp::min(from.clone(), to.clone())
                && row.time < std::cmp::max(from.clone(), to.clone())
            {
                rows.push((c.name.clone(), row.time.clone()));
            }
        }
        rows.sort_by(|a, b| a.1.cmp(&b.1));
        if from > to {
            rows.reverse();
        }
        rows.truncate(limit);
        let batch = self.begin_batch(id, "draft/chathistory-targets", "");
        for (name, time) in rows {
            let mut m = Message::new("CHATHISTORY", ["TARGETS", &name, &time]);
            m.prefix = Some(self.config.server_name.clone());
            if let Some(batch) = &batch {
                m.tags.insert("batch".into(), batch.clone());
            }
            self.send(id, m);
        }
        if let Some(batch) = batch {
            self.reply(id, "BATCH", &[&format!("-{batch}")]);
        }
        Ok(())
    }

    fn artifact(
        &mut self,
        account: &str,
        channel: &str,
        filename: &str,
        data: &[u8],
    ) -> Result<serde_json::Value> {
        let config = self
            .config
            .artifacts
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("artifacts disabled"))?;
        let channel = fold(channel);
        ensure!(
            self.config.may_read(account, &channel),
            "channel unavailable"
        );
        let c = &self.channels[&channel];
        let sender = c
            .members
            .iter()
            .find(|(id, _)| {
                self.clients[id].account.as_deref() == Some(account)
                    && self.may_speak(**id, &channel)
            })
            .map(|(id, _)| *id)
            .ok_or_else(|| {
                anyhow::anyhow!("join channel with speaking permission before uploading")
            })?;
        ensure!(data.len() <= config.max_bytes, "artifact too large");
        ensure!(
            !filename.is_empty()
                && filename.len() <= 100
                && filename
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b)),
            "filename must use letters, digits, dot, dash or underscore"
        );
        let id = uuid::Uuid::new_v4().to_string();
        let url = format!(
            "{}/v1/artifacts/{id}",
            config.public_url.trim_end_matches('/')
        );
        let digest = crate::config::token_hash_bytes(data);
        let mut m = Message::new(
            "PRIVMSG",
            [
                &c.name,
                &format!("Artifact {filename} ({} bytes): {url}", data.len()),
            ],
        );
        m.prefix = Some(self.prefix(sender));
        m.tags.insert("account".into(), account.into());
        // Reserved non-client tags: normal and multiline input strip these.
        // Store them in the same transaction as the artifact and announcement.
        m.tags.insert("airc-artifact".into(), id.clone());
        m.tags.insert("airc-artifact-name".into(), filename.into());
        m.tags
            .insert("airc-artifact-bytes".into(), data.len().to_string());
        m.tags.insert("airc-artifact-sha256".into(), digest.clone());
        m.encode()?;
        self.store.append(
            &channel,
            &mut m,
            Some((&id, filename, data, config.per_channel)),
        )?;
        self.broadcast(c.members.keys().copied().collect::<Vec<_>>(), m.clone());
        Ok(
            serde_json::json!({"id":id,"url":url,"sha256":digest,"bytes":data.len(),"msgid":m.tags["msgid"]}),
        )
    }
}

/// Kick details in a reload reply; 1024 entries keep it far below 1 MiB.
const MAX_KICK_DETAILS: usize = 1024;

fn control_failure(code: &str, error: &str) -> serde_json::Value {
    serde_json::json!({"ok": false, "code": code, "error": error})
}

fn valid_middle(param: &str) -> bool {
    !param.is_empty()
        && param.len() <= 128
        && !param.starts_with(':')
        && !param.contains([' ', '\r', '\n', '\0'])
}

/// Split a stored multiline message into lines that fit after its relay prefix.
/// Long logical lines become draft/multiline-concat parts, preferably after a space.
fn split_lines(m: &Message) -> Vec<(String, bool)> {
    let overhead = format!(
        ":{} {} {} :\r\n",
        m.prefix.as_deref().unwrap_or(""),
        m.command,
        m.params[0]
    )
    .len();
    let budget = 512usize.saturating_sub(overhead).max(4);
    let mut lines = Vec::new();
    for logical in m.params[1].split('\n') {
        let mut rest = logical;
        let mut concat = false;
        loop {
            let mut cut = rest.len().min(budget);
            while !rest.is_char_boundary(cut) {
                cut -= 1;
            }
            if cut < rest.len()
                && let Some(space) = rest[..cut].rfind(' ')
                && space + 1 > cut / 2
            {
                cut = space + 1;
            }
            lines.push((rest[..cut].to_owned(), concat));
            rest = &rest[cut..];
            concat = true;
            if rest.is_empty() {
                break;
            }
        }
    }
    lines
}

fn timestamp(reference: &str) -> Result<String> {
    let raw = reference
        .strip_prefix("timestamp=")
        .ok_or_else(|| anyhow::anyhow!("invalid timestamp reference"))?;
    Ok(chrono::DateTime::parse_from_rfc3339(raw)?
        .with_timezone(&chrono::Utc)
        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
}

enum Reference {
    Sequence(i64),
    Time(String),
    All,
}
impl Reference {
    fn parse(reference: &str, rows: &[Entry], allow_all: bool) -> Result<Self> {
        if allow_all && reference == "*" {
            return Ok(Self::All);
        }
        if let Some(id) = reference.strip_prefix("msgid=") {
            let row = rows
                .iter()
                .find(|r| r.id == id)
                .ok_or_else(|| anyhow::anyhow!("history reference no longer retained"))?;
            Ok(Self::Sequence(row.seq))
        } else {
            Ok(Self::Time(timestamp(reference)?))
        }
    }
    fn compare(&self, row: &Entry) -> std::cmp::Ordering {
        match self {
            Self::Sequence(seq) => row.seq.cmp(seq),
            Self::Time(time) => row.time.cmp(time),
            Self::All => std::cmp::Ordering::Greater,
        }
    }
}

/// Which end of a selected page stays nearest the request's reference.
#[derive(Clone, Copy, PartialEq, Debug)]
enum Trim {
    Forward,
    Backward,
    /// Index of the first row at or after an AROUND reference.
    Around(usize),
}

/// The contiguous `[start, end)` rows that fit `budget`, always at least one,
/// grown outward from the end nearest the reference.
fn page_window(sizes: &[usize], trim: Trim, budget: usize) -> (usize, usize) {
    let n = sizes.len();
    if n == 0 {
        return (0, 0);
    }
    let (mut start, mut end) = match trim {
        Trim::Forward => (0, 1),
        Trim::Backward => (n - 1, n),
        Trim::Around(anchor) => (anchor.min(n - 1), anchor.min(n - 1) + 1),
    };
    let mut used = sizes[start];
    loop {
        let mut grew = false;
        if trim != Trim::Backward && end < n && used + sizes[end] <= budget {
            used += sizes[end];
            end += 1;
            grew = true;
        }
        if trim != Trim::Forward && start > 0 && used + sizes[start - 1] <= budget {
            start -= 1;
            used += sizes[start];
            grew = true;
        }
        if !grew {
            return (start, end);
        }
    }
}

/// Selected rows in ascending order, and which end is nearest the reference.
fn select_history<'a>(
    rows: &'a [Entry],
    sub: &str,
    reference: &str,
    second: Option<&str>,
    limit: usize,
) -> Result<(Vec<&'a Entry>, Trim)> {
    use std::cmp::Ordering::{Equal, Greater, Less};
    let reference = Reference::parse(reference, rows, sub == "LATEST")?;
    let second = if sub == "BETWEEN" {
        Some(Reference::parse(
            second.ok_or_else(|| anyhow::anyhow!("missing second reference"))?,
            rows,
            false,
        )?)
    } else {
        None
    };
    let mut out = Vec::new();
    let mut trim = Trim::Backward;
    match sub {
        "LATEST" => {
            for r in rows {
                if reference.compare(r) == Greater {
                    out.push(r);
                }
            }
            out = out.into_iter().rev().take(limit).collect();
            out.reverse();
        }
        "BEFORE" => {
            for r in rows {
                if reference.compare(r) == Less {
                    out.push(r);
                }
            }
            out = out.into_iter().rev().take(limit).collect();
            out.reverse();
        }
        "AFTER" => {
            for r in rows {
                if reference.compare(r) == Greater {
                    out.push(r);
                    if out.len() == limit {
                        break;
                    }
                }
            }
            trim = Trim::Forward;
        }
        "AROUND" => {
            let anchor = rows
                .iter()
                .position(|r| reference.compare(r) != Less)
                .unwrap_or(rows.len());
            let start = anchor
                .saturating_sub(limit / 2)
                .min(rows.len().saturating_sub(limit));
            out = rows.iter().skip(start).take(limit).collect();
            trim = Trim::Around(anchor - start.min(anchor));
        }
        "BETWEEN" => {
            let second = second.ok_or_else(|| anyhow::anyhow!("missing second reference"))?;
            for r in rows {
                let a = reference.compare(r);
                let b = second.compare(r);
                if a != Equal && b != Equal && a != b {
                    out.push(r);
                }
            }
            let reverse = rows
                .iter()
                .any(|r| reference.compare(r) == Less && second.compare(r) == Greater);
            if reverse {
                out.reverse();
            }
            out.truncate(limit);
            if reverse {
                out.reverse();
            }
            trim = if reverse {
                Trim::Backward
            } else {
                Trim::Forward
            };
        }
        _ => bail!("invalid history operation"),
    }
    Ok((out, trim))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Account, Channel as ChannelConfig, Limits, token_hash};

    #[test]
    fn slow_output_disconnects_only_that_client() {
        let directory = tempfile::tempdir().unwrap();
        let config = Arc::new(Config {
            server_name: "test.airc".into(),
            listen: Some("127.0.0.1:0".parse().unwrap()),
            listen_unix: None,
            control_unix: None,
            membership_generation: None,
            database: directory.path().join("state.db"),
            allow_insecure: false,
            allow_private_messages: false,
            tls: None,
            limits: Limits::default(),
            accounts: vec![Account {
                name: "alice".into(),
                token_sha256: token_hash("test"),
            }],
            channels: vec![ChannelConfig {
                name: "#work".into(),
                members: vec!["alice".into()],
                operators: vec![],
                moderated: false,
            }],
            artifacts: None,
        });
        let store = Store::open(&config).unwrap();
        let mut state = State::new(config, store).unwrap();
        let (slow, mut unread) = mpsc::channel(2);
        let (fast, mut reading) = mpsc::channel(2);
        let (slow_kill, slow_status) = watch::channel(false);
        let (fast_kill, fast_status) = watch::channel(false);
        state.handle(Event::Connect {
            id: 1,
            out: slow,
            queued: Arc::default(),
            kill: slow_kill,
        });
        state.handle(Event::Connect {
            id: 2,
            out: fast,
            queued: Arc::default(),
            kill: fast_kill,
        });
        for n in 0..10 {
            let message = Message::new("NOTICE", ["#work".into(), n.to_string()]);
            state.broadcast([1, 2], message);
            assert_eq!(
                reading.try_recv().unwrap(),
                format!("NOTICE #work :{n}\r\n")
            );
        }
        assert!(*slow_status.borrow());
        assert!(!*fast_status.borrow());
        assert_eq!(unread.try_recv().unwrap(), "NOTICE #work :0\r\n");
        state.handle(Event::Tick);
        assert!(!state.clients.contains_key(&1));
        assert!(state.clients.contains_key(&2));
    }

    fn batch_state(lines: &[&str]) -> (State, mpsc::Receiver<String>, tempfile::TempDir) {
        let directory = tempfile::tempdir().unwrap();
        let config = Arc::new(Config {
            server_name: "test.airc".into(),
            listen: Some("127.0.0.1:0".parse().unwrap()),
            listen_unix: None,
            control_unix: None,
            membership_generation: None,
            database: directory.path().join("state.db"),
            allow_insecure: false,
            allow_private_messages: false,
            tls: None,
            limits: Limits::default(),
            accounts: vec![Account {
                name: "alice".into(),
                token_sha256: token_hash("test"),
            }],
            channels: vec![ChannelConfig {
                name: "#work".into(),
                members: vec!["alice".into()],
                operators: vec![],
                moderated: false,
            }],
            artifacts: None,
        });
        let store = Store::open(&config).unwrap();
        let mut state = State::new(config, store).unwrap();
        let (out, output) = mpsc::channel(64);
        let (kill, _status) = watch::channel(false);
        state.handle(Event::Connect {
            id: 1,
            out,
            queued: Arc::default(),
            kill,
        });
        for line in [
            "CAP REQ :batch draft/multiline echo-message",
            "PASS alice:test",
            "NICK alice",
            "USER alice 0 * :x",
            "CAP END",
            "JOIN #work",
        ]
        .iter()
        .chain(lines)
        {
            state.handle(Event::Message(1, Message::parse(line).unwrap()));
        }
        (state, output, directory)
    }

    #[test]
    fn unfinished_batch_times_out_on_tick_and_is_never_delivered() {
        let (mut state, mut output, _dir) = batch_state(&[
            "BATCH +slow draft/multiline #work",
            "@batch=slow PRIVMSG #work :never",
        ]);
        let open = state.clients.get_mut(&1).unwrap().batch.as_mut().unwrap();
        open.active = Instant::now()
            .checked_sub(BATCH_TIMEOUT + Duration::from_secs(1))
            .unwrap();
        state.handle(Event::Tick);
        state.handle(Event::Message(1, Message::parse("BATCH -slow").unwrap()));
        let mut sent = String::new();
        while let Ok(lines) = output.try_recv() {
            sent.push_str(&lines);
        }
        assert!(
            sent.contains("FAIL BATCH TIMEOUT slow :Batch timed out"),
            "{sent}"
        );
        assert!(!sent.contains("never"), "{sent}");
        assert!(state.clients[&1].batch.is_none());
    }

    #[test]
    fn page_window_is_contiguous_and_keeps_the_reference_edge() {
        let sizes = [5, 5, 5, 5, 5];
        assert_eq!(page_window(&sizes, Trim::Forward, 12), (0, 2));
        assert_eq!(page_window(&sizes, Trim::Backward, 12), (3, 5));
        assert_eq!(page_window(&sizes, Trim::Around(2), 16), (1, 4));
        assert_eq!(page_window(&sizes, Trim::Around(5), 1), (4, 5));
        assert_eq!(page_window(&[50, 1], Trim::Forward, 10), (0, 1));
        assert_eq!(page_window(&[], Trim::Forward, 10), (0, 0));
    }

    #[test]
    fn paced_batch_lines_renew_the_inactivity_timeout() {
        let (mut state, mut output, _dir) = batch_state(&["BATCH +paced draft/multiline #work"]);
        for text in ["one", "two", "three"] {
            // Each line arrives 25 s after the previous: 75 s in total.
            let open = state.clients.get_mut(&1).unwrap().batch.as_mut().unwrap();
            open.active = Instant::now().checked_sub(Duration::from_secs(25)).unwrap();
            state.handle(Event::Tick);
            let line = format!("@batch=paced PRIVMSG #work :{text}");
            state.handle(Event::Message(1, Message::parse(&line).unwrap()));
        }
        state.handle(Event::Message(1, Message::parse("BATCH -paced").unwrap()));
        let mut sent = String::new();
        while let Ok(lines) = output.try_recv() {
            sent.push_str(&lines);
        }
        assert!(!sent.contains("FAIL"), "{sent}");
        assert!(sent.contains("PRIVMSG #work :three"), "{sent}");
    }

    #[test]
    fn reload_reports_exact_kick_counts_with_bounded_details() {
        let directory = tempfile::tempdir().unwrap();
        let account = |name: &str| Account {
            name: name.into(),
            token_sha256: token_hash("test"),
        };
        let mut config = Config {
            server_name: "test.airc".into(),
            listen: Some("127.0.0.1:0".parse().unwrap()),
            listen_unix: None,
            control_unix: None,
            membership_generation: Some(1),
            database: directory.path().join("state.db"),
            allow_insecure: false,
            allow_private_messages: false,
            tls: None,
            limits: Limits::default(),
            accounts: vec![account("alice"), account("bob")],
            channels: vec![ChannelConfig {
                name: "#space".into(),
                members: vec!["alice".into(), "bob".into()],
                operators: vec![],
                moderated: false,
            }],
            artifacts: None,
        };
        let store = Store::open(&config).unwrap();
        let mut state = State::new(Arc::new(config.clone()), store).unwrap();
        state.set_source("one".into(), reloadable_baseline(&config).unwrap());
        // 1100 sessions of one account in the channel; outputs are discarded.
        for id in 1..=1100u64 {
            let (out, _) = mpsc::channel(1);
            let (kill, _) = watch::channel(false);
            state.handle(Event::Connect {
                id,
                out,
                queued: Arc::default(),
                kill,
            });
            let c = state.clients.get_mut(&id).unwrap();
            c.nick = format!("bob{id}");
            c.account = Some("bob".into());
            c.registered = true;
            state
                .channels
                .get_mut("#space")
                .unwrap()
                .members
                .insert(id, Role::default());
        }
        config.channels[0].members.retain(|m| m != "bob");
        config.membership_generation = Some(2);
        let reply = state.reload(config, "two".into());
        let result = &reply["result"];
        assert_eq!(result["applied_generation"], 2, "{reply}");
        assert_eq!(result["config_sha256"], "two");
        assert_eq!(result["kicked_count"], 1100);
        assert_eq!(result["kicked_truncated"], true);
        assert_eq!(result["kicked"].as_array().unwrap().len(), MAX_KICK_DETAILS);
        assert!(state.channels["#space"].members.is_empty());
        assert!(serde_json::to_vec(&reply).unwrap().len() < 1024 * 1024);
    }

    #[test]
    fn reload_adds_channels_only_against_the_applied_catalog_after_staging_storage() {
        let directory = tempfile::tempdir().unwrap();
        let channel = |name: &str| ChannelConfig {
            name: name.into(),
            members: vec!["alice".into()],
            operators: vec![],
            moderated: false,
        };
        let mut config = Config {
            server_name: "test.airc".into(),
            listen: Some("127.0.0.1:0".parse().unwrap()),
            listen_unix: None,
            control_unix: None,
            membership_generation: Some(1),
            database: directory.path().join("state.db"),
            allow_insecure: false,
            allow_private_messages: false,
            tls: None,
            limits: Limits::default(),
            accounts: vec![Account {
                name: "alice".into(),
                token_sha256: token_hash("test"),
            }],
            channels: vec![channel("#Mixed")],
            artifacts: None,
        };
        let store = Store::open(&config).unwrap();
        store.set_channel("#mixed", false, "kept topic").unwrap();
        let mut state = State::new(Arc::new(config.clone()), store).unwrap();
        state.set_source("one".into(), reloadable_baseline(&config).unwrap());
        // A -> A+B applies the addition with the exact applied spelling kept.
        config.channels.push(channel("#b"));
        config.membership_generation = Some(2);
        let reply = state.reload(config.clone(), "two".into());
        assert_eq!(reply["ok"], true, "{reply}");
        assert!(state.channels.contains_key("#b") && state.config.may_read("alice", "#b"));
        assert_eq!(state.channels["#mixed"].topic, "kept topic");
        // A+B -> A+C removes B, and a case rename of A, are both refused
        // against the currently applied catalog rather than the startup one.
        let mut replaced = config.clone();
        replaced.channels[1] = channel("#c");
        let mut renamed = config.clone();
        renamed.channels[0].name = "#mixed".into();
        for candidate in [replaced, renamed] {
            let mut candidate = candidate;
            candidate.membership_generation = Some(3);
            let reply = state.reload(candidate, "three".into());
            assert_eq!(reply["code"], "RESTART_REQUIRED", "{reply}");
        }
        assert!(!state.channels.contains_key("#c"));
        assert_eq!(state.config.membership_generation, Some(2));
        // A staging failure publishes nothing: no ACL, proof or actor state.
        state
            .store
            .execute_for_test(
                "INSERT INTO channels(name, moderated, topic) VALUES ('#d', 0, 'bad' || char(10) || 'topic')",
            )
            .unwrap();
        let mut added = config.clone();
        added.channels.push(channel("#d"));
        added.membership_generation = Some(3);
        let reply = state.reload(added.clone(), "three".into());
        assert_eq!(reply["code"], "STORAGE_ERROR", "{reply}");
        assert!(!state.channels.contains_key("#d") && !state.config.may_read("alice", "#d"));
        assert_eq!(state.config.membership_generation, Some(2));
        assert_eq!(state.digest.as_deref(), Some("two"));
        assert!(
            !state
                .shared_config()
                .read()
                .unwrap()
                .may_read("alice", "#d")
        );
        // Once storage is repaired, the same desired state applies once.
        state
            .store
            .execute_for_test("UPDATE channels SET topic='' WHERE name='#d'")
            .unwrap();
        assert_eq!(state.reload(added.clone(), "three".into())["ok"], true);
        assert!(state.channels.contains_key("#d"));
        let same = state.reload(added, "three".into());
        assert_eq!(same["result"]["applied_generation"], 3, "{same}");
    }
}
