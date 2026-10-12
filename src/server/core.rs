use super::{Event, ban};
use crate::{
    config::Config,
    protocol::{Message, fold, valid_nick},
    store::{Ban, Record, Store},
};
use anyhow::{Result, bail, ensure};
use base64::{Engine, engine::general_purpose::STANDARD};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
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
];

struct Client {
    nick: String,
    user: bool,
    account: Option<String>,
    registered: bool,
    negotiating: bool,
    cap302: bool,
    caps: BTreeSet<String>,
    sasl: Option<String>,
    failures: u8,
    out: mpsc::Sender<Message>,
    kill: watch::Sender<bool>,
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

pub(super) struct State {
    config: Arc<Config>,
    store: Store,
    clients: BTreeMap<u64, Client>,
    channels: BTreeMap<String, Channel>,
    batch: u64,
}

impl State {
    pub fn new(config: Arc<Config>, store: Store) -> Result<Self> {
        let mut channels = BTreeMap::new();
        for c in &config.channels {
            let key = fold(&c.name);
            let (moderated, topic) = store.channel(&key)?;
            let bans = store.bans(&key, ban::MAX_BANS)?;
            for entry in &bans {
                ensure!(
                    ban::normalize(&entry.mask).as_deref() == Some(entry.mask.as_str()),
                    "invalid stored ban mask"
                );
            }
            channels.insert(
                key,
                Channel {
                    name: c.name.clone(),
                    moderated,
                    topic,
                    bans: bans
                        .into_iter()
                        .map(|entry| (entry.mask.clone(), entry))
                        .collect(),
                    members: BTreeMap::new(),
                },
            );
        }
        Ok(Self {
            config,
            store,
            clients: BTreeMap::new(),
            channels,
            batch: 0,
        })
    }

    pub fn shutdown(&mut self) {
        for c in self.clients.values() {
            let _ = c.kill.send(true);
        }
    }

    pub fn handle(&mut self, event: Event) {
        match event {
            Event::Connect { id, out, kill } => {
                self.clients.insert(
                    id,
                    Client {
                        nick: String::new(),
                        user: false,
                        account: None,
                        registered: false,
                        negotiating: false,
                        cap302: false,
                        caps: BTreeSet::new(),
                        sasl: None,
                        failures: 0,
                        out,
                        kill,
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

    fn send(&mut self, id: u64, mut message: Message) {
        if let Some(c) = self.clients.get(&id) {
            message.tags.retain(|key, _| match key.as_str() {
                "time" => c.caps.contains("server-time"),
                "account" => c.caps.contains("account-tag"),
                "batch" => c.caps.contains("batch"),
                _ => c.caps.contains("message-tags"),
            });
            if message.encode().is_err() || c.out.try_send(message).is_err() {
                let _ = c.kill.send(true);
            }
        }
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
        let p = &m.params;
        let required = match m.command.as_str() {
            "NICK" | "PASS" | "AUTHENTICATE" | "JOIN" | "PART" | "PING" | "NAMES" | "WHO"
            | "WHOIS" | "MODE" | "TOPIC" | "TAGMSG" | "KICK" => 1,
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
            "AWAY" => self.numeric(id, "305", &["Away state is not stored"]),
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
                let caps = CAPS
                    .iter()
                    .map(|cap| {
                        if *cap == "sasl" && c.cap302 {
                            "sasl=PLAIN"
                        } else {
                            cap
                        }
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

    fn chat(&mut self, id: u64, mut m: Message) -> Result<()> {
        let target = m.params[0].clone();
        let notice = m.command == "NOTICE";
        let expected = if m.command == "TAGMSG" { 1 } else { 2 };
        if m.params.len() != expected {
            if !notice {
                self.numeric(id, "461", &[&m.command, "Invalid parameter count"]);
            }
            return Ok(());
        }
        if target.contains(',') {
            if !notice {
                self.numeric(id, "407", &["*", "One target per message"]);
            }
            return Ok(());
        }
        if m.command == "TAGMSG" && !self.clients[&id].caps.contains("message-tags") {
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
            if !notice {
                self.numeric(id, "417", &["Tags too long"]);
            }
            return Ok(());
        }
        m.prefix = Some(self.prefix(id));
        m.tags
            .insert("account".into(), self.clients[&id].account.clone().unwrap());
        m.tags.insert("time".into(), crate::store::now());
        if m.encode().is_err() {
            if !notice {
                self.numeric(id, "417", &["Message too long after relay prefix"]);
            }
            return Ok(());
        }
        let mut peers;
        if target.starts_with('#') {
            let key = fold(&target);
            let Some(c) = self.channels.get(&key) else {
                if !notice {
                    self.numeric(id, "403", &[&target, "No such channel"]);
                }
                return Ok(());
            };
            if !self.may_speak(id, &key) {
                if !notice {
                    self.numeric(id, "404", &[&target, "Cannot send to channel"]);
                }
                return Ok(());
            }
            m.params[0] = c.name.clone();
            peers = c.members.keys().copied().collect::<BTreeSet<_>>();
            if m.command != "TAGMSG" {
                self.store.append(&key, &mut m, None)?;
            }
        } else {
            if !self.config.allow_private_messages {
                if !notice {
                    self.numeric(id, "404", &[&target, "Private messages are disabled"]);
                }
                return Ok(());
            }
            let Some(target_id) = self
                .clients
                .iter()
                .find(|(_, c)| c.registered && fold(&c.nick) == fold(&target))
                .map(|(id, _)| *id)
            else {
                if !notice {
                    self.numeric(id, "401", &[&target, "No such nick"]);
                }
                return Ok(());
            };
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
        self.broadcast(peers, m);
        Ok(())
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
                        if r.op {
                            "H@"
                        } else if r.voice {
                            "H+"
                        } else {
                            "H"
                        },
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
                        flags,
                        "0 AIRC participant",
                    ],
                );
            }
        }
        self.numeric(id, "315", &[name, "End of WHO"]);
    }

    fn whois(&mut self, id: u64, nick: &str) {
        if let Some(c) = self
            .clients
            .values()
            .find(|c| c.registered && fold(&c.nick) == fold(nick))
        {
            let account = c.account.clone().unwrap();
            let nick = c.nick.clone();
            self.numeric(
                id,
                "311",
                &[&nick, &account, "airc", "*", "AIRC participant"],
            );
            self.numeric(id, "330", &[&nick, &account, "is logged in as"]);
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
        let selected = match selected {
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
        let name = self.channels[&channel].name.clone();
        let batch = self.begin_batch(id, "chathistory", &name);
        for row in selected {
            let mut m = row.message.clone();
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
    fn parse(reference: &str, rows: &[Record], allow_all: bool) -> Result<Self> {
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
    fn compare(&self, row: &Record) -> std::cmp::Ordering {
        match self {
            Self::Sequence(seq) => row.seq.cmp(seq),
            Self::Time(time) => row.time.cmp(time),
            Self::All => std::cmp::Ordering::Greater,
        }
    }
}

fn select_history<'a>(
    rows: &'a [Record],
    sub: &str,
    reference: &str,
    second: Option<&str>,
    limit: usize,
) -> Result<Vec<&'a Record>> {
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
        }
        _ => bail!("invalid history operation"),
    }
    Ok(out)
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
            listen: "127.0.0.1:0".parse().unwrap(),
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
            kill: slow_kill,
        });
        state.handle(Event::Connect {
            id: 2,
            out: fast,
            kill: fast_kill,
        });
        for n in 0..10 {
            let message = Message::new("NOTICE", ["#work".into(), n.to_string()]);
            state.broadcast([1, 2], message);
            assert_eq!(reading.try_recv().unwrap().params[1], n.to_string());
        }
        assert!(*slow_status.borrow());
        assert!(!*fast_status.borrow());
        assert_eq!(unread.try_recv().unwrap().params[1], "0");
        state.handle(Event::Tick);
        assert!(!state.clients.contains_key(&1));
        assert!(state.clients.contains_key(&2));
    }
}
