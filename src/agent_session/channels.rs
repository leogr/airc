//! Named-session channel state (saved-state version 2): the durable followed
//! preference, current-connection membership/topic evidence, refusals,
//! quarantine and caller-identified join/part/topic controls.
//!
//! Server ACLs remain authoritative. A followed channel is only a preference
//! for explicit reconnect; membership is evidence from this connection alone.
use super::history::{JoinHistory, Notice};
use crate::{
    client::{JoinRefusal, optional_text, own_prefix, server_numeric, topic_message},
    protocol::{Message, fold, valid_channel},
};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    time::{Duration, Instant},
};

/// Distinct channel keys across followed, membership, refusals, quarantine
/// and topics. Managed deployments allow at most 16 channels.
pub(super) const MAX_CHANNELS: usize = 128;
/// Retained channel control receipts (independent of the 32 send receipts).
const MAX_CONTROLS: usize = 32;
const MAX_REASON: usize = 256;
const MAX_DETAIL: usize = 512;
const MAX_TOPIC: usize = 200;
/// Automatic JOIN setup (initial topic and NAMES) must end within this bound.
pub(super) const SETUP_TIMEOUT: Duration = Duration::from_secs(10);
/// Server replies that refuse a JOIN of the named channel.
const JOIN_REFUSALS: &[&str] = &["403", "405", "471", "473", "474", "475", "476", "477"];

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Kind {
    Join,
    Part,
    Topic,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Presence {
    Joined,
    Absent,
    Unknown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Outcome {
    Pending,
    Confirmed,
    Refused,
    Uncertain,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Membership {
    pub channel: String,
    pub epoch: String,
    pub state: Presence,
}

/// Topic evidence from one connection. Empty text with `known` means no topic.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Topic {
    pub channel: String,
    pub epoch: String,
    pub known: bool,
    pub text: String,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Refusal {
    pub channel: String,
    pub epoch: String,
    pub code: String,
    pub reason: String,
}

/// A target excluded from startup rejoin until an explicit join or part.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Quarantine {
    pub channel: String,
    pub reason: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_id: Option<String>,
}

/// One caller-identified control and its exact request binding.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Control {
    pub operation_id: String,
    pub epoch: String,
    pub kind: Kind,
    pub channel: String,
    /// Requested topic: absent queries, empty clears. Never null.
    #[serde(
        default,
        deserialize_with = "optional_text",
        skip_serializing_if = "Option::is_none"
    )]
    pub topic: Option<String>,
    pub outcome: Outcome,
    pub detail: String,
}

impl Control {
    fn binds(&self, request: &Request) -> bool {
        self.kind == request.kind
            && fold(&self.channel) == fold(&request.channel)
            && self.topic == request.topic
    }
}

/// The single unresolved control. Its receipt holds the request binding.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Pending {
    pub operation_id: String,
    /// Follow preference before this control, restored if it is refused.
    pub followed: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Setup {
    /// Self JOIN observed; initial topic/NAMES end with a matching 366.
    /// `newer` records an authenticated TOPIC since that JOIN.
    Pending {
        until: Instant,
        newer: bool,
    },
    Complete,
    /// The bounded setup ended without proof; topic queries stay refused.
    Unknown,
}

/// Evidence and identity of the live connection; never persisted.
#[derive(Default)]
pub(super) struct Live {
    pub epoch: String,
    pub nick: String,
    pub account: String,
    /// Server name proven at registration; numerics must carry it.
    pub server: String,
    pub setup: BTreeMap<String, Setup>,
    pub history: JoinHistory,
    /// Quiet membership/topic changes that still need saving.
    pub dirty: bool,
}

/// A validated channel control from named IPC.
pub(super) struct Request {
    pub operation_id: String,
    pub kind: Kind,
    pub channel: String,
    pub topic: Option<String>,
}

impl Request {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            valid_operation_id(&self.operation_id),
            "operation_id must be 32 lowercase hexadecimal digits"
        );
        ensure!(valid_channel(&self.channel), "invalid channel");
        ensure!(
            self.kind == Kind::Topic || self.topic.is_none(),
            "only topic controls carry text"
        );
        topic_message(&self.channel, self.topic.as_deref())?;
        Ok(())
    }

    fn message(&self) -> Result<Message> {
        match self.kind {
            Kind::Join => Ok(Message::new("JOIN", [&self.channel])),
            Kind::Part => Ok(Message::new("PART", [&self.channel])),
            Kind::Topic => topic_message(&self.channel, self.topic.as_deref()),
        }
    }
}

/// A control decided locally, or one to write to the network exactly once.
pub(super) enum Begin {
    Done { ok: bool, text: String },
    Send(Message),
}

/// Membership changes that the session acts on beyond saving them.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Change {
    None,
    /// A fresh authentic self JOIN on this connection.
    Joined(String),
    Kicked(String),
}

pub(super) fn valid_operation_id(id: &str) -> bool {
    id.len() == 32
        && id
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn valid_epoch(epoch: &str) -> bool {
    (1..=64).contains(&epoch.len()) && epoch.bytes().all(|b| b.is_ascii_hexdigit() || b == b'-')
}

fn valid_topic(text: &str) -> bool {
    text.len() <= MAX_TOPIC && !text.contains(['\r', '\n', '\0'])
}

/// Truncate at a UTF-8 boundary; stored text is display-escaped when shown.
fn bounded(text: &str, limit: usize) -> String {
    let mut end = text.len().min(limit);
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    text[..end].to_owned()
}

fn key(keys: &mut BTreeSet<String>, key: &str, channel: &str) -> Result<()> {
    ensure!(
        valid_channel(channel) && fold(channel) == key,
        "invalid saved channel key"
    );
    keys.insert(key.to_owned());
    Ok(())
}

#[derive(Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Channels {
    /// Display spellings sorted by IRC fold; an explicit empty list stays empty.
    pub followed: Vec<String>,
    pub membership: BTreeMap<String, Membership>,
    pub refusals: BTreeMap<String, Refusal>,
    pub quarantine: BTreeMap<String, Quarantine>,
    pub topics: BTreeMap<String, Topic>,
    pub pending: Option<Pending>,
    pub receipts: VecDeque<Control>,
    #[serde(skip)]
    pub live: Live,
}

impl Channels {
    /// A new session or a version-1 upgrade follows the original configured channels.
    pub fn seeded(configured: &[String]) -> Result<Self> {
        let mut channels = Self::default();
        for channel in configured {
            channels.follow(channel, true);
        }
        channels.validate()?;
        Ok(channels)
    }

    pub fn validate(&self) -> Result<()> {
        let mut keys = BTreeSet::new();
        let mut previous: Option<String> = None;
        for channel in &self.followed {
            let folded = fold(channel);
            ensure!(
                valid_channel(channel) && previous.as_ref().is_none_or(|p| *p < folded),
                "followed channels must be valid, unique and sorted"
            );
            keys.insert(folded.clone());
            previous = Some(folded);
        }
        for (k, m) in &self.membership {
            key(&mut keys, k, &m.channel)?;
            ensure!(valid_epoch(&m.epoch), "invalid membership epoch");
        }
        for (k, r) in &self.refusals {
            key(&mut keys, k, &r.channel)?;
            ensure!(
                valid_epoch(&r.epoch)
                    && (1..=16).contains(&r.code.len())
                    && r.code.bytes().all(|b| b.is_ascii_alphanumeric())
                    && r.reason.len() <= MAX_REASON,
                "invalid channel refusal"
            );
        }
        for (k, q) in &self.quarantine {
            key(&mut keys, k, &q.channel)?;
            ensure!(
                q.reason.len() <= MAX_REASON
                    && q.operation_id.as_deref().is_none_or(valid_operation_id),
                "invalid channel quarantine"
            );
        }
        for (k, t) in &self.topics {
            key(&mut keys, k, &t.channel)?;
            ensure!(
                valid_epoch(&t.epoch) && valid_topic(&t.text) && (t.known || t.text.is_empty()),
                "invalid topic snapshot"
            );
        }
        ensure!(
            keys.len() <= MAX_CHANNELS,
            "more than {MAX_CHANNELS} tracked channels"
        );
        ensure!(
            self.receipts.len() <= MAX_CONTROLS,
            "too many channel control receipts"
        );
        let mut ids = BTreeSet::new();
        for r in &self.receipts {
            ensure!(
                valid_operation_id(&r.operation_id)
                    && ids.insert(r.operation_id.as_str())
                    && valid_channel(&r.channel)
                    && valid_epoch(&r.epoch)
                    && r.detail.len() <= MAX_DETAIL
                    && (r.kind == Kind::Topic || r.topic.is_none())
                    && r.topic.as_deref().is_none_or(valid_topic),
                "invalid channel control receipt"
            );
        }
        let pending = self
            .receipts
            .iter()
            .filter(|r| r.outcome == Outcome::Pending)
            .collect::<Vec<_>>();
        ensure!(
            match &self.pending {
                None => pending.is_empty(),
                Some(p) => pending.len() == 1 && pending[0].operation_id == p.operation_id,
            },
            "pending control does not match its receipt"
        );
        Ok(())
    }

    fn keys(&self) -> BTreeSet<String> {
        self.followed
            .iter()
            .map(|c| fold(c))
            .chain(self.membership.keys().cloned())
            .chain(self.refusals.keys().cloned())
            .chain(self.quarantine.keys().cloned())
            .chain(self.topics.keys().cloned())
            .collect()
    }

    fn room_for(&self, key: &str) -> bool {
        let keys = self.keys();
        keys.contains(key) || keys.len() < MAX_CHANNELS
    }

    fn is_followed(&self, key: &str) -> bool {
        self.followed.iter().any(|c| fold(c) == key)
    }

    fn follow(&mut self, channel: &str, on: bool) {
        let key = fold(channel);
        self.followed.retain(|c| fold(c) != key);
        if on {
            self.followed.push(channel.to_owned());
            self.followed.sort_by_cached_key(|c| fold(c));
        }
    }

    /// Membership on the live connection: a new connection is in no channel.
    pub fn presence(&self, channel: &str) -> Presence {
        self.membership
            .get(&fold(channel))
            .filter(|m| m.epoch == self.live.epoch)
            .map_or(Presence::Absent, |m| m.state)
    }

    pub fn joined(&self, channel: &str) -> bool {
        self.presence(channel) == Presence::Joined
    }

    /// Followed channels attempted at startup; quarantined targets wait for
    /// an explicit join or part.
    pub fn startup(&self) -> Vec<String> {
        self.followed
            .iter()
            .filter(|c| !self.quarantine.contains_key(&fold(c)))
            .cloned()
            .collect()
    }

    /// Resolve a control left pending by an earlier process or connection.
    /// Its outcome is unknown; membership targets stay quarantined.
    pub fn interrupt(&mut self, why: &str) -> Option<String> {
        let pending = self.pending.take()?;
        let receipt = self
            .receipts
            .iter_mut()
            .find(|r| r.operation_id == pending.operation_id)?;
        receipt.outcome = Outcome::Uncertain;
        receipt.detail = bounded(
            &format!("{why}; outcome unknown and nothing was retried"),
            MAX_DETAIL,
        );
        let (kind, channel) = (receipt.kind, receipt.channel.clone());
        let id = pending.operation_id;
        if kind != Kind::Topic {
            let key = fold(&channel);
            self.quarantine.insert(
                key.clone(),
                Quarantine {
                    channel: channel.clone(),
                    reason: format!("interrupted {} control", kind_name(kind)),
                    operation_id: Some(id.clone()),
                },
            );
            if !self.live.epoch.is_empty() {
                self.membership.insert(
                    key,
                    Membership {
                        channel: channel.clone(),
                        epoch: self.live.epoch.clone(),
                        state: Presence::Unknown,
                    },
                );
            }
        }
        self.live.dirty = true;
        Some(format!(
            "operation_id={id} {} {channel}: {why}; outcome uncertain, nothing was retried{}",
            kind_name(kind),
            if kind == Kind::Topic {
                ""
            } else {
                "; the channel is quarantined until an explicit airc join or part"
            }
        ))
    }

    /// Start a connection's evidence. Earlier membership and topics prove nothing.
    pub fn reset(&mut self, epoch: &str, nick: &str, account: &str, server: &str) {
        self.membership.clear();
        self.topics.clear();
        self.live = Live {
            epoch: epoch.into(),
            nick: nick.into(),
            account: account.into(),
            server: server.into(),
            ..Live::default()
        };
    }

    /// Record startup JOIN outcomes. Joined targets complete setup when the
    /// buffered initial numerics are tracked; refusals keep the preference.
    pub fn started(&mut self, joined: &[String], refusals: &[JoinRefusal]) -> Vec<String> {
        for channel in joined {
            let key = fold(channel);
            self.set_presence(channel, Presence::Joined);
            self.refusals.remove(&key);
            self.live.setup.insert(
                key,
                Setup::Pending {
                    until: Instant::now() + SETUP_TIMEOUT,
                    newer: false,
                },
            );
        }
        refusals
            .iter()
            .map(|refusal| {
                self.refuse(&refusal.channel, &refusal.code, &refusal.reason);
                format!(
                    "{}: JOIN refused by server {}: {}; follow preference kept, not joined",
                    refusal.channel,
                    refusal.code,
                    bounded(&refusal.reason, MAX_REASON)
                )
            })
            .collect()
    }

    fn set_presence(&mut self, channel: &str, state: Presence) {
        let key = fold(channel);
        if state == Presence::Absent {
            self.membership.remove(&key);
        } else {
            self.membership.insert(
                key,
                Membership {
                    channel: channel.into(),
                    epoch: self.live.epoch.clone(),
                    state,
                },
            );
        }
        self.live.dirty = true;
    }

    fn refuse(&mut self, channel: &str, code: &str, reason: &str) {
        let key = fold(channel);
        self.set_presence(channel, Presence::Absent);
        self.refusals.insert(
            key,
            Refusal {
                channel: channel.into(),
                epoch: self.live.epoch.clone(),
                code: bounded(code, 16),
                reason: bounded(reason, MAX_REASON),
            },
        );
    }

    /// Absent on this connection: setup, topic evidence and history end.
    fn leave(&mut self, channel: &str) -> Option<Notice> {
        let key = fold(channel);
        self.set_presence(channel, Presence::Absent);
        self.topics.remove(&key);
        self.live.setup.remove(&key);
        self.live.history.cancel(&key)
    }

    fn set_topic(&mut self, channel: &str, text: &str) {
        let known = valid_topic(text);
        self.topics.insert(
            fold(channel),
            Topic {
                channel: channel.into(),
                epoch: self.live.epoch.clone(),
                known,
                text: if known { text.into() } else { String::new() },
            },
        );
        self.live.dirty = true;
    }

    /// Update evidence from one live inbound event. Authority comes only from
    /// the authenticated prefix (nick and account) or the registered server.
    pub fn track(&mut self, m: &Message) -> (Change, Vec<Notice>) {
        let own = own_prefix(m, &self.live.nick, &self.live.account);
        let numeric = server_numeric(m, &self.live.server, &self.live.nick);
        let at = |index: usize| m.params.get(index).filter(|c| valid_channel(c)).cloned();
        let mut notices = Vec::new();
        let change = match m.command.as_str() {
            "JOIN" if own => match at(0) {
                Some(channel) if !self.joined(&channel) && self.room_for(&fold(&channel)) => {
                    self.set_presence(&channel, Presence::Joined);
                    self.live.setup.insert(
                        fold(&channel),
                        Setup::Pending {
                            until: Instant::now() + SETUP_TIMEOUT,
                            newer: false,
                        },
                    );
                    Change::Joined(channel)
                }
                _ => Change::None,
            },
            "PART" if own => {
                if let Some(channel) = at(0).filter(|c| self.joined(c)) {
                    notices.extend(self.leave(&channel));
                }
                Change::None
            }
            "KICK"
                if m.params
                    .get(1)
                    .is_some_and(|nick| fold(nick) == fold(&self.live.nick)) =>
            {
                match at(0).filter(|c| self.joined(c)) {
                    Some(channel) => {
                        notices.extend(self.leave(&channel));
                        let reason = bounded(
                            &format!(
                                "removed by KICK from {}: {}",
                                m.prefix.as_deref().unwrap_or("server"),
                                m.params.get(2).map_or("", String::as_str)
                            ),
                            MAX_REASON,
                        );
                        let key = fold(&channel);
                        self.refusals.insert(
                            key.clone(),
                            Refusal {
                                channel: channel.clone(),
                                epoch: self.live.epoch.clone(),
                                code: "KICK".into(),
                                reason: reason.clone(),
                            },
                        );
                        self.quarantine.insert(
                            key,
                            Quarantine {
                                channel: channel.clone(),
                                reason,
                                operation_id: None,
                            },
                        );
                        Change::Kicked(channel)
                    }
                    None => Change::None,
                }
            }
            "TOPIC"
                if m.params.len() == 2 && m.prefix.as_deref().is_some_and(|p| p.contains('!')) =>
            {
                if let Some(channel) = at(0).filter(|c| self.joined(c)) {
                    self.set_topic(&channel, &m.params[1]);
                    if let Some(Setup::Pending { newer, .. }) =
                        self.live.setup.get_mut(&fold(&channel))
                    {
                        *newer = true;
                    }
                }
                Change::None
            }
            // Initial JOIN topic: only before 366, and never over a newer TOPIC.
            "331" | "332" if numeric => {
                if let Some(channel) = at(1).filter(|c| {
                    matches!(
                        self.live.setup.get(&fold(c)),
                        Some(Setup::Pending { newer: false, .. })
                    )
                }) {
                    let text = match m.command.as_str() {
                        "332" => m.params.get(2).map_or("", String::as_str),
                        _ => "",
                    };
                    self.set_topic(&channel, text);
                }
                Change::None
            }
            "366" if numeric => {
                if let Some(channel) = at(1)
                    && let Some(setup) = self.live.setup.get_mut(&fold(&channel))
                    && matches!(setup, Setup::Pending { .. })
                {
                    *setup = Setup::Complete;
                }
                Change::None
            }
            _ => Change::None,
        };
        (change, notices)
    }

    /// Mark setups that missed their bound unknown; a late 366 cannot clear it.
    pub fn expire(&mut self, now: Instant) -> Vec<String> {
        let expired = self
            .live
            .setup
            .iter_mut()
            .filter(|(_, s)| matches!(s, Setup::Pending { until, .. } if now >= *until))
            .map(|(key, setup)| {
                *setup = Setup::Unknown;
                key.clone()
            })
            .collect::<Vec<_>>();
        expired
            .into_iter()
            .filter_map(|key| {
                let channel = self.membership.get(&key)?.channel.clone();
                self.topics.insert(
                    key,
                    Topic {
                        channel: channel.clone(),
                        epoch: self.live.epoch.clone(),
                        known: false,
                        text: String::new(),
                    },
                );
                self.live.dirty = true;
                Some(format!(
                    "{channel}: JOIN setup did not end within {} s; topic and roster evidence are unknown and topic queries are refused until an explicit reconnect",
                    SETUP_TIMEOUT.as_secs()
                ))
            })
            .collect()
    }

    /// Bind a control to its operation ID, deciding it locally when possible.
    /// A repeated identical ID returns its saved outcome and is never resent.
    pub fn begin(&mut self, request: Request, busy: Option<&str>) -> Result<(Begin, Vec<String>)> {
        if let Some(saved) = self
            .receipts
            .iter()
            .find(|r| r.operation_id == request.operation_id)
        {
            let (ok, text) = if !saved.binds(&request) {
                (
                    false,
                    "operation_id is already bound to a different request; nothing was sent".into(),
                )
            } else {
                match saved.outcome {
                    Outcome::Pending => (
                        false,
                        "this operation is still pending; nothing was resent".into(),
                    ),
                    outcome => (outcome == Outcome::Confirmed, saved.detail.clone()),
                }
            };
            return Ok((
                Begin::Done {
                    ok,
                    text: format!("{text}; operation_id={}", request.operation_id),
                },
                Vec::new(),
            ));
        }
        let channel = request.channel.clone();
        let key = fold(&channel);
        let unknown = format!(
            "not sent: membership of {channel} is unknown on this connection; disconnect and reconnect explicitly to resolve it"
        );
        let decided = if let Some(why) = busy {
            Some((Outcome::Refused, format!("not sent: {why}")))
        } else {
            match (request.kind, self.presence(&channel)) {
                (Kind::Join, Presence::Joined) => {
                    self.follow(&channel, true);
                    self.refusals.remove(&key);
                    self.quarantine.remove(&key);
                    Some((Outcome::Confirmed, format!("already joined {channel}")))
                }
                (Kind::Part, Presence::Absent) => {
                    self.follow(&channel, false);
                    self.refusals.remove(&key);
                    self.quarantine.remove(&key);
                    Some((Outcome::Confirmed, format!("already absent from {channel}")))
                }
                (_, Presence::Unknown) => Some((Outcome::Refused, unknown)),
                (Kind::Join, Presence::Absent) if !self.room_for(&key) => Some((
                    Outcome::Refused,
                    format!("not sent: at most {MAX_CHANNELS} channels are tracked"),
                )),
                (Kind::Topic, presence) if presence != Presence::Joined => Some((
                    Outcome::Refused,
                    format!("not sent: not joined to {channel} on this connection"),
                )),
                (Kind::Topic, _) if request.topic.is_none() => match self.live.setup.get(&key) {
                    Some(Setup::Pending { .. }) => Some((
                        Outcome::Refused,
                        format!(
                            "not sent: JOIN setup for {channel} is still pending; a topic query waits for its end of NAMES"
                        ),
                    )),
                    Some(Setup::Unknown) => Some((
                        Outcome::Refused,
                        format!(
                            "not sent: JOIN setup for {channel} did not complete; topic queries are refused until an explicit reconnect"
                        ),
                    )),
                    _ => None,
                },
                _ => None,
            }
        };
        let message = match decided {
            None => Some(request.message()?),
            Some(_) => None,
        };
        let (outcome, detail) = decided.unwrap_or((
            Outcome::Pending,
            "pending: sent once, awaiting the server".into(),
        ));
        let mut notices = Vec::new();
        if self.receipts.len() >= MAX_CONTROLS
            && let Some(index) = self
                .receipts
                .iter()
                .position(|r| r.outcome != Outcome::Pending)
            && let Some(old) = self.receipts.remove(index)
            && old.outcome == Outcome::Uncertain
        {
            notices.push(format!(
                "An uncertain channel control receipt (operation_id={}) expired from the {MAX_CONTROLS}-entry journal; inspect channel state before repeating it.",
                old.operation_id
            ));
        }
        let id = request.operation_id.clone();
        self.receipts.push_back(Control {
            operation_id: request.operation_id,
            epoch: self.live.epoch.clone(),
            kind: request.kind,
            channel: channel.clone(),
            topic: request.topic,
            outcome,
            detail: bounded(&detail, MAX_DETAIL),
        });
        self.live.dirty = true;
        let Some(message) = message else {
            return Ok((
                Begin::Done {
                    ok: outcome == Outcome::Confirmed,
                    text: format!("{detail}; operation_id={id}"),
                },
                notices,
            ));
        };
        // The intent is saved before any network write; a refusal restores it.
        self.pending = Some(Pending {
            operation_id: id,
            followed: self.is_followed(&key),
        });
        match request.kind {
            Kind::Join => {
                self.quarantine.remove(&key);
                self.follow(&channel, true);
            }
            Kind::Part => self.follow(&channel, false),
            Kind::Topic => {}
        }
        Ok((Begin::Send(message), notices))
    }

    /// The pending control could not be queued; nothing reached the network.
    pub fn unsent(&mut self, why: &str) -> Option<(bool, String)> {
        self.settle(Outcome::Refused, format!("not sent: {why}"))
    }

    fn settle(&mut self, outcome: Outcome, detail: String) -> Option<(bool, String)> {
        let pending = self.pending.take()?;
        let receipt = self
            .receipts
            .iter_mut()
            .find(|r| r.operation_id == pending.operation_id)?;
        receipt.outcome = outcome;
        receipt.detail = bounded(&detail, MAX_DETAIL);
        let (kind, channel) = (receipt.kind, receipt.channel.clone());
        if kind != Kind::Topic {
            let key = fold(&channel);
            if outcome == Outcome::Confirmed {
                self.refusals.remove(&key);
                self.quarantine.remove(&key);
            } else {
                self.follow(&channel, pending.followed);
            }
        }
        self.live.dirty = true;
        Some((
            outcome == Outcome::Confirmed,
            format!("{detail}; operation_id={}", pending.operation_id),
        ))
    }

    /// Settle the pending control if this event is its correlated outcome.
    /// Call after `track`, which records the membership change itself.
    pub fn resolve(&mut self, m: &Message) -> Option<(bool, String)> {
        let pending = self.pending.as_ref()?;
        let receipt = self
            .receipts
            .iter()
            .find(|r| r.operation_id == pending.operation_id)?;
        let (kind, channel, topic) = (receipt.kind, receipt.channel.clone(), receipt.topic.clone());
        let key = fold(&channel);
        let own = own_prefix(m, &self.live.nick, &self.live.account);
        let server = server_numeric(m, &self.live.server, &self.live.nick);
        let numeric = server && m.params.get(1).is_some_and(|c| fold(c) == key);
        let first = m.params.first().is_some_and(|c| fold(c) == key);
        let command = m.command.as_str();
        let refused = || {
            format!(
                "refused by server {command}: {}",
                bounded(
                    &m.params
                        .iter()
                        .skip(2)
                        .cloned()
                        .collect::<Vec<_>>()
                        .join(" "),
                    MAX_REASON
                )
            )
        };
        match kind {
            Kind::Join if own && command == "JOIN" && first => {
                self.settle(Outcome::Confirmed, format!("joined {channel}"))
            }
            Kind::Join if numeric && JOIN_REFUSALS.contains(&command) => {
                let reason = m.params.get(2).cloned().unwrap_or_default();
                let detail = refused();
                self.refuse(&channel, command, &reason);
                self.settle(Outcome::Refused, detail)
            }
            Kind::Part if own && command == "PART" && first => {
                self.settle(Outcome::Confirmed, format!("left {channel}"))
            }
            Kind::Part if numeric && matches!(command, "442" | "403") => {
                // The server reports this connection absent from the channel.
                let detail = refused();
                if self.joined(&channel) {
                    self.leave(&channel);
                }
                self.settle(Outcome::Refused, format!("{detail}; now absent"))
            }
            Kind::Topic if topic.is_none() && numeric && matches!(command, "331" | "332") => {
                let text = match command {
                    "332" => m.params.get(2).cloned().unwrap_or_default(),
                    _ => String::new(),
                };
                self.set_topic(&channel, &text);
                self.settle(
                    Outcome::Confirmed,
                    if text.is_empty() {
                        format!("no topic is set for {channel}")
                    } else {
                        format!("topic for {channel}: {}", bounded(&text, MAX_TOPIC))
                    },
                )
            }
            Kind::Topic
                if own
                    && command == "TOPIC"
                    && first
                    && m.params.len() == 2
                    && topic.as_ref() == m.params.get(1) =>
            {
                let cleared = topic.as_deref() == Some("");
                self.settle(
                    Outcome::Confirmed,
                    format!(
                        "topic {} for {channel}",
                        if cleared { "cleared" } else { "set" }
                    ),
                )
            }
            Kind::Topic if numeric && matches!(command, "403" | "442" | "482") => {
                self.settle(Outcome::Refused, refused())
            }
            Kind::Topic if topic.is_some() && server && command == "417" => {
                self.settle(Outcome::Refused, refused())
            }
            _ => None,
        }
    }

    /// Lines for `airc status`. Stopped sessions prove no membership.
    pub fn describe(&self, running: bool, out: &mut String, safe: impl Fn(&str) -> String) {
        for key in self.keys() {
            let display = self
                .followed
                .iter()
                .find(|c| fold(c) == key)
                .or_else(|| self.membership.get(&key).map(|m| &m.channel))
                .or_else(|| self.refusals.get(&key).map(|r| &r.channel))
                .or_else(|| self.quarantine.get(&key).map(|q| &q.channel))
                .or_else(|| self.topics.get(&key).map(|t| &t.channel))
                .cloned()
                .unwrap_or_else(|| key.clone());
            let mut line = format!(
                "channel {}: {}; ",
                safe(&display),
                if self.is_followed(&key) {
                    "followed"
                } else {
                    "not followed"
                }
            );
            if !running || self.live.epoch.is_empty() {
                line.push_str("membership unknown while not connected");
            } else {
                let presence = self.presence(&display);
                line.push_str(match presence {
                    Presence::Joined => "joined",
                    Presence::Absent => "absent",
                    Presence::Unknown => "membership unknown",
                });
                if presence == Presence::Joined {
                    line.push_str(match self.live.setup.get(&key) {
                        Some(Setup::Pending { .. }) => "; setup pending",
                        Some(Setup::Unknown) => "; setup unknown",
                        Some(Setup::Complete) => "; setup complete",
                        None => "",
                    });
                    if let Some(history) = self.live.history.status(&key) {
                        line.push_str(&format!("; history {}", safe(history)));
                    }
                    match self.topics.get(&key) {
                        Some(t) if t.known && t.text.is_empty() => line.push_str("; no topic"),
                        Some(t) if t.known => line.push_str(&format!("; topic: {}", safe(&t.text))),
                        _ => line.push_str("; topic unknown"),
                    }
                }
            }
            if let Some(r) = self.refusals.get(&key) {
                line.push_str(&format!(
                    "; refused {} (epoch={}): {}",
                    safe(&r.code),
                    r.epoch,
                    safe(&r.reason)
                ));
            }
            if let Some(q) = self.quarantine.get(&key) {
                line.push_str(&format!("; quarantined: {}", safe(&q.reason)));
                if let Some(id) = &q.operation_id {
                    line.push_str(&format!(" (operation_id={id})"));
                }
            }
            out.push_str(&line);
            out.push('\n');
        }
        for r in self.receipts.iter().rev() {
            let topic = match (r.kind, r.topic.as_deref()) {
                (Kind::Topic, None) => " query",
                (Kind::Topic, Some("")) => " clear",
                (Kind::Topic, Some(_)) => " set",
                _ => "",
            };
            let outcome = match r.outcome {
                Outcome::Pending => "pending",
                Outcome::Confirmed => "confirmed",
                Outcome::Refused => "refused",
                Outcome::Uncertain => "uncertain",
            };
            out.push_str(&format!(
                "operation_id={} {}{topic} {} (epoch={}): {outcome}: {}\n",
                r.operation_id,
                kind_name(r.kind),
                safe(&r.channel),
                r.epoch,
                safe(&r.detail)
            ));
        }
    }
}

fn kind_name(kind: Kind) -> &'static str {
    match kind {
        Kind::Join => "join",
        Kind::Part => "part",
        Kind::Topic => "topic",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const A: &str = "0123456789abcdef0123456789abcdef";
    const B: &str = "fedcba9876543210fedcba9876543210";

    fn live() -> Channels {
        let mut channels = Channels::seeded(&["#work".into()]).unwrap();
        channels.reset("e1", "Alice", "alice", "test.airc");
        channels
    }

    fn from(prefix: &str, line: &str) -> Message {
        let mut m = Message::parse(line).unwrap();
        m.prefix = Some(prefix.into());
        m
    }

    fn me(line: &str) -> Message {
        from("Alice!alice@airc", line)
    }

    fn server(line: &str) -> Message {
        from("test.airc", line)
    }

    fn request(id: &str, kind: Kind, channel: &str, topic: Option<&str>) -> Request {
        Request {
            operation_id: id.into(),
            kind,
            channel: channel.into(),
            topic: topic.map(Into::into),
        }
    }

    fn send(channels: &mut Channels, request: Request) -> Message {
        match channels.begin(request, None).unwrap().0 {
            Begin::Send(message) => message,
            Begin::Done { text, .. } => panic!("decided locally: {text}"),
        }
    }

    fn done(channels: &mut Channels, request: Request) -> (bool, String) {
        match channels.begin(request, None).unwrap().0 {
            Begin::Done { ok, text } => (ok, text),
            Begin::Send(message) => panic!("sent {message:?}"),
        }
    }

    /// Track, then resolve, in the worker's order.
    fn observe(channels: &mut Channels, m: &Message) -> Option<(bool, String)> {
        channels.track(m);
        channels.resolve(m)
    }

    fn join(channels: &mut Channels, id: &str, channel: &str) {
        send(channels, request(id, Kind::Join, channel, None));
        let joined = observe(channels, &me(&format!("JOIN {channel}"))).unwrap();
        assert!(joined.0, "{joined:?}");
    }

    #[test]
    fn saved_state_is_strict_bounded_and_keeps_an_explicit_empty_follow_set() {
        let empty = Channels::seeded(&[]).unwrap();
        let json = serde_json::to_value(&empty).unwrap();
        assert_eq!(json["followed"], serde_json::json!([]));
        let restored: Channels = serde_json::from_value(json.clone()).unwrap();
        assert!(restored.followed.is_empty() && restored.startup().is_empty());
        let mut unknown = json.clone();
        unknown["extra"] = serde_json::json!(1);
        assert!(serde_json::from_value::<Channels>(unknown).is_err());
        let mut missing = json;
        missing.as_object_mut().unwrap().remove("followed");
        assert!(serde_json::from_value::<Channels>(missing).is_err());
        let mut channels = Channels::seeded(&["#B".into(), "#a".into()]).unwrap();
        assert_eq!(
            channels.followed,
            ["#a", "#B"],
            "sorted by fold, spelling kept"
        );
        channels.followed.push("#A".into());
        assert!(channels.validate().is_err(), "duplicate by fold");
        let many = (0..=MAX_CHANNELS)
            .map(|n| format!("#c{n}"))
            .collect::<Vec<_>>();
        assert!(Channels::seeded(&many).is_err());
        let mut receipt = serde_json::to_value(Control {
            operation_id: A.into(),
            epoch: "e1".into(),
            kind: Kind::Topic,
            channel: "#work".into(),
            topic: None,
            outcome: Outcome::Confirmed,
            detail: String::new(),
        })
        .unwrap();
        assert!(receipt.get("topic").is_none(), "a query omits topic");
        receipt["topic"] = serde_json::Value::Null;
        assert!(serde_json::from_value::<Control>(receipt).is_err());
    }

    #[test]
    fn controls_validate_operation_ids_and_topic_text_before_any_state_change() {
        for (id, kind, channel, topic) in [
            (
                "0123456789ABCDEF0123456789ABCDEF",
                Kind::Join,
                "#work",
                None,
            ),
            ("short", Kind::Part, "#work", None),
            (A, Kind::Join, "work", None),
            (A, Kind::Join, "#work", Some("text")),
            (A, Kind::Topic, "#work", Some("line\nbreak")),
        ] {
            assert!(request(id, kind, channel, topic).validate().is_err());
        }
        assert!(
            request(A, Kind::Topic, "#work", Some(&"é".repeat(101)))
                .validate()
                .is_err()
        );
        assert!(
            request(A, Kind::Topic, "#work", Some(&"é".repeat(100)))
                .validate()
                .is_ok()
        );
        assert!(
            request(A, Kind::Topic, "#work", Some(""))
                .validate()
                .is_ok()
        );
    }

    #[test]
    fn join_and_part_are_idempotent_from_confirmed_membership_only() {
        let mut c = live();
        assert_eq!(
            c.presence("#work"),
            Presence::Absent,
            "a new connection is in no channel"
        );
        let wire = send(&mut c, request(A, Kind::Join, "#Work", None));
        assert_eq!(wire.params, ["#Work"]);
        // Another connection of the same account, or a forged nick, cannot confirm.
        for forged in [
            from("Alice2!alice@airc", "JOIN #work"),
            from("Alice!mallory@airc", "JOIN #work"),
            me("JOIN #other"),
        ] {
            assert!(observe(&mut c, &forged).is_none());
        }
        let (ok, text) = observe(&mut c, &me("JOIN #work")).unwrap();
        assert!(
            ok && text.contains("joined #Work") && text.contains(A),
            "{text}"
        );
        let (ok, text) = done(&mut c, request(B, Kind::Join, "#work", None));
        assert!(ok && text.contains("already joined"), "{text}");
        assert!(
            c.pending.is_none(),
            "no wire output, no duplicate JOIN to await"
        );
        // A repeated identical ID returns the saved outcome; a conflicting one is refused.
        let (ok, text) = done(&mut c, request(A, Kind::Join, "#WORK", None));
        assert!(ok && text.contains("joined #Work"), "{text}");
        let (ok, text) = done(&mut c, request(A, Kind::Part, "#work", None));
        assert!(!ok && text.contains("different request"), "{text}");
        send(
            &mut c,
            request("1".repeat(32).as_str(), Kind::Part, "#work", None),
        );
        assert!(c.followed.is_empty(), "intent is saved before the wire");
        let (ok, _) = observe(&mut c, &me("PART #work")).unwrap();
        assert!(ok && !c.joined("#work"));
        let (ok, text) = done(&mut c, request(&"2".repeat(32), Kind::Part, "#work", None));
        assert!(ok && text.contains("already absent"), "{text}");
        c.validate().unwrap();
    }

    #[test]
    fn refusal_restores_the_previous_follow_preference_and_records_evidence() {
        let mut c = live();
        send(&mut c, request(A, Kind::Join, "#denied", None));
        assert!(c.followed.iter().any(|f| f == "#denied"));
        // Another channel's or another recipient's refusal cannot settle it.
        for unrelated in [
            server("474 Alice #other :Cannot join channel (+b)"),
            server("474 Bob #denied :Cannot join channel (+b)"),
            from("evil.airc", "474 Alice #denied :forged origin"),
        ] {
            assert!(observe(&mut c, &unrelated).is_none());
        }
        let (ok, text) =
            observe(&mut c, &server("403 Alice #denied :No accessible channel")).unwrap();
        assert!(!ok && text.contains("403"), "{text}");
        assert_eq!(
            c.followed,
            ["#work"],
            "refusal restores the prior preference"
        );
        assert_eq!(c.refusals["#denied"].code, "403");
        assert_eq!(c.presence("#denied"), Presence::Absent);
        // PART of a known-absent refused target clears it without the wire.
        let (ok, _) = done(&mut c, request(B, Kind::Part, "#denied", None));
        assert!(ok && !c.refusals.contains_key("#denied"));
    }

    #[test]
    fn kick_preserves_preference_quarantines_and_requires_an_explicit_choice() {
        let mut c = live();
        join(&mut c, A, "#work");
        c.track(&server("366 Alice #work :End of NAMES"));
        let (change, _) = c.track(&from("test.airc", "KICK #work Alice :Membership revoked"));
        assert_eq!(change, Change::Kicked("#work".into()));
        assert_eq!(c.followed, ["#work"], "preference kept for inspection");
        assert!(c.quarantine.contains_key("#work") && c.startup().is_empty());
        assert!(!c.joined("#work") && !c.topics.contains_key("#work"));
        // An explicit join clears the quarantine and sends exactly once.
        let wire = send(&mut c, request(B, Kind::Join, "#work", None));
        assert_eq!(wire.command, "JOIN");
        assert!(!c.quarantine.contains_key("#work"));
        observe(&mut c, &server("403 Alice #work :No accessible channel")).unwrap();
        assert_eq!(c.followed, ["#work"]);
        assert!(
            c.startup() == ["#work"],
            "explicit join resolved the quarantine"
        );
    }

    #[test]
    fn delayed_join_setup_refuses_topic_queries_and_keeps_newer_topic_evidence() {
        let mut c = live();
        join(&mut c, A, "#work");
        // Review schedule 1: an immediate query is not sent before 366.
        let (ok, text) = done(&mut c, request(B, Kind::Topic, "#work", None));
        assert!(
            !ok && text.contains("setup") && text.contains("still pending"),
            "{text}"
        );
        // A newer authenticated topic arrives before the stale initial numeric.
        c.track(&from("Op!op@airc", "TOPIC #work :T1 newer"));
        for stale in [
            server("332 Alice #work :T0 stale"),
            server("331 Alice #work :No topic"),
        ] {
            c.track(&stale);
            assert_eq!(c.topics["#work"].text, "T1 newer");
        }
        c.track(&server("366 Alice #work :End of NAMES"));
        assert_eq!(
            c.topics["#work"].text, "T1 newer",
            "366 does not reset newer evidence"
        );
        assert_eq!(c.live.setup["#work"], Setup::Complete);
        // After the barrier, a query is sent and only a matching numeric settles it.
        let id = "3".repeat(32);
        assert_eq!(
            send(&mut c, request(&id, Kind::Topic, "#work", None)).params,
            ["#work"]
        );
        for wrong in [
            server("332 Alice #other :wrong channel"),
            server("332 Bob #work :wrong recipient"),
            from("Op!op@airc", "TOPIC #work :unsolicited update"),
        ] {
            assert!(observe(&mut c, &wrong).is_none());
        }
        let (ok, text) = observe(&mut c, &server("332 Alice #work :T2 current")).unwrap();
        assert!(ok && text.contains("T2 current"), "{text}");
        assert_eq!(c.topics["#work"].text, "T2 current");
    }

    #[test]
    fn empty_initial_topic_is_known_until_a_newer_set_and_a_set_does_not_end_setup() {
        let mut c = live();
        join(&mut c, A, "#work");
        c.track(&server("331 Alice #work :No topic"));
        assert!(c.topics["#work"].known && c.topics["#work"].text.is_empty());
        let set = send(&mut c, request(B, Kind::Topic, "#work", Some("")));
        assert_eq!(set.params, ["#work", ""]);
        assert!(observe(&mut c, &from("Alice2!alice@airc", "TOPIC #work :")).is_none());
        let (ok, text) = observe(&mut c, &me("TOPIC #work :")).unwrap();
        assert!(ok && text.contains("cleared"), "{text}");
        assert!(matches!(c.live.setup["#work"], Setup::Pending { .. }));
    }

    #[test]
    fn setup_timeout_marks_evidence_unknown_and_a_late_terminator_cannot_clear_it() {
        let mut c = live();
        join(&mut c, A, "#work");
        let notices = c.expire(Instant::now() + SETUP_TIMEOUT);
        assert_eq!(notices.len(), 1, "{notices:?}");
        assert!(!c.topics["#work"].known);
        c.track(&server("332 Alice #work :late"));
        c.track(&server("366 Alice #work :End of NAMES"));
        assert_eq!(c.live.setup["#work"], Setup::Unknown);
        assert!(!c.topics["#work"].known);
        let (ok, text) = done(&mut c, request(B, Kind::Topic, "#work", None));
        assert!(!ok && text.contains("did not complete"), "{text}");
        assert!(
            c.joined("#work"),
            "missing setup data does not imply absence"
        );
    }

    #[test]
    fn interrupted_controls_are_uncertain_and_quarantine_membership_targets() {
        let mut c = live();
        join(&mut c, A, "#work");
        send(&mut c, request(B, Kind::Part, "#work", None));
        let notice = c.interrupt("connection ended before confirmation").unwrap();
        assert!(
            notice.contains(B) && notice.contains("quarantined"),
            "{notice}"
        );
        assert_eq!(c.presence("#work"), Presence::Unknown);
        let (ok, text) = done(&mut c, request(&"4".repeat(32), Kind::Join, "#work", None));
        assert!(!ok && text.contains("unknown"), "{text}");
        let (ok, text) = done(&mut c, request(B, Kind::Part, "#work", None));
        assert!(!ok && text.contains("outcome unknown"), "{text}");
        // A new connection starts absent; the uncertain PART is not replayed.
        c.reset("e2", "Alice", "alice", "test.airc");
        assert!(c.startup().is_empty() && c.followed.is_empty());
        let (ok, _) = done(&mut c, request(&"5".repeat(32), Kind::Part, "#work", None));
        assert!(ok && c.quarantine.is_empty());
        c.validate().unwrap();
    }

    #[test]
    fn busy_and_full_journals_stay_bounded_without_evicting_the_pending_control() {
        let mut c = live();
        join(&mut c, A, "#work");
        c.track(&server("366 Alice #work :End of NAMES"));
        send(&mut c, request(B, Kind::Topic, "#work", Some("new")));
        for n in 0..40u32 {
            let id = format!("{n:032x}");
            let (begin, _) = c
                .begin(
                    request(&id, Kind::Join, "#work", None),
                    Some("another operation awaits confirmation"),
                )
                .unwrap();
            assert!(matches!(begin, Begin::Done { ok: false, .. }));
        }
        assert_eq!(c.receipts.len(), MAX_CONTROLS);
        assert!(
            c.receipts
                .iter()
                .any(|r| r.operation_id == B && r.outcome == Outcome::Pending)
        );
        c.validate().unwrap();
        let (ok, _) = observe(&mut c, &me("TOPIC #work :new")).unwrap();
        assert!(ok);
    }
}
