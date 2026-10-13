use crate::{
    client::ClientConfig,
    protocol::{Message, fold, valid_channel, valid_nick},
};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashSet, VecDeque};

const RETAIN: usize = 512;
const RETAIN_BYTES: usize = 512 * 1024;
pub(super) const SEND_TAG: &str = "+airc-send";

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct State {
    version: u8,
    pub identity: String,
    pub epoch: String,
    pub account: String,
    pub nick: String,
    sequence: u64,
    cursor: u64,
    events: VecDeque<Event>,
    references: VecDeque<Reference>,
    next_reference: u64,
    pub checkpoints: BTreeMap<String, String>,
    pub receipts: VecDeque<Receipt>,
    pub last_read: String,
    pub status: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Event {
    sequence: u64,
    text: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Reference {
    number: u64,
    msgid: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Receipt {
    pub token: String,
    pub target: String,
    pub text: String,
    pub outcome: String,
}

impl State {
    pub fn new(config: &ClientConfig) -> Result<Self> {
        Ok(Self {
            version: 1,
            identity: identity(config)?,
            epoch: uuid::Uuid::new_v4().to_string(),
            account: config.account.clone(),
            nick: config.nick.clone(),
            sequence: 0,
            cursor: 0,
            events: VecDeque::new(),
            references: VecDeque::new(),
            next_reference: 0,
            checkpoints: BTreeMap::new(),
            receipts: VecDeque::new(),
            last_read: "No previous read.\n".into(),
            status: "not connected".into(),
        })
    }

    pub fn validate(&self) -> Result<()> {
        ensure!(self.version == 1, "unsupported session state version");
        ensure!(
            self.events.len() <= RETAIN
                && self.references.len() <= RETAIN
                && self.receipts.len() <= 32
                && self.events.iter().map(|e| e.text.len()).sum::<usize>() <= RETAIN_BYTES
                && self.checkpoints.len() <= 128
                && self.cursor <= self.sequence,
            "invalid session state bounds"
        );
        Ok(())
    }

    pub fn notice(&mut self, text: impl Into<String>) {
        self.record(format!("! {}", safe(&text.into())));
    }

    fn record(&mut self, text: String) {
        self.sequence += 1;
        self.events.push_back(Event {
            sequence: self.sequence,
            text,
        });
        let mut bytes: usize = self.events.iter().map(|e| e.text.len()).sum();
        while self.events.len() > RETAIN || bytes > RETAIN_BYTES {
            bytes -= self.events.pop_front().unwrap().text.len();
        }
    }

    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    fn reference(&mut self, msgid: &str) -> String {
        if let Some(r) = self.references.iter().find(|r| r.msgid == msgid) {
            return format!("@{}", r.number);
        }
        self.next_reference += 1;
        self.references.push_back(Reference {
            number: self.next_reference,
            msgid: msgid.into(),
        });
        if self.references.len() > RETAIN {
            self.references.pop_front();
        }
        format!("@{}", self.next_reference)
    }

    pub fn resolve_reference(&self, reference: &str) -> Result<String> {
        if let Some(n) = reference.strip_prefix('@') {
            let number: u64 = n.parse()?;
            return self
                .references
                .iter()
                .find(|r| r.number == number)
                .map(|r| r.msgid.clone())
                .ok_or_else(|| {
                    anyhow::anyhow!("reply reference has expired or belongs to another session")
                });
        }
        ensure!(
            !reference.is_empty()
                && reference.len() <= 128
                && !reference.chars().any(char::is_whitespace)
                && !reference.chars().any(char::is_control),
            "invalid reply reference"
        );
        Ok(reference.into())
    }

    pub fn remembered_ids(&self) -> HashSet<String> {
        self.references.iter().map(|r| r.msgid.clone()).collect()
    }

    // Called only with decoded messages from the authenticated IRC connection.
    // The +airc-send tag alone is never authority: account, nick, target and
    // text must all agree with the locally recorded operation.
    pub fn ingest(&mut self, message: &Message, history: bool) -> Option<String> {
        let duplicate = message
            .tags
            .get("msgid")
            .is_some_and(|id| self.references.iter().any(|r| r.msgid == *id));
        let mut confirmed = None;
        if message.command == "PRIVMSG" && message.params.len() == 2 {
            let own = message.tags.get("account") == Some(&self.account)
                && message
                    .prefix
                    .as_deref()
                    .and_then(|p| p.split_once('!'))
                    .is_some_and(|(nick, _)| fold(nick) == fold(&self.nick));
            if own
                && let Some(token) = message.tags.get(SEND_TAG)
                && let Some(index) = self.receipts.iter().position(|r| {
                    r.token == *token
                        && fold(&r.target) == fold(&message.params[0])
                        && r.text == message.params[1]
                })
            {
                let reference = message
                    .tags
                    .get("msgid")
                    .map(|id| self.reference(id))
                    .unwrap_or("live-only".into());
                let receipt = &mut self.receipts[index];
                receipt.outcome = format!("confirmed {} {}", receipt.target, reference);
                confirmed = Some(token.clone());
                if history && !duplicate {
                    self.notice(format!("recovered send {token}: confirmed {reference}"));
                }
            }
        }
        if matches!(message.command.as_str(), "PRIVMSG" | "NOTICE") && message.params.len() >= 2 {
            let target = &message.params[0];
            let id = message.tags.get("msgid");
            if !duplicate
                && valid_channel(target)
                && let Some(id) = id
            {
                self.checkpoints.insert(fold(target), id.clone());
            }
            // Locally initiated sends have a compact receipt; retain other own
            // messages (e.g. an earlier JSONL client) as ordinary conversation.
            if !duplicate && confirmed.is_none() {
                let reference = id
                    .map(|id| self.reference(id))
                    .unwrap_or("live-only".into());
                let nick = message
                    .prefix
                    .as_deref()
                    .unwrap_or("server")
                    .split('!')
                    .next()
                    .unwrap_or("?");
                let account = message
                    .tags
                    .get("account")
                    .map(String::as_str)
                    .unwrap_or("unverified");
                let reply = message
                    .tags
                    .get("+reply")
                    .map(|id| {
                        let reference = self
                            .references
                            .iter()
                            .find(|r| r.msgid == *id)
                            .map(|r| format!("@{}", r.number))
                            .unwrap_or_else(|| {
                                let bounded: String = id.chars().take(128).collect();
                                format!(
                                    "msgid:{}{}",
                                    safe(&bounded),
                                    if bounded.len() < id.len() {
                                        "…[truncated]"
                                    } else {
                                        ""
                                    }
                                )
                            });
                        format!(" reply={reference}")
                    })
                    .unwrap_or_default();
                self.record(format!(
                    "{}{} {} {}({}){}: {}",
                    if history { "history " } else { "" },
                    reference,
                    safe(target),
                    safe(nick),
                    safe(account),
                    reply,
                    safe(&message.params[1])
                ));
            }
        } else if matches!(
            message.command.as_str(),
            "JOIN" | "PART" | "QUIT" | "NICK" | "MODE" | "KICK" | "TOPIC"
        ) {
            let prefix = message.prefix.as_deref().unwrap_or("server");
            // Initial own JOIN is routine; peers joining/leaving still matter.
            if message.command != "JOIN" || !prefix.starts_with(&format!("{}!", self.nick)) {
                self.notice(format!(
                    "{} {} {}",
                    message.command,
                    prefix,
                    message.params.join(" ")
                ));
            }
        } else if message.command == "FAIL"
            || message.command == "ERROR"
            || (message.command.starts_with(['4', '5']) && message.command != "422")
        {
            self.notice(format!(
                "server {} {}",
                message.command,
                message.params.join(" ")
            ));
        }
        confirmed
    }

    pub fn has_unread(&self) -> bool {
        self.sequence > self.cursor
    }

    pub fn read(&mut self, limit: usize) -> String {
        let mut text = String::new();
        if let Some(front) = self.events.front()
            && front.sequence > self.cursor + 1
        {
            text.push_str(
                "! GAP: unread events exceeded the 512-event buffer; context is incomplete.\n",
            );
        }
        for event in self
            .events
            .iter()
            .filter(|e| e.sequence > self.cursor)
            .take(limit)
        {
            text.push_str(&event.text);
            text.push('\n');
        }
        if let Some(last) = self
            .events
            .iter()
            .filter(|e| e.sequence > self.cursor)
            .take(limit)
            .last()
        {
            self.cursor = last.sequence;
        }
        if self.has_unread() {
            text.push_str("! More unread events; read again.\n");
        }
        if text.is_empty() {
            text.push_str("No new events.\n");
        }
        self.last_read = text.clone();
        text
    }

    pub fn prepare_send(
        &mut self,
        target: &str,
        text: &str,
        reply: Option<&str>,
    ) -> Result<Message> {
        ensure!(
            valid_channel(target) || valid_nick(target),
            "invalid message target"
        );
        ensure!(!text.is_empty(), "message text is empty");
        let token = uuid::Uuid::new_v4().to_string();
        let mut message = Message::new("PRIVMSG", [target, text]);
        message.tags.insert(SEND_TAG.into(), token.clone());
        if let Some(reply) = reply {
            message
                .tags
                .insert("+reply".into(), self.resolve_reference(reply)?);
        }
        message.encode()?;
        if self.receipts.len() == 32 {
            if self
                .receipts
                .front()
                .is_some_and(|r| r.outcome.starts_with("uncertain"))
            {
                self.notice("An uncertain send receipt expired from the 32-entry journal; inspect server history before resending.");
            }
            self.receipts.pop_front();
        }
        self.receipts.push_back(Receipt {
            token,
            target: target.into(),
            text: text.into(),
            outcome: "uncertain (prepared; delivery not observed)".into(),
        });
        Ok(message)
    }

    pub fn outcome(&self, token: &str) -> String {
        self.receipts
            .iter()
            .find(|r| r.token == token)
            .map(|r| format!("{}; send={}", r.outcome, token))
            .unwrap_or_else(|| {
                "uncertain: receipt expired; inspect history before resending".into()
            })
    }

    pub fn set_outcome(&mut self, token: &str, outcome: String) {
        if let Some(r) = self.receipts.iter_mut().find(|r| r.token == token) {
            r.outcome = outcome;
        }
    }

    pub fn describe(&self, running: bool) -> String {
        let mut text = format!(
            "{}; {}({}); epoch={}; unread={}\n{}\n",
            if running { "connected" } else { "stopped" },
            safe(&self.nick),
            safe(&self.account),
            self.epoch,
            self.sequence - self.cursor,
            if !running && self.status.starts_with("connected") {
                "Previous connection ended without a recorded shutdown; inspect pending send receipts before retrying.".into()
            } else {
                safe(&self.status)
            }
        );
        for receipt in self.receipts.iter().rev() {
            text.push_str(&format!(
                "send={} target={}: {}\n",
                receipt.token,
                safe(&receipt.target),
                safe(&receipt.outcome)
            ));
            if receipt.outcome.starts_with("uncertain") {
                text.push_str(&format!("  text: {}\n", safe(&receipt.text)));
            }
        }
        for (channel, id) in &self.checkpoints {
            text.push_str(&format!(
                "history {} after=msgid={}\n",
                safe(channel),
                safe(id)
            ));
        }
        if !running {
            text.push_str("Reconnect explicitly: airc connect SESSION --config FILE. Nothing will be resent.\n");
        }
        text
    }
}

pub(super) fn identity(config: &ClientConfig) -> Result<String> {
    Ok(format!("{:x}", Sha256::digest(serde_json::to_vec(config)?)))
}

pub(super) fn safe(text: &str) -> String {
    // Use the pinned Rust Unicode tables for non-printing characters, including
    // invisible format/tag characters. Quotes need no escaping in text records.
    text.escape_debug()
        .to_string()
        .replace("\\\"", "\"")
        .replace("\\'", "'")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state() -> State {
        State::new(&ClientConfig {
            host: "localhost".into(),
            port: 6697,
            tls: true,
            allow_insecure: false,
            ca_certificate: None,
            proxy: None,
            account: "alice".into(),
            nick: "Alice".into(),
            token_file: "unused".into(),
            channels: vec!["#work".into()],
            artifact_url: None,
        })
        .unwrap()
    }

    fn chat(text: &str, id: &str) -> Message {
        let mut m = Message::new("PRIVMSG", ["#work", text]);
        m.prefix = Some("Human!human@airc".into());
        m.tags.insert("account".into(), "human".into());
        m.tags.insert("msgid".into(), id.into());
        m
    }

    #[test]
    fn ring_gaps_references_and_replay_are_bounded_without_skipping_pages() {
        let mut state = state();
        for n in 0..600 {
            state.ingest(&chat(&format!("message {n}"), &format!("id-{n}")), false);
        }
        let first = state.read(50);
        assert!(first.contains("GAP"));
        assert!(first.contains("message 88\n") && first.contains("message 137\n"));
        assert!(!first.contains("message 138\n"));
        assert_eq!(state.last_read, first);
        assert!(state.read(50).contains("message 138\n"));
        assert!(state.resolve_reference("@1").is_err());
        assert_eq!(state.resolve_reference("@600").unwrap(), "id-599");
        assert!(state.validate().is_ok());
    }

    #[test]
    fn controls_and_untrusted_identity_cannot_forge_an_output_record() {
        let mut state = state();
        let mut m = chat("hello\n! confirmed\r\x1b[2J\u{2028}\u{202e}", "unique");
        m.tags.insert("+reply".into(), "id\nforged".into());
        state.ingest(&m, false);
        let output = state.read(50);
        assert_eq!(output.lines().count(), 1);
        assert!(output.contains("Human(human)"));
        assert!(output.contains("\\n! confirmed") && !output.contains('\x1b'));
        let mut unverified = chat("no account tag", "other");
        unverified.tags.remove("account");
        state.ingest(&unverified, false);
        assert!(state.read(50).contains("Human(unverified)"));
        let hidden = "\u{e0061}\u{2060}\u{ad}\u{206a}\u{180e}\u{fff9}\u{600}\u{6dd}\u{70f}\u{8e2}";
        assert!(
            safe(hidden).is_ascii(),
            "Unicode format characters must be visible escapes"
        );
        assert_eq!(
            safe("e\u{301} and \"can't\" \\"),
            "e\u{301} and \"can't\" \\\\"
        );
    }

    #[test]
    fn untrusted_reply_metadata_is_distinct_and_cannot_exhaust_persistent_state() {
        let mut state = state();
        let mut m = chat("not a local reference", "first");
        m.tags.insert("+reply".into(), "@1".into());
        state.ingest(&m, false);
        assert!(state.read(50).contains("reply=msgid:@1"));
        for n in 0..512 {
            let mut m = chat(&"\u{7f}".repeat(400), &format!("id-{n}"));
            m.tags.insert("+reply".into(), "\u{7f}".repeat(1000));
            assert!(m.encode().is_ok(), "fixture must fit real wire bounds");
            state.ingest(&m, false);
        }
        let text = state.read(200);
        assert!(text.contains("GAP") && text.contains("[truncated]"));
        assert!(state.validate().is_ok());
        assert!(serde_json::to_vec(&state).unwrap().len() < 4 * 1024 * 1024);
        state.ingest(&chat("still usable", "last"), false);
        assert!(state.read(200).contains("still usable"));
    }

    #[test]
    fn forged_or_mismatched_echo_cannot_confirm_and_recovered_echo_can() {
        let mut state = state();
        let sent = state.prepare_send("#work", "duplicate", None).unwrap();
        let token = sent.tags[SEND_TAG].clone();
        let mut echo = sent.clone();
        echo.prefix = Some("Alice!alice@airc".into());
        echo.tags.insert("account".into(), "human".into());
        echo.tags.insert("msgid".into(), "forgery".into());
        assert!(state.ingest(&echo, false).is_none());
        echo.tags.insert("account".into(), "alice".into());
        echo.prefix = Some("Other!alice@airc".into());
        assert!(state.ingest(&echo, false).is_none());
        echo.prefix = Some("ALICE!alice@airc".into());
        echo.params[1] = "different".into();
        assert!(state.ingest(&echo, false).is_none());
        echo.params[1] = "duplicate".into();
        echo.params[0] = "#other".into();
        assert!(state.ingest(&echo, false).is_none());
        echo.params[0] = "#WORK".into();
        echo.tags.insert("msgid".into(), "real-id".into());
        let persisted = serde_json::to_vec(&state).unwrap();
        let mut recovered: State = serde_json::from_slice(&persisted).unwrap();
        assert!(recovered.outcome(&token).starts_with("uncertain"));
        assert_eq!(recovered.ingest(&echo, true), Some(token.clone()));
        assert!(recovered.outcome(&token).starts_with("confirmed"));
        assert!(recovered.read(50).contains("recovered send"));
        assert_eq!(recovered.checkpoints["#work"], "real-id");
        let old = chat("earlier duplicate", "forgery");
        recovered.ingest(&old, false);
        assert_eq!(
            recovered.checkpoints["#work"], "real-id",
            "replay must not move recovery cursor backwards"
        );
    }

    #[test]
    fn fixed_transcript_reports_bytes_without_claiming_model_token_savings() {
        let mut state = state();
        let mut raw = 0;
        for n in 0..20 {
            let mut message = chat(
                &format!("Test result {n}: all checks passed."),
                &uuid::Uuid::new_v4().to_string(),
            );
            message
                .tags
                .insert("time".into(), "2026-10-13T00:00:00.000Z".into());
            let mut event = serde_json::to_value(&message).unwrap();
            event["cursor"] = serde_json::json!(n + 1);
            raw += serde_json::to_vec(&event).unwrap().len() + 1;
            state.ingest(&message, false);
        }
        let output = state.read(50);
        println!(
            "20-message fixture: JSON event bytes={raw}, compact bytes={}; not a model-token benchmark",
            output.len()
        );
        assert!(output.len() < raw);
        assert_eq!(output.lines().count(), 20);
    }
}
