//! Durable delivery batches for one consumer, separate from the read cursor.
//!
//! A batch is persisted as pending before any caller sees it. Only an accept
//! (the sink took it) or an explicit drop moves the delivery cursor. Nothing
//! here replays a pending batch on its own: a restarted consumer is refused
//! until the operator names the pending ID.

use super::attention::{Classification, WakeMode};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};

pub(super) const MIN_BYTES: usize = 1024;
pub(super) const MAX_BYTES: usize = 1024 * 1024;
pub(super) const DEFAULT_BYTES: usize = 8 * 1024;

#[derive(Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub(super) struct Delivery {
    /// Last event sequence accepted or dropped; reads never move it.
    pub cursor: u64,
    /// Relevant events above the cursor that were evicted before delivery.
    #[serde(default)]
    pub lost: Lost,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending: Option<Batch>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last: Option<Outcome>,
}

/// Per wake mode, whether an event relevant to it was evicted undelivered.
/// Restored events without a class count as critical, hence relevant.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Lost {
    pub all: bool,
    pub messages: bool,
    pub mentions: bool,
}

impl Lost {
    pub fn merge(&mut self, other: Self) {
        self.all |= other.all;
        self.messages |= other.messages;
        self.mentions |= other.mentions;
    }

    pub fn record(&mut self, class: Classification) {
        self.all |= class.relevant(WakeMode::All);
        self.messages |= class.relevant(WakeMode::Messages);
        self.mentions |= class.relevant(WakeMode::Mentions);
    }

    pub fn everything() -> Self {
        Self {
            all: true,
            messages: true,
            mentions: true,
        }
    }

    pub fn any(self, mode: WakeMode) -> bool {
        match mode {
            WakeMode::All => self.all,
            WakeMode::Messages => self.messages,
            WakeMode::Mentions => self.mentions,
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Batch {
    pub id: String,
    /// Delivery cursor when the batch was made; its events follow it.
    pub after: u64,
    /// Accepting or dropping moves the cursor here.
    pub through: u64,
    pub created: String,
    /// The header up to (excluding) its final ` retry=…]` field.
    pub header: String,
    pub records: String,
    /// Losses this batch reported; restored if it is reverted undelivered.
    #[serde(default)]
    pub lost: Lost,
}

impl Batch {
    pub fn render(&self, retry: bool) -> String {
        format!(
            "{} retry={}]\n{}",
            self.header,
            if retry { "yes" } else { "no" },
            self.records
        )
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Outcome {
    pub id: String,
    /// `accepted` (by the sink, not proof of processing) or `dropped`.
    pub kind: String,
    pub at: String,
}

pub(super) struct Header<'a> {
    pub id: &'a str,
    pub session: &'a str,
    pub account: &'a str,
    pub nick: &'a str,
    pub mode: WakeMode,
    pub events: usize,
    pub context: usize,
    pub bytes: usize,
    pub more: bool,
    pub gap: bool,
}

impl Header<'_> {
    /// Fields up to `retry=`. Callers escape account and nick; a body line can
    /// never start with `[` because continuation lines start with `| `.
    pub fn open(&self) -> String {
        let yes = |flag: bool| if flag { "yes" } else { "no" };
        format!(
            "[airc delivery id={} session={} account={} nick={} wake={} events={} context={} bytes={} more={} gap={}",
            self.id,
            self.session,
            self.account,
            self.nick,
            self.mode.name(),
            self.events,
            self.context,
            self.bytes,
            yes(self.more),
            yes(self.gap)
        )
    }

    /// The longest header these fields can produce, for byte budgeting.
    pub fn bound(&self) -> usize {
        Header {
            events: usize::MAX,
            context: usize::MAX,
            bytes: usize::MAX,
            more: true,
            gap: true,
            ..*self
        }
        .open()
        .len()
            + " retry=yes]\n".len()
    }
}

/// Bounded caller-supplied delivery ID: a UUID as this release prints it.
pub(super) fn validate_id(id: &str) -> Result<()> {
    ensure!(
        uuid::Uuid::parse_str(id).is_ok() && id.len() == 36,
        "invalid delivery ID"
    );
    Ok(())
}

/// The longest prefix of `text` within `limit` bytes that ends on a char boundary.
pub(super) fn prefix(text: &str, limit: usize) -> &str {
    let mut end = text.len().min(limit);
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

/// The local `@N` that starts a rendered message record, if it has one.
pub(super) fn record_reference(text: &str) -> Option<&str> {
    let text = text.strip_prefix("history ").unwrap_or(text);
    let token = text.split(' ').next()?;
    (token.len() > 1 && token.starts_with('@') && token[1..].bytes().all(|b| b.is_ascii_digit()))
        .then_some(token)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn header() -> Header<'static> {
        Header {
            id: "00000000-0000-4000-8000-000000000000",
            session: "space",
            account: "alice",
            nick: "Alice",
            mode: WakeMode::Mentions,
            events: 2,
            context: 5,
            bytes: 80,
            more: false,
            gap: false,
        }
    }

    #[test]
    fn header_is_one_line_and_its_bound_covers_every_value() {
        let h = header();
        let batch = Batch {
            id: h.id.into(),
            after: 0,
            through: 1,
            created: String::new(),
            header: h.open(),
            records: "@1 #work Bob(bob): hi\n".into(),
            lost: Lost::default(),
        };
        let text = batch.render(true);
        assert!(text.starts_with(
            "[airc delivery id=00000000-0000-4000-8000-000000000000 session=space account=alice nick=Alice wake=mentions events=2 context=5 bytes=80 more=no gap=no retry=yes]\n@1 "
        ));
        assert!(
            batch
                .render(false)
                .lines()
                .next()
                .unwrap()
                .ends_with(" retry=no]")
        );
        assert!(h.bound() > text.lines().next().unwrap().len());
    }

    #[test]
    fn prefixes_and_references_respect_utf8_and_record_shape() {
        assert_eq!(prefix("aé", 2), "a");
        assert_eq!(prefix("aé", 3), "aé");
        assert_eq!(prefix("abc", 9), "abc");
        assert_eq!(record_reference("@12 #work A(a): x"), Some("@12"));
        assert_eq!(record_reference("history @3 #work A(a): x"), Some("@3"));
        assert_eq!(record_reference("live-only #work A(a): x"), None);
        assert_eq!(record_reference("! GAP"), None);
        assert!(validate_id("00000000-0000-4000-8000-000000000000").is_ok());
        assert!(validate_id("d12").is_err());
    }

    #[test]
    fn lost_tracks_relevance_per_mode() {
        let mut lost = Lost::default();
        lost.record(Classification::Context);
        assert!(lost.any(WakeMode::All) && !lost.any(WakeMode::Messages));
        lost.record(Classification::Message {
            channel: true,
            addressed: false,
        });
        assert!(lost.any(WakeMode::Messages) && !lost.any(WakeMode::Mentions));
        lost.record(Classification::Critical);
        assert_eq!(lost, Lost::everything());
    }
}
