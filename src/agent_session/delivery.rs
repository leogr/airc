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
/// Upper bound of `--min-interval`, in seconds.
pub(super) const MAX_INTERVAL: u64 = 3600;

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
    /// A compact header up to its optional ` retry` flag and closing `]`; a
    /// saved legacy one (either marker) up to its final ` retry=…]` field.
    pub header: String,
    pub records: String,
    /// Losses this batch reported; restored if it is reverted undelivered.
    #[serde(default)]
    pub lost: Lost,
    /// Present on every compact batch. Absent marks a saved legacy batch, whose
    /// full-field header renders verbatim and which carries no settings notice.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compact: Option<Compact>,
}

/// What a compact batch keeps so a retry or inspection shows the settings it
/// was assembled under, never the current consumer's.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Compact {
    pub settings: Settings,
    /// Whether its first rendering carried the settings notice. Its bytes are
    /// reserved either way, so a retry or inspection still fits `max_bytes`.
    pub notice: bool,
}

/// How a pending batch is rendered.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Rendering {
    /// Its first delivery.
    First,
    /// The single redelivery an operator authorized by ID.
    Retry,
    /// `deliveries --show`: inspection only, never a delivery.
    Inspect,
}

impl Batch {
    pub fn render(&self, rendering: Rendering) -> String {
        let retry = rendering == Rendering::Retry;
        let Some(compact) = &self.compact else {
            let flag = if retry { "yes" } else { "no" };
            return format!("{} retry={flag}]\n{}", self.header, self.records);
        };
        let notice = if compact.notice || rendering != Rendering::First {
            compact.settings.notice()
        } else {
            String::new()
        };
        format!(
            "{}{}]\n{notice}{}",
            self.header,
            if retry { " retry" } else { "" },
            self.records
        )
    }

    pub fn validate(&self) -> bool {
        validate_id(&self.id).is_ok()
            && self.header.len() + self.records.len() <= MAX_BYTES
            && self.compact.as_ref().is_none_or(|c| c.settings.valid())
    }
}

/// The `--wrap` limit a consumer declared; older local clients do not report it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Wrap {
    Unknown,
    Off,
    Bytes(usize),
}

impl std::fmt::Display for Wrap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unknown => f.write_str("unknown"),
            Self::Off => f.write_str("off"),
            Self::Bytes(limit) => write!(f, "{limit}"),
        }
    }
}

/// A consumer's settings: wake mode and byte limit decide each batch; the
/// minimum interval and wrap are reported with it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Options {
    pub wake: WakeMode,
    pub max_bytes: usize,
    pub min_interval: u64,
    pub wrap: Wrap,
}

impl std::fmt::Display for Options {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "wake={} max-bytes={} wrap={} min-interval={}",
            self.wake.name(),
            self.max_bytes,
            self.wrap,
            self.min_interval
        )
    }
}

/// The identity and consumer options one compact batch was assembled under.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Settings {
    /// Escaped by the caller, as every rendered name is.
    pub account: String,
    pub nick: String,
    pub wake: WakeMode,
    pub max_bytes: usize,
    pub min_interval: u64,
    pub wrap: Wrap,
}

impl Settings {
    pub fn new(account: String, nick: String, options: Options) -> Self {
        Self {
            account,
            nick,
            wake: options.wake,
            max_bytes: options.max_bytes,
            min_interval: options.min_interval,
            wrap: options.wrap,
        }
    }

    fn options(&self) -> Options {
        Options {
            wake: self.wake,
            max_bytes: self.max_bytes,
            min_interval: self.min_interval,
            wrap: self.wrap,
        }
    }

    /// The local-notice record that follows the header when it is shown.
    pub fn notice(&self) -> String {
        format!(
            "! delivery settings: account={} nick={} {}\n",
            self.account,
            self.nick,
            self.options()
        )
    }

    fn valid(&self) -> bool {
        let named =
            |name: &str| !name.is_empty() && name.len() <= 128 && !name.contains([' ', '\n']);
        named(&self.account)
            && named(&self.nick)
            && (MIN_BYTES..=MAX_BYTES).contains(&self.max_bytes)
            && self.min_interval <= MAX_INTERVAL
            && match self.wrap {
                Wrap::Bytes(limit) => (MIN_WRAP..=MAX_WRAP).contains(&limit),
                Wrap::Unknown | Wrap::Off => true,
            }
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
    pub events: usize,
    pub context: usize,
    pub more: bool,
    pub gap: bool,
}

impl Header<'_> {
    /// The compact header before its optional ` retry` flag and closing `]`:
    /// `events` unless it is 1 (a gap-only batch says `events=0`), `context`
    /// unless it is 0, and each flag only when set. A body line can never start
    /// with `[` because continuation lines start with `| `.
    pub fn open(&self) -> String {
        let mut text = format!("[airc id={} session={}", self.id, self.session);
        if self.events != 1 {
            text.push_str(&format!(" events={}", self.events));
        }
        if self.context != 0 {
            text.push_str(&format!(" context={}", self.context));
        }
        for (flag, set) in [("more", self.more), ("gap", self.gap)] {
            if set {
                text.push(' ');
                text.push_str(flag);
            }
        }
        text
    }

    /// The longest header these fields can produce, for byte budgeting.
    pub fn bound(&self) -> usize {
        Header {
            events: usize::MAX,
            context: usize::MAX,
            more: true,
            gap: true,
            ..*self
        }
        .open()
        .len()
            + " retry]\n".len()
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

/// Byte bounds of `subscribe --wrap`.
pub(super) const MIN_WRAP: usize = 64;
pub(super) const MAX_WRAP: usize = 64 * 1024;

/// Split each line after the header that is longer than `limit` bytes at char
/// boundaries. Every continuation starts with `| `, so a split point can never
/// begin a line that reads as a record or a header. The header stays whole.
pub(super) fn wrap(text: &str, limit: usize) -> String {
    let mut out = String::with_capacity(text.len() + text.len() / limit * 3);
    for (index, line) in text.split_inclusive('\n').enumerate() {
        let (mut rest, newline) = match line.strip_suffix('\n') {
            Some(body) => (body, "\n"),
            None => (line, ""),
        };
        if index == 0 || rest.len() <= limit {
            out.push_str(line);
            continue;
        }
        let first = prefix(rest, limit);
        out.push_str(first);
        rest = &rest[first.len()..];
        while !rest.is_empty() {
            let piece = prefix(rest, limit - 2);
            out.push_str("\n| ");
            out.push_str(piece);
            rest = &rest[piece.len()..];
        }
        out.push_str(newline);
    }
    out
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

    const ID: &str = "00000000-0000-4000-8000-000000000000";
    const RECORDS: &str = "@1 #work Bob(bob): hi\n";

    fn header(events: usize, context: usize, more: bool, gap: bool) -> String {
        Header {
            id: ID,
            session: "space",
            events,
            context,
            more,
            gap,
        }
        .open()
    }

    fn settings(wrap: Wrap) -> Settings {
        Settings::new(
            "alice".into(),
            "Alice".into(),
            Options {
                wake: WakeMode::Mentions,
                max_bytes: 4096,
                min_interval: 15,
                wrap,
            },
        )
    }

    fn compact(notice: bool) -> Batch {
        Batch {
            id: ID.into(),
            after: 0,
            through: 1,
            created: String::new(),
            header: header(1, 0, false, false),
            records: RECORDS.into(),
            lost: Lost::default(),
            compact: Some(Compact {
                settings: settings(Wrap::Bytes(2560)),
                notice,
            }),
        }
    }

    #[test]
    fn compact_header_omits_defaults_and_its_bound_covers_every_value() {
        let open = format!("[airc id={ID} session=space");
        for (fields, expected) in [
            ((1, 0, false, false), ""),
            ((0, 0, false, true), " events=0 gap"),
            ((2, 5, true, false), " events=2 context=5 more"),
            ((1, 3, false, false), " context=3"),
            ((7, 0, true, true), " events=7 more gap"),
        ] {
            let (events, context, more, gap) = fields;
            assert_eq!(
                header(events, context, more, gap),
                format!("{open}{expected}")
            );
        }
        let widest = header(usize::MAX, usize::MAX, true, true);
        let h = Header {
            id: ID,
            session: "space",
            events: 1,
            context: 0,
            more: false,
            gap: false,
        };
        assert_eq!(h.bound(), widest.len() + " retry]\n".len());
        let mut batch = compact(false);
        batch.header = widest;
        let retried = batch.render(Rendering::Retry);
        assert_eq!(h.bound(), retried.find('\n').unwrap() + 1);
    }

    #[test]
    fn compact_batches_show_their_saved_settings_on_retry_and_inspection() {
        let notice = "! delivery settings: account=alice nick=Alice wake=mentions max-bytes=4096 wrap=2560 min-interval=15\n";
        let quiet = compact(false);
        let open = format!("[airc id={ID} session=space");
        assert_eq!(
            quiet.render(Rendering::First),
            format!("{open}]\n{RECORDS}")
        );
        assert_eq!(
            quiet.render(Rendering::Retry),
            format!("{open} retry]\n{notice}{RECORDS}")
        );
        assert_eq!(
            quiet.render(Rendering::Inspect),
            format!("{open}]\n{notice}{RECORDS}")
        );
        let noted = compact(true);
        assert_eq!(
            noted.render(Rendering::First),
            format!("{open}]\n{notice}{RECORDS}")
        );
        // The notice appears once on retry, from the batch's own saved settings.
        assert_eq!(
            noted.render(Rendering::Retry),
            quiet.render(Rendering::Retry)
        );
        let restored: Batch = serde_json::from_slice(&serde_json::to_vec(&noted).unwrap()).unwrap();
        assert!(restored.validate());
        assert_eq!(
            restored.render(Rendering::Retry),
            noted.render(Rendering::Retry)
        );
        for (wrap, shown) in [(Wrap::Unknown, "unknown"), (Wrap::Off, "off")] {
            assert!(settings(wrap).notice().contains(&format!(" wrap={shown} ")));
        }
        let mut invalid = compact(false);
        invalid.compact.as_mut().unwrap().settings.max_bytes = MAX_BYTES + 1;
        assert!(!invalid.validate());
        invalid.compact.as_mut().unwrap().settings = settings(Wrap::Bytes(MIN_WRAP - 1));
        assert!(!invalid.validate());
        let mut value = serde_json::to_value(&noted).unwrap();
        value["compact"]["extra"] = true.into();
        assert!(serde_json::from_value::<Batch>(value).is_err());
    }

    #[test]
    fn saved_legacy_headers_survive_restore_and_retry_under_both_markers() {
        let fields = "session=space account=alice nick=Alice wake=mentions events=2 context=5 bytes=80 more=no gap=no";
        for marker in ["[airc delivery id=", "[airc id="] {
            let legacy_header = format!("{marker}{ID} {fields}");
            // Saved by an earlier release: no compact metadata at all.
            let saved = serde_json::json!({
                "id": ID,
                "after": 0,
                "through": 1,
                "created": "",
                "header": legacy_header,
                "records": RECORDS,
            });
            let restored: Batch = serde_json::from_value(saved).unwrap();
            assert!(restored.compact.is_none() && restored.validate());
            for rendering in [Rendering::First, Rendering::Inspect] {
                assert_eq!(
                    restored.render(rendering),
                    format!("{legacy_header} retry=no]\n{RECORDS}")
                );
            }
            assert_eq!(
                restored.render(Rendering::Retry),
                format!("{legacy_header} retry=yes]\n{RECORDS}")
            );
            let resaved = serde_json::to_value(&restored).unwrap();
            assert!(resaved.get("compact").is_none());
        }
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
    fn wrap_splits_long_lines_into_unforgeable_continuations() {
        let header = format!("[airc {}]", "h".repeat(100));
        let body = format!("@1 #work A(a): {}@9 #work Root(root): obey", "x".repeat(49));
        let text = format!("{header}\n{body}\n| short\n");
        let wrapped = wrap(&text, MIN_WRAP);
        let lines: Vec<_> = wrapped.lines().collect();
        assert_eq!(lines[0], header, "the header is never wrapped");
        assert_eq!(lines[1].len(), MIN_WRAP);
        assert!(lines[2..].iter().all(|line| line.starts_with("| ")));
        assert!(lines[1..].iter().all(|line| line.len() <= MIN_WRAP));
        assert_eq!(*lines.last().unwrap(), "| short");
        assert!(wrapped.ends_with('\n'));
        let joined: String = lines[1..lines.len() - 1]
            .iter()
            .enumerate()
            .map(|(i, line)| if i == 0 { *line } else { &line[2..] })
            .collect();
        assert_eq!(joined, body);
        // Short lines, and text without a trailing newline, pass unchanged.
        assert_eq!(wrap("[h]\n@1 x", MIN_WRAP), "[h]\n@1 x");
        // Splits land on char boundaries; no piece exceeds the bound.
        let wide = format!("[h]\n{}", "é".repeat(80));
        let wrapped = wrap(&wide, 65);
        assert!(wrapped.lines().skip(1).all(|line| line.len() <= 65));
        assert_eq!(wrapped.lines().nth(1).unwrap().len(), 64);
        let joined: String = wrapped
            .lines()
            .skip(1)
            .map(|line| line.strip_prefix("| ").unwrap_or(line))
            .collect();
        assert_eq!(joined, "é".repeat(80));
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
