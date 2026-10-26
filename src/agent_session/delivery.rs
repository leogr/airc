//! Durable delivery batches for one consumer, separate from the read cursor.
//!
//! A batch is persisted as pending before any caller sees it. Only an accept
//! (the sink took it) or an explicit drop moves the delivery cursor. Nothing
//! here replays a pending batch on its own: a restarted consumer is refused
//! until the operator names the pending ID.
//!
//! A receipt-enabled consumer also asks its receiver to report each batch.
//! Transport acceptance of such a batch starts a hold on later batches until
//! that exact report, its timeout, or urgency/retention pressure ends it. The
//! report is a same-user local call: never acceptance, never processing proof.

use super::{
    attention::{Classification, WakeMode},
    reference,
    storage::SOCKET_PATH_BYTES,
};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use std::collections::{HashSet, VecDeque};

pub(super) const MIN_BYTES: usize = 1024;
pub(super) const MAX_BYTES: usize = 1024 * 1024;
pub(super) const DEFAULT_BYTES: usize = 8 * 1024;
/// Upper bound of `--min-interval`, in seconds.
pub(super) const MAX_INTERVAL: u64 = 3600;
/// Upper bound of `--receipt-timeout`, in seconds.
pub(super) const MAX_RECEIPT_TIMEOUT: u64 = 3600;
/// Receipt records kept as metadata; the active hold is never evicted.
pub(super) const CREDITS: usize = 32;
/// Bytes a receipt-enabled consumer's header and notices must leave for
/// records within `--max-bytes`; registration is refused otherwise.
pub(super) const RECORD_ROOM: usize = 256;

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
    /// Accepted receipt-enabled batches, oldest first, without payload. Only the
    /// newest may still be unreleased: the active hold (version 4).
    #[serde(default, skip_serializing_if = "VecDeque::is_empty")]
    pub credits: VecDeque<Credit>,
}

impl Delivery {
    /// Keep a new record, evicting the oldest released one at the bound.
    pub fn credit(&mut self, credit: Credit) {
        if self.credits.len() >= CREDITS
            && let Some(oldest) = self.credits.iter().position(|c| c.released.is_some())
        {
            self.credits.remove(oldest);
        }
        self.credits.push_back(credit);
    }

    /// The record still holding later batches, if any.
    pub fn hold(&self) -> Option<&Credit> {
        self.credits.back().filter(|c| c.released.is_none())
    }

    pub fn hold_mut(&mut self) -> Option<&mut Credit> {
        self.credits.back_mut().filter(|c| c.released.is_none())
    }

    /// Bounded, unique, well-formed records; only the newest may be active, and
    /// none shares the pending batch's ID.
    pub fn credits_valid(&self) -> bool {
        let mut ids = HashSet::new();
        self.credits.len() <= CREDITS
            && self
                .credits
                .iter()
                .all(|c| c.valid() && ids.insert(c.id.as_str()))
            && self
                .credits
                .iter()
                .rev()
                .skip(1)
                .all(|c| c.released.is_some())
            && self
                .pending
                .as_ref()
                .is_none_or(|batch| !ids.contains(batch.id.as_str()))
    }
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
    /// The receipt its consumer requested; only compact batches carry one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub receipt: Option<ReceiptRequest>,
}

/// What a receipt-enabled batch asks of its receiver, as assembled.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ReceiptRequest {
    /// The worker's canonical state directory, so the printed command reaches
    /// this store whatever the receiver's HOME or XDG_STATE_HOME.
    pub store: String,
    pub session: String,
    pub scope: String,
    /// Seconds its accepted batch holds later ones without a report.
    pub timeout: u64,
    /// A report that arrived while transport acceptance was still pending.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub received: Option<String>,
}

impl ReceiptRequest {
    /// The local notice that every rendering of the batch carries: one
    /// complete command line, its store path quoted as one shell word.
    pub fn notice(&self, id: &str) -> String {
        format!(
            "! receipt requested: scope={scope} timeout={}s; check the scope, then report receipt once before other work: airc --state-dir {} deliveries {} --mark-received {id} --receipt-scope {scope}\n",
            self.timeout,
            shell_word(&self.store),
            self.session,
            scope = self.scope,
        )
    }

    fn valid(&self) -> bool {
        valid_session(&self.session)
            && valid_store(&self.store, &self.session)
            && validate_scope(&self.scope).is_ok()
            && (1..=MAX_RECEIPT_TIMEOUT).contains(&self.timeout)
            && self.received.as_deref().is_none_or(|at| time(at).is_some())
    }
}

/// One accepted receipt-enabled batch: metadata only, never its payload.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Credit {
    pub id: String,
    pub scope: String,
    /// Transport acceptance (UTC), which started its hold.
    pub accepted: String,
    /// The original hold in seconds and its UTC end.
    pub timeout: u64,
    pub deadline: String,
    /// The receiver's first report, before or after acceptance.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub received: Option<String>,
    /// Absent only while this record is the active hold.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub released: Option<Release>,
}

impl Credit {
    fn valid(&self) -> bool {
        let deadline = time(&self.accepted).and_then(|accepted| {
            accepted
                .checked_add_signed(chrono::Duration::seconds(i64::try_from(self.timeout).ok()?))
        });
        validate_id(&self.id).is_ok()
            && validate_scope(&self.scope).is_ok()
            && (1..=MAX_RECEIPT_TIMEOUT).contains(&self.timeout)
            && deadline.is_some_and(|deadline| time(&self.deadline) == Some(deadline))
            && self.received.as_deref().is_none_or(|at| time(at).is_some())
            && self.released.as_ref().is_none_or(|release| {
                time(&release.at).is_some()
                    && (release.reason != Reason::Received || self.received.is_some())
            })
    }

    /// One ledger line: transport acceptance, receipt and release kept apart.
    pub fn describe(&self) -> String {
        format!(
            "receipt {} scope={} accepted={} deadline={} received={} {}\n",
            self.id,
            self.scope,
            self.accepted,
            self.deadline,
            self.received.as_deref().unwrap_or("none"),
            self.released
                .as_ref()
                .map_or("hold=active".into(), |release| {
                    format!("released={} at={}", release.reason.name(), release.at)
                })
        )
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Release {
    pub reason: Reason,
    pub at: String,
}

/// Why a hold ended. Only `received` reflects a receiver report.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Reason {
    Received,
    Expired,
    Urgent,
    Pressure,
}

impl Reason {
    pub fn name(self) -> &'static str {
        match self {
            Self::Received => "received",
            Self::Expired => "expired",
            Self::Urgent => "urgent",
            Self::Pressure => "pressure",
        }
    }
}

/// A receipt scope: one exact lowercase UUID, such as a Codex thread ID.
pub(super) fn validate_scope(scope: &str) -> Result<()> {
    ensure!(
        uuid::Uuid::parse_str(scope).is_ok_and(|uuid| uuid.hyphenated().to_string() == scope),
        "invalid receipt scope; give the receiver's exact lowercase hyphenated UUID"
    );
    Ok(())
}

/// A saved RFC 3339 timestamp.
pub(super) fn time(text: &str) -> Option<chrono::DateTime<chrono::Utc>> {
    chrono::DateTime::parse_from_rfc3339(text)
        .ok()
        .map(|at| at.with_timezone(&chrono::Utc))
}

/// A store path a receipt notice can print: absolute, without control or
/// other non-printing characters, and short enough that its session socket
/// path keeps the bound `Storage::open` enforces.
pub(super) fn valid_store(store: &str, session: &str) -> bool {
    store.starts_with('/')
        && !store.chars().any(char::is_control)
        && !store.escape_debug().to_string().contains("\\u{")
        && store.len() + session.len() + "//client.sock".len() < SOCKET_PATH_BYTES
}

/// One POSIX shell word for `text`: bare when every byte is unambiguous,
/// otherwise single-quoted with each `'` written as `'\''`, so nothing in it
/// is expanded, split or executed.
pub(super) fn shell_word(text: &str) -> String {
    if !text.is_empty()
        && text
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"/._-".contains(&b))
    {
        text.into()
    } else {
        format!("'{}'", text.replace('\'', r"'\''"))
    }
}

fn valid_session(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 32
        && name
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"_-".contains(&c))
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
        let receipt = self
            .receipt
            .as_ref()
            .map_or(String::new(), |request| request.notice(&self.id));
        format!(
            "{}{}]\n{notice}{receipt}{}",
            self.header,
            if retry { " retry" } else { "" },
            self.records
        )
    }

    pub fn validate(&self) -> bool {
        validate_id(&self.id).is_ok()
            && self.header.len() + self.records.len() <= MAX_BYTES
            && self.compact.as_ref().is_none_or(|c| c.settings.valid())
            && self
                .receipt
                .as_ref()
                .is_none_or(|request| self.compact.is_some() && request.valid())
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
    /// Requested on each new batch when set; never part of the settings notice.
    pub receipt: Option<ReceiptOptions>,
}

/// `subscribe --receipt-timeout/--receipt-scope`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct ReceiptOptions {
    pub timeout: u64,
    pub scope: uuid::Uuid,
}

impl std::fmt::Display for ReceiptOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "receipt-timeout={} receipt-scope={}",
            self.timeout,
            self.scope.hyphenated()
        )
    }
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
            receipt: None,
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

/// The shared reference that starts a rendered message record, if it has one
/// that `show` can take.
pub(super) fn record_reference(text: &str) -> Option<&str> {
    let text = text.strip_prefix("history ").unwrap_or(text);
    let token = text.split(' ').next()?;
    reference::shareable(token).then_some(token)
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
                receipt: None,
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
            receipt: None,
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
    fn receipt_requests_need_compact_batches_exact_scopes_and_bounds() {
        let scope = "019a0000-0000-7000-8000-00000000c0de";
        let request = || ReceiptRequest {
            store: "/srv/airc state".into(),
            session: "space".into(),
            scope: scope.into(),
            timeout: 60,
            received: None,
        };
        let mut batch = compact(false);
        batch.receipt = Some(request());
        assert!(batch.validate());
        let notice = request().notice(ID);
        assert!(notice.ends_with(&format!(
            "airc --state-dir '/srv/airc state' deliveries space --mark-received {ID} --receipt-scope {scope}\n"
        )));
        for rendering in [Rendering::First, Rendering::Retry, Rendering::Inspect] {
            let text = batch.render(rendering);
            assert!(text.ends_with(&format!("{notice}{RECORDS}")), "{text}");
        }
        let restored: Batch = serde_json::from_slice(&serde_json::to_vec(&batch).unwrap()).unwrap();
        assert!(restored.validate());
        let mut value = serde_json::to_value(&batch).unwrap();
        value["receipt"]["extra"] = true.into();
        assert!(serde_json::from_value::<Batch>(value).is_err());
        // A saved legacy batch never carries one.
        let mut legacy = compact(false);
        legacy.compact = None;
        legacy.receipt = Some(request());
        assert!(!legacy.validate());
        for invalid in [
            ReceiptRequest {
                scope: scope.to_uppercase(),
                ..request()
            },
            ReceiptRequest {
                scope: scope.replace('-', ""),
                ..request()
            },
            ReceiptRequest {
                timeout: 0,
                ..request()
            },
            ReceiptRequest {
                timeout: MAX_RECEIPT_TIMEOUT + 1,
                ..request()
            },
            ReceiptRequest {
                session: "two words".into(),
                ..request()
            },
            ReceiptRequest {
                store: "relative".into(),
                ..request()
            },
            ReceiptRequest {
                store: "/tmp/new\nline".into(),
                ..request()
            },
            ReceiptRequest {
                store: format!("/{}", "a".repeat(SOCKET_PATH_BYTES)),
                ..request()
            },
            ReceiptRequest {
                received: Some("yesterday".into()),
                ..request()
            },
        ] {
            let mut batch = compact(false);
            batch.receipt = Some(invalid);
            assert!(!batch.validate());
        }
        assert!(validate_scope(scope).is_ok());
        assert!(validate_scope(ID).is_ok());
        assert!(validate_scope("not-a-uuid").is_err());
    }

    #[test]
    fn shell_words_never_expand_split_or_execute() {
        assert_eq!(shell_word("/srv/airc-state_1.d"), "/srv/airc-state_1.d");
        assert_eq!(shell_word("/a b"), "'/a b'");
        assert_eq!(shell_word("/it's"), r"'/it'\''s'");
        assert_eq!(shell_word("/$(x)`y`"), "'/$(x)`y`'");
        assert_eq!(shell_word("/é"), "'/é'");
        assert_eq!(shell_word(""), "''");
        // A POSIX shell turns each word back into exactly the original bytes.
        for text in ["/a b", "/it's", "/$(touch pwned)`touch pwned`", "/q\"\\'"] {
            let out = std::process::Command::new("sh")
                .args(["-c", &format!("printf %s {}", shell_word(text))])
                .current_dir(std::env::temp_dir())
                .output()
                .unwrap();
            assert_eq!(String::from_utf8(out.stdout).unwrap(), text);
        }
        assert!(valid_store("/srv/a b'c", "space"));
        assert!(!valid_store("/srv/\u{202e}", "space"));
        assert!(!valid_store("srv", "space"));
    }

    #[test]
    fn prefixes_and_references_respect_utf8_and_record_shape() {
        assert_eq!(prefix("aé", 2), "a");
        assert_eq!(prefix("aé", 3), "aé");
        assert_eq!(prefix("abc", 9), "abc");
        let shared = "m:Ej5FZ-ibQtOkVkJmFBdAAA";
        assert_eq!(
            record_reference(&format!("{shared} #work A(a): x")),
            Some(shared)
        );
        assert_eq!(
            record_reference(&format!("history {shared} #work A(a): x")),
            Some(shared)
        );
        assert_eq!(
            record_reference("msgid:id-3 #work A(a): x"),
            Some("msgid:id-3")
        );
        // Neither an older local number nor an unusable token is offered to show.
        assert_eq!(record_reference("@12 #work A(a): x"), None);
        assert_eq!(record_reference("unavailable #work A(a): x"), None);
        assert_eq!(
            record_reference("m:Ej5FZ-ibQtOkVkJmFBdAAB #work A(a): x"),
            None
        );
        assert_eq!(record_reference("msgid:x…[truncated] #work A(a): x"), None);
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
